use async_task::Runnable;

use futures_lite::{Future, FutureExt};

use crate::queues::{LocalQueue, GLOBAL_QUEUE};
use crate::*;

use once_cell::unsync::Lazy as UnsyncLazy;

thread_local! {
    static LOCAL_QUEUE: UnsyncLazy<Arc<LocalQueue>> =
        UnsyncLazy::new(||{
            GLOBAL_QUEUE.subscribe()
        });
}

/// Runs a queue
pub async fn run_local_queue(
    idle_timeout: Option<Duration>,
) {
    let local_queue = LOCAL_QUEUE
        .with(|lq| UnsyncLazy::force(lq).clone());

    scopeguard::defer!({
        // push all un-handled task to GLOBAL_QUEUE
        local_queue.clear();
    });

    local_queue.run(idle_timeout).await;
}

/*
=======
/// Runs a queue
pub async fn run_local_queue() {
    LOCAL_QUEUE_ACTIVE.with(|r| r.set(true));
    scopeguard::defer!(LOCAL_QUEUE_ACTIVE.with(|r| r.set(false)));
    loop {
        for _ in 0..200 {
            while let Some(r) = LOCAL_QUEUE.with(|q| q.pop()) {
                GLOBAL_QUEUE.notify();
                r.run();
            }

            // we only wait here because we want *idle* workers to be notified, not just anyone
            let evt = GLOBAL_QUEUE.wait();
            evt.await;
        }
        futures_lite::future::yield_now().await;
    }
>>>>>>> upstream/master
*/

/// Spawns a task
pub fn spawn<F>(future: F) -> async_task::Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // next-generation scheduler with load balancing
    fn lb_scheduler(runnable: Runnable) {
        let locals = &GLOBAL_QUEUE.locals;
        let mut maybe_selected =
            Option::<Arc<LocalQueue>>::None;

        let guard = scc::ebr::Guard::new();
        for (_, this) in locals.iter(&guard) {
            log::debug!("scanning {:?}", &this);

            // ignore all inactive LocalQueue
            if !this.active() {
                continue;
            }

            // ignore all busy LocalQueue
            if !this.idle() {
                continue;
            }

            let selected = match maybe_selected {
                Some(ref v) => v.clone(),
                None => {
                    maybe_selected = Some(this.clone());
                    continue;
                },
            };

            // Find the most idle worker thread, as it has less work than the others.
            if this.cpu_usage() > selected.cpu_usage() {
                continue;
            }
            if this.stress() < selected.stress() {
                maybe_selected = Some(this.clone());
                continue;
            }

            continue;
            // normally useless
            if (this.worker.is_empty()
                && !selected.worker.is_empty())
                && this.worker.spare_capacity()
                    > selected.worker.spare_capacity()
                && this.worker.capacity()
                    > selected.worker.capacity()
            {
                maybe_selected = Some(this.clone());
                continue;
            }
        } // for (_, this) in locals.iter()
        core::mem::drop(guard);

        // check if selected one
        if let Some(selected) = maybe_selected {
            log::info!(
                "lb_scheduler: scanned, pushed to {:#?}",
                &selected
            );
            selected.push(runnable);
        } else {
            log::info!(
                "lb_scheduler: pushed to GlobalQueue"
            );
            GLOBAL_QUEUE.push(runnable);

            /*
            log::info!("lb_scheduler: fallback to old_scheduler.");
            old_scheduler(runnable);
            */
        }
    }

    // the "simple" scheduler, just push to GlobalQueue.
    fn simple_scheduler(runnable: Runnable) {
        GLOBAL_QUEUE.push(runnable);
    }

    let (runnable, task) = async_task::spawn(
        future,
        if *SMOLSCALE2_SCHED_LB {
            lb_scheduler
        } else {
            //old_scheduler
            simple_scheduler
        },
    );

    runnable.schedule();
    if GLOBAL_QUEUE.queue.len() > running_threads() {
        tick_monitor();
    }
    task
}

/// Globally rebalance.
pub fn global_rebalance() {
    GLOBAL_QUEUE.notify();
}
