use async_task::Runnable;

use futures_lite::{Future, FutureExt};

use crate::queues::{GLOBAL_QUEUE, LocalQueue};
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
    idle_timeout: Option<Duration>
) {
    LOCAL_QUEUE.with(|lq|{ lq.active.set(true); });
    scopeguard::defer!({
        LOCAL_QUEUE.with(|lq|{ lq.active.set(false); });
    });

    let mut running: bool = true;

    while running {
        // subscribe
        let local_evt = async {
            let le =
                LOCAL_QUEUE.with(|lq|lq.event.clone() );
            le.wait().await;
            le.reset();
            true
        };
        let global_evt = async {
            GLOBAL_QUEUE.wait().await;
            true
        };
        let evt = local_evt.or(global_evt);

        // handle a bulk of Runnable
        {
            while let Some(runnable) =
                LOCAL_QUEUE.with(|lq| lq.pop())
            {
                if let Err(any_err) =
                    // if possible (panic="unwind"), catch all panic from Runnable, so we can prevent worker thread exited unexpectedly.
                    std::panic::catch_unwind(||{
                        runnable.run();
                    })
                {
                    let thr = std::thread::current();
                    log::error!(
                        "worker thread ({thr:?}): 'Runnable' panic: error={} | typeid={:?}",
                        any_fmt(&any_err),
                        any_err.type_id(),
                    );
                }

                if fastrand::u8(..) < 3 {
                    futures_lite::future::yield_now().await;
                }
            }
        }
        // wait now, so that when we get woken up, we *know* that something happened to the local-queue or global-queue.
        running = evt.or(async {
            if let Some(t) = idle_timeout {
                async_io::Timer::after(t).await;
                false
            } else {
                async_io::Timer::never().await;
                true
            }
        }).await;
    }
}

/// Spawns a task
pub fn spawn<F>(future: F) -> async_task::Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{

    // the original scheduler.
    fn old_scheduler(runnable: Runnable) {
        // if the current thread is not processing tasks, we go to the global queue directly.
        let lq_active =
            LOCAL_QUEUE.with(|lq|{ lq.active.get() });
        if !lq_active || fastrand::usize(0..512) == 0 {
            GLOBAL_QUEUE.push(runnable);
            log::trace!("old_scheduler: pushed to global queue");
        } else {
            log::trace!("old_scheduler: pushed to local queue");
            LOCAL_QUEUE.with(|lq|{ lq.push(runnable);});
        }
    }

    // next-generation scheduler with load balancing
    fn lb_scheduler(runnable: Runnable) {
        let locals = &GLOBAL_QUEUE.locals;
        if let Some(entry) = locals.first_entry() {
            let mut idle_lq: Arc<LocalQueue> =
                entry.get().clone();

            // prevent deadlocks in scc::HashMap
            core::mem::drop(entry);

            log::trace!("lb_scheduler: scanning");
            locals.scan(
                |_, local|{
                    // Find the most idle worker thread, as it has less work than the others.

                    if local.worker.is_empty() {
                        if ! idle_lq.worker.is_empty() {
                            idle_lq = local.clone();
                            return;
                        }
                    }
                    if local.worker.spare_capacity() > idle_lq.worker.spare_capacity() {
                        idle_lq = local.clone();
                        return;
                    }
                    if local.worker.capacity() > idle_lq.worker.capacity() {
                        idle_lq = local.clone();
                        return;
                    }
                        
                }
            );
            log::debug!("lb_scheduler: scanned, pushed to {}", idle_lq.id());
            idle_lq.push(runnable);
        } else {
            log::debug!("lb_scheduler: fallback to old_scheduler.");
            old_scheduler(runnable);
        }
    }

    let (runnable, task) =
        async_task::spawn(
            future,
            //old_scheduler
            lb_scheduler
        );

    runnable.schedule();
    task
}

/// Globally rebalance.
pub fn global_rebalance() {
    GLOBAL_QUEUE.rebalance();
}

fn any_fmt(
    any: &dyn core::any::Any
) -> String {
    // try detect type of core::any::Any
    if let Some(val) = any.downcast_ref::<String>() {
        return val.to_owned();
    }
    if let Some(val) = any.downcast_ref::<&str>() {
        return val.to_string();
    }
    if let Some(val) =any.downcast_ref::<std::io::Error>(){
        return format!("{:?}", val);
    }

    format!("{:?}", &any)
}
