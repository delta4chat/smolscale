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
    let local_queue =
        LOCAL_QUEUE.with(|lq|{
            UnsyncLazy::force(lq).clone()
        });

    local_queue.run(idle_timeout).await;
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
            let mut selected: Arc<LocalQueue> =
                entry.get().clone();

            // prevent deadlocks in scc::HashMap
            core::mem::drop(entry);

            log::trace!("lb_scheduler: scanning");
            locals.scan(
                |_, this|{
                    // Find the most idle worker thread, as it has less work than the others.

                    if this.idle() {
                        if ! selected.idle() {
                            selected = this.clone();
                            return;
                        }
                    }
                    if this.worker.is_empty() {
                        if ! selected.worker.is_empty(){
                            selected = this.clone();
                            return;
                        }
                    }

                    if this.backlogs() < selected.backlogs() {
                        selected = this.clone();
                        return;
                    }
                    if this.worker.spare_capacity() > selected.worker.spare_capacity() {
                        selected = this.clone();
                        return;
                    }
                    if this.worker.capacity() > selected.worker.capacity() {
                        selected = this.clone();
                        return;
                    }
                        
                }
            );
            log::debug!("lb_scheduler: scanned, pushed to {}", selected.id());
            selected.push(runnable);
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

