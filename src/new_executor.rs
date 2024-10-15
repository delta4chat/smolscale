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

use std::thread::available_parallelism;

use futures_lite::Future;
use once_cell::sync::Lazy;

/// Spawns a task
pub fn spawn<F>(future: F) -> async_task::Task<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    // the "simple" scheduler, just push to GlobalQueue.
    fn simple_scheduler(runnable: Runnable) {
        GLOBAL_QUEUE.push(runnable);
    }


    let (runnable, task) = async_task::spawn(future, simple_scheduler);

    runnable.schedule();
    if GLOBAL_QUEUE.queue.len() > running_threads() {
        tick_monitor();
    }
    task
}
