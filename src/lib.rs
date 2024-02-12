//! A global, auto-scaling scheduler for [async-task] using work-balancing.
//!
//! ## What? Another executor?
//!
//! `smolscale` is a **work-balancing** executor based on [async-task], designed to be a drop-in replacement to `smol` and `async-global-executor`. It is designed based on the idea that work-stealing, the usual approach in async executors like `async-executor` and `tokio`, is not the right algorithm for scheduling huge amounts of tiny, interdependent work units, which are what message-passing futures end up being. Instead, `smolscale` uses *work-balancing*, an approach also found in Erlang, where a global "balancer" thread periodically instructs workers with no work to do to steal work from each other, but workers are not signalled to steal tasks from each other on every task scheduling. This avoids the extremely frequent stealing attempts that work-stealing schedulers generate when applied to async tasks.
//!
//! `smolscale`'s approach especially excels in two circumstances:
//! - **When the CPU cores are not fully loaded**: Traditional work stealing optimizes for the case where most workers have work to do, which is only the case in fully-loaded scenarios. When workers often wake up and go back to sleep, however, a lot of CPU time is wasted stealing work. `smolscale` will instead drastically reduce CPU usage in these circumstances --- a `async-executor` app that takes 80% of CPU time may now take only 20%. Although this does not improve fully-loaded throughput, it significantly reduces power consumption and does increase throughput in circumstances where multiple thread pools compete for CPU time.
//! - **When a lot of message-passing is happening**: Message-passing workloads often involve tasks quickly waking up and going back to sleep. In a work-stealing scheduler, this again floods the scheduler with stealing requests. `smolscale` can significantly improve throughput, especially compared to executors like `async-executor` that do not special-case message passing.

use async_compat::CompatExt;
use backtrace::Backtrace;
use futures_lite::prelude::*;
use once_cell::sync::{Lazy, OnceCell};
use std::{
    io::{Write, stderr},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tabwriter::TabWriter;

use portable_atomic::{
    AtomicU128, AtomicUsize, AtomicBool,
    Ordering::Relaxed
};

pub mod fastcounter;
pub use fastcounter::*;

pub mod immortal;
pub use immortal::*;

pub mod reaper;
pub use reaper::*;

mod new_executor;

mod queues;
use queues::GLOBAL_QUEUE;

//pub const CHANGE_THRESH: u32 = 10;

static POLL_COUNT: FastCounter = FastCounter::new();
static ACTIVE_TASKS: FastCounter = FastCounter::new();
static FUTURES_BEING_POLLED: FastCounter = FastCounter::new();

pub const MIN_THREADS: usize = 1;
static MAX_THREADS: Lazy<AtomicUsize> = Lazy::new(||{
    let mut max = num_cpus::get();
    max *= 10;

    if max < 20 {
        max = 20;
    }

    AtomicUsize::new(max)
});

pub(crate) static THREAD_MAP:
    Lazy<scc::TreeIndex<
            u128,
            Arc<std::thread::JoinHandle<()>>
    >> = Lazy::new(scc::TreeIndex::new);
static THREAD_SPAWN_LOCK: Lazy<parking_lot::Mutex<()>> = Lazy::new(||{ parking_lot::Mutex::new(()) });

pub(crate) static MONITOR: OnceCell<std::thread::JoinHandle<()>> = OnceCell::new();
pub const MONITOR_INTERVAL: Duration =
    // this long-interval is completely fine due to moniter_loop() uses 'park_timeout' for immediately woken if necessary
    Duration::from_secs(1);

static SINGLE_THREAD: AtomicBool = AtomicBool::new(false);

pub const SMOLSCALE_USE_AGEX: Lazy<bool> =
    Lazy::new(|| {
        std::env::var("SMOLSCALE_USE_AGEX").is_ok()
    });

pub const SMOLSCALE_PROFILE: Lazy<bool> =
    Lazy::new(|| {
        std::env::var("SMOLSCALE_PROFILE").is_ok()
    });

pub const SMOLSCALE2_SCHED_LB: Lazy<bool> =
    Lazy::new(||{
        std::env::var("SMOLSCALE2_SCHED_LB").is_ok()
    });

/// [deprecated] Irrevocably puts smolscale into single-threaded mode.
#[deprecated]
pub fn permanently_single_threaded() {
    set_max_threads(1);
    SINGLE_THREAD.store(true, Relaxed);
}

/// set maximum number of worker threads
pub fn set_max_threads(mut max: usize) {
    if max < MIN_THREADS {
        max = MIN_THREADS;
    }
    MAX_THREADS.store(max, Relaxed);
}

/// Returns the number of running worker threads.
pub fn running_threads() -> usize {
    let mut running = 0;
    let mut to_remove = vec![];

    {
        let guard = scc::ebr::Guard::new();
        for (idx, join) in THREAD_MAP.iter(&guard) {
            if join.is_finished() {
                let name = join.thread().name();
                log::warn!("worker thread (id={idx}, name={name:?}) exited!");
                to_remove.push(*idx);
            } else {
                running += 1;
            }
        }
        // guard dropping here
    }

    for rm_idx in to_remove.iter() {
        THREAD_MAP.remove(rm_idx);
    }

    running
}

fn start_monitor() {
    MONITOR.get_or_init(|| {
        std::thread::Builder::new()
            .name("sscale-mon".into())
            .spawn(monitor_loop)
            .expect("cannot spawn monitor thread")
    });
}


/// tells the monitor something happening
///
/// # Panics
/// this function will panics if no monitor thread running.
pub fn tick_monitor() {
    if let Some(mon) = MONITOR.get() {
        mon.thread().unpark();
    } else {
        panic!("fatal error: no running monitor!");
    }
}

pub(crate) fn any_fmt(
    any: &dyn core::any::Any
) -> String {
    // try detect type of core::any::Any
    if let Some(v) = any.downcast_ref::<String>() {
        return v.to_owned();
    }
    if let Some(v) = any.downcast_ref::<&str>() {
        return v.to_string();
    }
    if let Some(v) = any.downcast_ref::<std::io::Error>() {
        return format!("{:?}", v);
    }

    format!("{:?}", &any)
}
pub(crate) fn average<T>(set: &[T]) -> T
where
    T: Default + Copy + From<u64> +
    core::ops::Add<Output=T> + core::ops::Div<Output=T>
{
    let len = set.len();
    if len > 0 {
        let len: T = T::from(len as u64);
        let mut sum: T = Default::default();
        for n in set.iter() {
            sum = sum + *n;
        }
        sum / len
    } else {
        Default::default()
    }
}

fn monitor_loop() {
    use std::thread::sleep;

    if *SMOLSCALE_USE_AGEX { return; }
    log::trace!("monitor started");

    fn start_thread(exitable: bool, process_io: bool) {
        let _lock = THREAD_SPAWN_LOCK.lock();

        let current_threads: usize = running_threads();
        let max_threads: usize = MAX_THREADS.load(Relaxed);

        if current_threads >= max_threads {
            log::info!("cannot spawn new worker thread: max threads ({max_threads}) exceeded");
            return;
        }

        let idx: u128 =
            namespace_unique_id("smolscale2-thread-id");

        let thread = std::thread::Builder::new()
            .name(
                if exitable {
                    format!("sscale-wkr-e-{idx}")
                } else {
                    format!("sscale-wkr-c-{idx}")
                }
            )
            .spawn(move || {
                scopeguard::defer!({
                    THREAD_MAP.remove(&idx);
                });

                // let local_exec = LEXEC.with(|v| Rc::clone(v));
                let fut = async {
                    // let run_local = local_exec.run(futures_lite::future::pending::<()>());
                    if exitable {
                        new_executor::run_local_queue(
                            Some(Duration::from_secs(9))
                        ).await;
                    } else {
                        new_executor::run_local_queue(
                            None
                        ).await;
                    };
                }
                .compat();

                if process_io {
                    async_io::block_on(fut)
                } else {
                    futures_lite::future::block_on(fut)
                }
            })
            .expect("cannot spawn worker thread");

        THREAD_MAP.insert(
            idx, Arc::new(thread)
        ).expect("cannot add spawned worker thread to global list of join handles");
    }

    let mut threads = num_cpus::get();
    if SINGLE_THREAD.load(Relaxed) || std::env::var("SMOLSCALE_SINGLE").is_ok() {
        threads = 1;
    }

    for _ in 0..threads {
        start_thread(false, true);
    }

    std::thread::Builder::new()
        .name("sscale-watchdog".to_string())
        .spawn(|| {
            sleep(MONITOR_INTERVAL);
            loop {
                if running_threads() >= MIN_THREADS {
                    sleep(MONITOR_INTERVAL);
                    continue;
                }

                log::info!("start new thread for panic safe!");
                start_thread(false, true);
            }
        })
        .expect("cannot spawn panic safe thread");

    if SINGLE_THREAD.load(Relaxed) {
        return;
    }

    // sleep or woken
    fn resting() {
        std::thread::park_timeout(MONITOR_INTERVAL);
    }

    // "Token bucket"
    let mut token_bucket: i8 = 100;
    for count in 0u64.. {
        // print debug info
        if count % 10 == 0 {
            let g = scc::ebr::Guard::new();
            for (_, lq) in GLOBAL_QUEUE.locals.iter(&g){
                log::info!("queue status: {lq:?}");
            }
            // guard dropping now
        }

        if count % 15 == 0 && token_bucket < 100 {
            token_bucket += 1;
        }
        new_executor::global_rebalance();

        #[cfg(not(feature = "preempt"))]
        {
            resting();
        }
        #[cfg(feature = "preempt")]
        {
            let before_sleep = POLL_COUNT.count();
            resting();
            let after_sleep = POLL_COUNT.count();

            let running_tasks: u128 =
                FUTURES_BEING_POLLED.count();

            let running_workers: u128 =
                running_threads() as u128;

            if after_sleep.abs_diff(before_sleep) < 2
                && token_bucket > 0
                && running_tasks >= running_workers
            {
                start_thread(true, false);
                token_bucket -= 1;
            }
        }
    }
    unreachable!()
}

/// Spawns a future onto the global executor and immediately blocks on it.`
pub fn block_on<T: Send + 'static>(future: impl Future<Output = T> + Send + 'static) -> T {
    async_io::block_on(WrappedFuture::new(future).compat())
}

/// Spawns a task onto the lazily-initialized global executor.
pub fn spawn<T: Send + 'static>(
    future: impl Future<Output = T> + Send + 'static,
) -> async_executor::Task<T> {
    start_monitor();
    if *SMOLSCALE_USE_AGEX {
        async_global_executor::spawn(future)
    } else {
        new_executor::spawn(WrappedFuture::new(future))
    }
}

struct WrappedFuture<T, F: Future<Output = T>> {
    task_id: u128,
    spawn_btrace: Option<Arc<Backtrace>>,
    fut: F,
}

/// Returns the current number of active tasks.
pub fn active_task_count() -> u128 {
    ACTIVE_TASKS.count()
}

impl<T, F: Future<Output = T>> Drop for WrappedFuture<T, F> {
    fn drop(&mut self) {
        ACTIVE_TASKS.decr();
        if *SMOLSCALE_PROFILE {
            PROFILE_MAP.remove(&self.task_id);
        }
    }
}

impl<T, F: Future<Output = T>> Future for WrappedFuture<T, F> {
    type Output = T;

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        #[cfg(feature = "preempt")]
        {
            POLL_COUNT.incr();
            FUTURES_BEING_POLLED.incr();
        }

        #[cfg(feature = "preempt")]
        scopeguard::defer!({
            FUTURES_BEING_POLLED.decr();
        });

        let task_id = self.task_id;
        let btrace = self.spawn_btrace.as_ref().map(Arc::clone);
        let fut = unsafe { self.map_unchecked_mut(|v| &mut v.fut) };
        if *SMOLSCALE_PROFILE {
            let start = Instant::now();
            let result = fut.poll(cx);
            let elapsed = start.elapsed();
            let mut entry =
                PROFILE_MAP
                .entry(task_id)
                .or_insert_with(|| {
                    (btrace.unwrap(), Duration::new(0, 0) )
                });
            let mut tuple = entry.get_mut();
            tuple.1 += elapsed;
            result
        } else {
            fut.poll(cx)
        }
    }
}

impl<T, F: Future<Output = T> + 'static> WrappedFuture<T, F> {
    pub fn new(fut: F) -> Self {
        ACTIVE_TASKS.incr();

        let task_id =
            namespace_unique_id("smolscale2-task-id");

        WrappedFuture {
            task_id,
            spawn_btrace: if *SMOLSCALE_PROFILE {
                let bt = Arc::new(Backtrace::new());
                PROFILE_MAP.insert(task_id, (bt.clone(), Duration::from_secs(0))).expect("bug: duplicated task id");
                Some(bt)
            } else {
                None
            },
            fut,
        }
    }
}

/// Profiling map.
static PROFILE_MAP: Lazy<scc::HashMap<u128, (Arc<Backtrace>, Duration)>> = Lazy::new(|| {
    std::thread::Builder::new()
        .name("sscale-prof".into())
        .spawn(|| loop {
            let mut vv = vec![];
            PROFILE_MAP.scan(|k, v|{
                vv.push( (*k, v.clone()) );
            });
            vv.sort_unstable_by_key(|s| s.1 .1);
            vv.reverse();
            eprintln!("----- SMOLSCALE PROFILE -----");
            let mut tw = TabWriter::new(stderr());
            writeln!(&mut tw, "TASK ID\tCPU TIME\tBACKTRACE").unwrap();
            for (task_id, info) in vv.into_iter() {
                let (bt, duration) = info;
                writeln!(
                    &mut tw,
                    "{}\t{:?}\t{}",
                    task_id,
                    duration,
                    {
                        let s = format!("{:?}", bt);
                        format!(
                            "{:?}",
                            s.lines()
                                .filter(|l| !l.contains("smolscale")
                                    && !l.contains("spawn")
                                    && !l.contains("runtime"))
                                .take(2)
                                .map(|s| s.trim())
                                .collect::<Vec<_>>()
                        )
                    }
                )
                .unwrap();
            }
            tw.flush().unwrap();
            std::thread::sleep(Duration::from_secs(10));
        })
        .unwrap();
    Default::default()
});

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn nested_block_on() {
        assert_eq!(1u64, block_on(async { block_on(async { 1u64 }) }))
    }
}
