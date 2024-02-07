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
use fastcounter::FastCounter;
use futures_lite::prelude::*;
use once_cell::sync::{Lazy, OnceCell};
use std::io::Write;
use std::{
    io::stderr,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tabwriter::TabWriter;

pub type AtomicU128 = crossbeam_utils::atomic::AtomicCell<u128>;

mod fastcounter;
pub mod immortal;
mod new_executor;
mod queues;
pub mod reaper;

//static CHANGE_THRESH: u32 = 10;
static MONITOR_MS: u64 = 1000;

static MAX_THREADS: AtomicU128 = AtomicU128::new(20);

static POLL_COUNT: Lazy<FastCounter> = Lazy::new(Default::default);

static THREAD_MAP: Lazy<scc::HashMap<u128, std::thread::JoinHandle<()>>> = Lazy::new(|| { scc::HashMap::new() });
static THREAD_SPAWN_LOCK: Lazy<parking_lot::Mutex<()>> = Lazy::new(|| { parking_lot::Mutex::new(()) });

static MONITOR: OnceCell<std::thread::JoinHandle<()>> = OnceCell::new();

static SINGLE_THREAD: AtomicBool = AtomicBool::new(false);

static SMOLSCALE_USE_AGEX: Lazy<bool> = Lazy::new(|| {
    std::env::var("SMOLSCALE_USE_AGEX").is_ok()
});

static SMOLSCALE_PROFILE: Lazy<bool> = Lazy::new(|| {
    std::env::var("SMOLSCALE_PROFILE").is_ok()
});

/// Irrevocably puts smolscale into single-threaded mode.
pub fn permanently_single_threaded() {
    set_max_threads(1);
    SINGLE_THREAD.store(true, Ordering::Relaxed);
}

pub fn set_max_threads(mut max: u128) {
    if max == 0 {
        max = 1;
    }
    MAX_THREADS.store(max);
}

/// Returns the number of running worker threads.
pub fn running_threads() -> u128 {
    let mut to_remove = vec![];
    let mut running = 0;

    THREAD_MAP.scan(|idx, join| {
        if join.is_finished() {
            let name = join.thread().name();
            log::warn!("worker thread (id={idx}, name={name:?}) exited!");
            to_remove.push(*idx);
        } else {
            running += 1;
        }
    });

    for rm_idx in to_remove {
        THREAD_MAP.remove(&rm_idx);
    }

    running
}

fn start_monitor() {
    MONITOR.get_or_init(|| {
        std::thread::Builder::new()
            .name("sscale-mon".into())
            .spawn(monitor_loop)
            .unwrap()
    });
}

fn monitor_loop() {
    if *SMOLSCALE_USE_AGEX {
        //return;
    }
    fn start_thread(exitable: bool, process_io: bool) {
        let _lock = THREAD_SPAWN_LOCK.lock();

        let current_threads: u128 = running_threads();
        let max_threads: u128 = MAX_THREADS.load();

        if current_threads >= max_threads {
            log::info!("cannot spawn new worker thread: max threads ({max_threads}) exceeded");
            return;
        }

        static THREAD_ID: Lazy<FastCounter> = Lazy::new(Default::default);
        let idx: u128 = THREAD_ID.incr();

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
                let future = async {
                    // let run_local = local_exec.run(futures_lite::future::pending::<()>());
                    if exitable {
                        new_executor::run_local_queue()
                            .or(async {
                                async_io::Timer::after(Duration::from_secs(5)).await;
                            })
                            .await;
                    } else {
                        new_executor::run_local_queue().await;
                    };
                }
                .compat();
                if process_io {
                    async_io::block_on(future)
                } else {
                    futures_lite::future::block_on(future)
                }
            })
            .expect("cannot spawn worker thread");

        THREAD_MAP.insert(idx, thread).expect("cannot add spawned worker thread to global list of join handles");
    }
    if SINGLE_THREAD.load(Ordering::Relaxed) || std::env::var("SMOLSCALE_SINGLE").is_ok() {
        start_thread(false, true);
        //return;
    } else {
        for _ in 0..num_cpus::get() {
            start_thread(false, true);
        }
    }

    std::thread::Builder::new()
        .name("sscale-panic-safe".to_string())
        .spawn(|| {
            std::thread::sleep(Duration::from_millis(MONITOR_MS));
            loop {
                if running_threads() > 0 {
                    std::thread::sleep(Duration::from_millis(MONITOR_MS));
                    continue;
                }

                log::info!("start new thread for panic safe!");
                start_thread(false, true);
            }
        })
        .expect("cannot spawn panic safe thread");

    // "Token bucket"
    let mut token_bucket = 100;
    for count in 0u64.. {
        if count % 100 == 0 && token_bucket < 100 {
            token_bucket += 1
        }
        new_executor::global_rebalance();
        if SINGLE_THREAD.load(Ordering::Relaxed) {
            //return;
        }
        #[cfg(not(feature = "preempt"))]
        {
            std::thread::sleep(Duration::from_millis(MONITOR_MS));
        }
        #[cfg(feature = "preempt")]
        {
            let before_sleep = POLL_COUNT.count();
            std::thread::sleep(Duration::from_millis(MONITOR_MS));
            let after_sleep = POLL_COUNT.count();
            let running_tasks = FUTURES_BEING_POLLED.count();
            let running_threads = running_threads();
            if after_sleep == before_sleep
                && token_bucket > 0
                && running_tasks >= running_threads
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
    log::trace!("monitor started");
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

static ACTIVE_TASKS: Lazy<FastCounter> = Lazy::new(Default::default);

static FUTURES_BEING_POLLED: Lazy<FastCounter> = Lazy::new(Default::default);

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
        POLL_COUNT.incr();
        #[cfg(feature = "preempt")]
        FUTURES_BEING_POLLED.incr();
        #[cfg(feature = "preempt")]
        scopeguard::defer!({
            FUTURES_BEING_POLLED.decr();
        });
        let task_id = self.task_id;
        let btrace = self.spawn_btrace.as_ref().map(Arc::clone);
        let fut = unsafe { self.map_unchecked_mut(|v| &mut v.fut) };
        if *SMOLSCALE_PROFILE && fastrand::u64(0..100) == 0 {
            let start = Instant::now();
            let result = fut.poll(cx);
            let elapsed = start.elapsed();
            let mut entry = PROFILE_MAP
                .entry(task_id)
                .or_insert_with(|| (btrace.unwrap(), Duration::from_secs(0)))
                .get_mut().to_owned();
            entry.1 += elapsed * 100;
            result
        } else {
            fut.poll(cx)
        }
    }
}

impl<T, F: Future<Output = T> + 'static> WrappedFuture<T, F> {
    pub fn new(fut: F) -> Self {
        ACTIVE_TASKS.incr();

        static TASK_ID: Lazy<FastCounter> = Lazy::new(Default::default);
        let task_id = TASK_ID.incr();

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
            PROFILE_MAP.scan(|k, v| {
                vv.push( (*k, v.clone()) );
            });
            vv.sort_unstable_by_key(|s| s.1 .1);
            vv.reverse();
            eprintln!("----- SMOLSCALE PROFILE -----");
            let mut tw = TabWriter::new(stderr());
            writeln!(&mut tw, "INDEX\tTOTAL\tTASK ID\tCPU TIME\tBACKTRACE").unwrap();
            for (count, (task_id, (bt, duration))) in vv.into_iter().enumerate() {
                writeln!(
                    &mut tw,
                    "{}\t{}\t{}\t{:?}\t{}",
                    count,
                    ACTIVE_TASKS.count(),
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
