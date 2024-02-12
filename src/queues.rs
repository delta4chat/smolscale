use async_task::Runnable;

use event_listener::{Event, EventListener};
use futures_intrusive::sync::LocalManualResetEvent;

use st3::fifo::{Stealer, Worker};
use concurrent_queue::ConcurrentQueue;

use core::cell::{Cell, RefCell};
use once_cell::sync::Lazy;

use std::time::Instant;
use std::rc::Rc;

use crate::*;

pub static MIN_SFO_AMOUNT: Lazy<u128> =
    Lazy::new(||{
        num_cpus::get_physical() as u128
    });

pub static GLOBAL_QUEUE: Lazy<GlobalQueue> =
    Lazy::new(||{
        let dynamic_sfo = MAX_THREADS.load(Relaxed) / 4;
        GlobalQueue::new(
            MIN_SFO_AMOUNT.max(dynamic_sfo)
        )
    });

/// The global task queue, also including handles for stealing from local queues.
///
/// Tasks can be pushed to it. Popping requires first subscribing to it, producing a [LocalQueue], which then can be popped from.
#[derive(Debug)]
pub struct GlobalQueue {
    pub(crate) locals:
        scc::TreeIndex<u128, Arc<LocalQueue>>,

    queue: ConcurrentQueue<Runnable>,
    event: Event,

    sfo_amount: FastCounter,
}
unsafe impl Send for GlobalQueue {}
unsafe impl Sync for GlobalQueue {}

impl GlobalQueue {
    /// Creates a new GlobalQueue.
    /// # Panics
    /// this constructor will panic if another GlobalQueue already initialized.
    pub fn new(sfo: u128) -> Self {
        if Lazy::get(&GLOBAL_QUEUE).is_some() {
            panic!("programming error: global queue already initialized!");
        }

        Self {
            queue: ConcurrentQueue::unbounded(),
            sfo_amount: FastCounter::from(sfo),
            locals: scc::TreeIndex::new(),
            event: Event::new(),
        }
    }

    fn stealers(&self)
        -> Vec<(u128, Stealer<Runnable>)>
    {
        let mut x = vec![];

        let guard = scc::ebr::Guard::new();
        for (id, local) in self.locals.iter(&guard) {
            x.push(
                ( *id, local.worker.stealer() )
            );
        }
        core::mem::drop(guard);

        x
    }

    /// Pushes a task to the GlobalQueue, notifying at least one [LocalQueue].  
    pub fn push(&self, task: Runnable) {
        if let Err(err) = self.queue.push(task) {
            log::error!("cannot push to global queue! error={err:?}");
        }

        self.event.notify(1);
    }

    /// Rebalances the executor.
    pub fn rebalance(&self) {
        log::trace!("global queue: rebalance()");
        self.event.notify_relaxed(usize::MAX);
    }

    /// Subscribes to tasks, returning a LocalQueue.
    pub fn subscribe(&self) -> Arc<LocalQueue> {
        let stealing_from_others: bool =
            if self.sfo_amount.count() > 0 {
                // spent one token from "sfo balances"
                self.sfo_amount.decr();
                // allow this LocalQueue to stealing tasks from others
                true
            } else {
                false
            };

        let local =
            Arc::new(LocalQueue {
                id: namespace_unique_id(
                    "smolscale2-localqueue-id"
                ),

                worker: Worker::<Runnable>::new(1024),
                stealing_from_others,

                thread: RefCell::new(None),

                event: Rc::new(
                    LocalManualResetEvent::new(false)
                ),

                backlogs: FastCounter::new(),
                cpu_usage: scc::TreeIndex::new(),
                idle: Cell::new(true),
            });

        self.locals.insert( local.id, local.clone() )
            .unwrap();

        log::info!("LocalQueue {} has been subscribed", local.id);
        local
    }

    /// Wait for activity
    pub fn wait(&self) -> EventListener {
        self.event.listen()
    }
}

/// A thread-local queue, bound to GLOBAL_QUEUE.
pub struct LocalQueue {
    /// unique identifier of this LocalQueue.
    /// (private, read-only field)
    id: u128,

    /// Worker queue for this LocalQueue
    pub(crate) worker: Worker<Runnable>,

    /// event pub/sub
    pub(crate) event: Rc<LocalManualResetEvent>,

    /// Whether this LocalQueue active and ready to handle tasks? and running by which thread?
    pub(crate) thread:
        RefCell<Option<std::thread::Thread>>,

    /// Whether allows to stealing task from others LocalQueue ?
    pub(crate) stealing_from_others: bool,

    // pref
    backlogs: FastCounter,
    cpu_usage: scc::TreeIndex<Instant, f64>,
    idle: Cell<bool>,
}

impl Drop for LocalQueue {
    fn drop(&mut self) {
        // deregister this LocalQueue from GLOBAL_QUEUE.
        GLOBAL_QUEUE.locals.remove(&self.id);

        // push all the local tasks to the global queue
        while let Some(runnable) = self.worker.pop() {
            GLOBAL_QUEUE.push(runnable);
        }
    }
}

impl core::fmt::Debug for LocalQueue {
    fn fmt(&self, f: &mut core::fmt::Formatter)
        -> Result<(), core::fmt::Error>
    {
        let g = scc::ebr::Guard::new();
        f.debug_struct("LocalQueue")
            .field("EPOCH", &Self::EPOCH)
            .field("id", &self.id())
            .field("active", &self.active())
            .field("idle", &self.idle.get())
            .field("backlogs", &self.backlogs.count())
            .field("cpu_usage",
                   &self.cpu_usage.iter(&g).map(|(_,v)|*v).collect::<Vec<f64>>().pop().unwrap_or(f64::NAN)
            )
            .finish()
    }
}

impl LocalQueue {
    // within ten minutes
    const EPOCH: Duration = Duration::from_secs(600);

    /* ===== read-only Getters ===== */
    pub fn id(&self) -> u128 {
        self.id
    }
    pub fn idle(&self) -> bool {
        let val = self.idle.get();
        log::debug!(
            "LocalQueue {} = {}",
            self.id,
            if val {
                "IDLE"
            } else {
                "WORKING"
            }
        );
        val
    }
    pub fn active(&self) -> bool {
        self.thread().is_some()
    }
    pub fn thread(&self)
        -> Option<std::thread::Thread>
    {
        if let Some(ref thr) = *self.thread.borrow() {
            Some( thr.clone() )
        } else {
            None
        }
    }

    /// CPU usage of this worker thread
    pub fn cpu_usage(&self) -> f64 {
        let usage_bits: Vec<u128> =
            self._cpu_usages_within_epoch()
            .iter()
            .map(|x| { x.to_bits() as u128 })
            .collect();

        let avg_bits = average(&usage_bits);
        let avg = f64::from_bits(
            if avg_bits > (u64::MAX as u128) {
                log::error!("overflow in LocalQueue.cpu_usage()");
                u64::MAX
            } else {
                avg_bits as u64
            }
        );

        log::debug!("LocalQueue {}: average cpu usage = {:?}", self.id, avg);

        if avg.is_nan() {
            f64::MAX
        } else {
            avg
        }
    }
    fn _cpu_usages_within_epoch(&self) -> Vec<f64> {
        let mut usages = vec![];
        let mut to_remove = vec![];

        let guard = scc::ebr::Guard::new();
        for (ts, usage) in self.cpu_usage.iter(&guard) {
            if ts.elapsed() > Self::EPOCH {
                to_remove.push(*ts);
            } else {
                usages.push(*usage);
            }
        }
        core::mem::drop(guard);

        for key in to_remove.iter() {
            self.cpu_usage.remove(key);
        }

        return usages;

    }

    /// how many works done within this epoch?
    fn workload(&self) -> usize {
        self._cpu_usages_within_epoch().len()
    }
    fn backlogs(&self) -> u128 {
        self.backlogs.count()
    }

    /// short-term and long-term workload stress
    pub fn stress(&self) -> u128 {
        let wl = self.workload() as u128;
        let bl = self.backlogs();

        log::debug!("LocalQueue {}: workload={wl}, backlogs={bl}", self.id);
        wl + bl
    }

    /// Ticks this LocalQueue.
    /// running until the queue exhausted.
    pub fn tick(&self) {
        self.idle.set(false);
        scopeguard::defer!({
            self.idle.set(true);
        });

        // handle a bulk of Runnable
        while let Some(runnable) = self.pop() {
            self._record_cpu_usage();

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
        }
    }

    fn _get_cpu_usage() -> anyhow::Result<f64> {
        let mut stat = perfmon::cpu::ThreadStat::cur()?;
        let usage: f64 = stat.cpu()?;

        Ok(usage)
    }

    fn _record_cpu_usage(&self) {
        let usage: f64 =
            Self::_get_cpu_usage().unwrap_or(1.0);

        let now = Instant::now();
        let _ = self.cpu_usage.insert(now, usage);
    }

    /// if `idle_timeout` is None, then running this LocalQueue forever
    /// otherwise this method will exit if idle within the specified duration.
    ///
    /// # Panics
    /// this method will panic if this LocalQueue already running by another thread
    pub(crate) async fn run(
        &self,
        idle_timeout: Option<Duration>
    ) {
        // make easier to debug: prevent to running this parallels
        if self.active() {
            panic!("programming bug: this LocalQueue is already running!");
        }

        self.thread.replace(
            Some( std::thread::current() )
        );
        scopeguard::defer!({
            self.thread.replace(None);
        });

        /* to processing tasks... */

        let mut running: bool = true;

        while running {
            // subscribe
            let local_evt = async {
                self.event.wait().await;
                self.event.reset();
                true
            };
            let global_evt = async {
                GLOBAL_QUEUE.wait().await;
                true
            };
            let evt = local_evt.or(global_evt);

            // Ticks this local queue
            self.tick();
            if fastrand::u8(..) < 5 {
                futures_lite::future::yield_now().await;
            }

            // wait now, so that when we get woken up, we *know* that something happened to the local-queue or global-queue.
            if let Some(t) = idle_timeout {
                running = evt.or(async {
                    async_io::Timer::after(t).await;
                    false
                }).await;
            } else {
                evt.await;
            }
        }
    }

    /// 1. Pops a task from this LocalQueue itself.
    /// 2. if this LocalQueue is empty, then try stealing task from other local queues (if allowed).
    /// 3. if all others LocalQueue is still empty (or disallowed to stealing from others), then fallback to stealing task from GlobalQueue anyway.
    pub fn pop(&self) -> Option<Runnable> {
        let maybe = self.worker.pop();
        if maybe.is_some() {
            if self.backlogs.count() > 0 {
                self.backlogs.decr();
            }
        }

        maybe.or_else(|| { self.steal_and_pop() })
    }

    /// Pushes an item to the local queue, falling back to the global queue if the local queue is full.
    pub fn push(&self, runnable: Runnable) {
        if let Err(runnable)=self.worker.push(runnable){
            log::trace!("LocalQueue {}: new task pushed globally", self.id);
            GLOBAL_QUEUE.push(runnable);
        } else {
            log::trace!("LocalQueue {}: new task pushed locally", self.id);
            // notify the worker thread for new runnable
            self.event.set();
            self.backlogs.incr();
        }
    }

    /// Steals a whole batch and pops one.
    fn steal_and_pop(&self) -> Option<Runnable> {
        // try stealing from other local queues
        if self.stealing_from_others {
            let mut stealers = GLOBAL_QUEUE.stealers();
            fastrand::shuffle(&mut stealers);

            for (id, stealer) in stealers.into_iter() {
                if let Ok((val, count)) =
                    stealer.steal_and_pop(
                        &self.worker,
                        |n| {
                            (n / 2 + 1).min(64)
                        }
                    )
                {
                    let count =
                        count.checked_add(1)
                        .unwrap_or(count);

                    GLOBAL_QUEUE.locals.peek_with(
                        &id,
                        |_, lq| {
                            lq.backlogs.decr();
                        }
                    );

                    log::debug!("stolen {count} tasks from {id} to {}", self.id);
                    return Some(val);
                }
            }
        }

        // try stealing from the global queue
        let global = &GLOBAL_QUEUE.queue;
        let global_len = global.len();
        let to_steal =
            ( (global_len / 2) + 1 )
            .min(64)
            .min(global_len);

        for _ in 0..to_steal {
            if let Ok(stolen) = global.pop() {
                if let Err(back) = self.worker.push(stolen){
                    return Some(back);
                }
            }
        }

        self.worker.pop()
    }
}
