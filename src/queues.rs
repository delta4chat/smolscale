use async_task::Runnable;

use event_listener::{Event, EventListener};

use st3::fifo::{Stealer, Worker};
use concurrent_queue::ConcurrentQueue;

use once_cell::sync::Lazy;

use crate::{FastCounter, AtomicBool, Relaxed};

static QUEUE_ID: Lazy<FastCounter> = Lazy::new(Default::default);

static GLOBAL_QUEUE_INIT: AtomicBool = AtomicBool::new(false);

/// The global task queue, also including handles for stealing from local queues.
///
/// Tasks can be pushed to it. Popping requires first subscribing to it, producing a [LocalQueue], which then can be popped from.
pub struct GlobalQueue {
    queue: ConcurrentQueue<Runnable>,
    stealers: scc::HashMap<u128, Stealer<Runnable>>,
    event: Event,
}

impl GlobalQueue {
    /// Creates a new GlobalQueue.
    /// # Panics
    /// this constructor will panic if another GlobalQueue already initialized.
    pub fn new() -> Self {
        if GLOBAL_QUEUE_INIT.load(Relaxed) {
            panic!("programming error: global queue already initialized!");
        }

        GLOBAL_QUEUE_INIT.store(true, Relaxed);

        Self {
            queue: ConcurrentQueue::unbounded(),
            stealers: scc::HashMap::new(),
            event: Event::new(),
        }
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
        self.event.notify_relaxed(usize::MAX);
    }

    /// Subscribes to tasks, returning a LocalQueue.
    pub fn subscribe(&self) -> LocalQueue<'_> {
        let worker = Worker::<Runnable>::new(1024);
        let id = loop {
            let n = QUEUE_ID.incr();
            if let Ok(_) =
                // scc::HashMap::insert() that does not overwrite if key already exists.
                self.stealers.insert(n, worker.stealer())
            {
                break n;
            } else {
                log::warn!("bug: GlobalQueue.subscribe generates duplicated id! {n:?}");
            }
        };

        LocalQueue {
            id,
            global: self,
            local: worker,
        }
    }

    /// Wait for activity
    pub fn wait(&self) -> EventListener {
        self.event.listen()
    }
}

/// A thread-local queue, bound to some [GlobalQueue].
pub struct LocalQueue<'a> {
    id: u128,
    global: &'a GlobalQueue,
    local: Worker<Runnable>,
    // next_task: RefCell<Option<Runnable>>,
}

impl<'a> Drop for LocalQueue<'a> {
    fn drop(&mut self) {
        // push all the local tasks to the global queue
        while let Some(task) = self.local.pop() {
            self.global.push(task);
        }
        // deregister the local queue from the global list
        self.global.stealers.remove(&self.id);
    }
}

impl<'a> LocalQueue<'a> {
    /// Pops a task from the local queue, other local queues, or the global queue.
    pub fn pop(&self) -> Option<Runnable> {
        self.local.pop()
            .or_else(|| { self.steal_and_pop() })
    }

    /// Pushes an item to the local queue, falling back to the global queue if the local queue is full.
    pub fn push(&self, runnable: Runnable) {
        if let Err(runnable) = self.local.push(runnable) {
            log::trace!("{} pushed globally", self.id);
            self.global.push(runnable);
        } else {
            log::trace!("{} pushed locally", self.id);
        }
    }

    /// Steals a whole batch and pops one.
    fn steal_and_pop(&self) -> Option<Runnable> {
        {
            let mut ids = vec![];
            self.global.stealers.scan(|id, _|{
                ids.push(*id);
            });
            fastrand::shuffle(&mut ids);
            for id in ids.iter() {
                let entry =
                    match self.global.stealers.get(id) {
                        Some(v) => v,
                        None => { continue; },
                    };

                if let Ok((val, count)) =
                    entry.get().steal_and_pop(
                        &self.local,
                        |n| {
                            (n / 2 + 1).min(64)
                        }
                    )
                {
                    log::trace!("{} stole {} from {id}", count + 1, self.id);
                    return Some(val);
                }
            }
        }

        // try stealing from the global
        let global = &self.global.queue;
        let global_len = global.len();
        let to_steal =
            (global_len / 2 + 1)
            .min(64)
            .min(global_len);

        for _ in 0..to_steal {
            if let Ok(stolen) = global.pop() {
                if let Err(back) = self.local.push(stolen){
                    return Some(back);
                }
            }
        }

        return self.local.pop();
    }
}
