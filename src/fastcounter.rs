use crate::{AtomicU128, Relaxed};

/// A write-mostly, read-rarely counter
#[derive(Default, Debug)]
pub struct FastCounter {
    counter: AtomicU128,
}

impl FastCounter {
    /// Increment the counter, and returns current value
    #[inline(always)]
    pub fn incr(&self) -> u128 {
        self._update(true);
        self.count()
    }

    /// Decrement the counter, and returns current value
    #[inline(always)]
    pub fn decr(&self) -> u128 {
        self._update(false);
        self.count()
    }

    #[inline(always)]
    fn _update(&self, add: bool) -> Option<usize> {
        let op = if add { "Add" } else { "Sub" };

        for i in 0.. {
            let prev = self.count();

            let maybe_new =
                if add {
                    prev.checked_add(1)
                } else {
                    prev.checked_sub(1)
                };
            let new = match maybe_new {
                Some(v) => v,
                None => {
                    log::error!("fast counter: u128 overflow! (update={op}, iterations={i})");
                    return None;
                }
            };

            match self.counter.compare_exchange(
                prev, new,
                Relaxed, Relaxed,
            ) {
                Ok(_) => {
                    return Some(i);
                },
                Err(_) => {
                    log::warn!("fast counter: compare exchange failed. (update={op}, iterations={i})");
                }
            }

            if i > 10 {
                break;
            }
        }

        None
    }

    /// Get the total count
    #[inline(always)]
    pub fn count(&self) -> u128 {
        self.counter.load(Relaxed)
    }
}
