use crate::{AtomicU128, Relaxed};

/// A write-mostly, read-rarely counter
#[derive(Default, Debug)]
pub struct FastCounter {
    counter: AtomicU128,
}

impl FastCounter {
    /// Increment the counter, and returns current value
    #[inline]
    pub fn incr(&self) -> u128 {
        let prev = self.count();

        if prev == u128::MAX {
            log::error!("fast counter overflow! try incrementing the max value of unsigned 128-bit integer");
            return prev;
        }

        match self.counter.compare_exchange(
            prev, prev + 1,
            Relaxed, Relaxed
        ) {
            Ok(_) => {},
            Err(_) => {
                log::warn!("fast counter: incr(): compare exchange failed");
            }
        }

        self.count()
    }

    /// Decrement the counter, and returns current value
    #[inline]
    pub fn decr(&self) -> u128 {
        let prev = self.count();

        if prev == 0 {
            log::error!("fast counter overflow! try decrementing to negative number!");
            return prev;
        }

        match self.counter.compare_exchange(
            prev, prev - 1,
            Relaxed, Relaxed
        ) {
            Ok(_) => {},
            Err(_) => {
                log::warn!("fast counter: decr(): compare exchange failed");
            }
        }

        self.count()
    }

    /// Get the total count
    #[inline]
    pub fn count(&self) -> u128 {
        self.counter.load(Relaxed)
    }
}
