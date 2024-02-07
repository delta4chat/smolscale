use crate::AtomicU128;

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

        match self.counter.compare_exchange(prev, prev+1) {
            Ok(v) => v,
            Err(v) => {
                log::warn!("fast counter: incr(): compare exchange failed");
                v
            }
        }
    }

    /// Decrement the counter, and returns current value
    #[inline]
    pub fn decr(&self) -> u128 {
        let prev = self.count();

        if prev == 0 {
            log::error!("fast counter overflow! try decrementing to negative number!");
            return prev;
        }

        match self.counter.compare_exchange(prev, prev-1) {
            Ok(v) => v,
            Err(v) => {
                log::warn!("fast counter: decr(): compare exchange failed");
                v
            }
        }
    }

    /// Get the total count
    #[inline]
    pub fn count(&self) -> u128 {
        self.counter.load()
    }
}
