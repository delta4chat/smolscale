use crate::AtomicU128;

/// A write-mostly, read-rarely counter
#[derive(Default, Debug)]
pub struct FastCounter {
    counter: AtomicU128,
}

impl FastCounter {
    /// Increment the counter
    #[inline]
    pub fn incr(&self) {
        self.counter.fetch_add(1);
    }

    /// Decrement the counter
    #[inline]
    pub fn decr(&self) {
        self.counter.fetch_sub(1);
    }

    /// Get the total count
    #[inline]
    pub fn count(&self) -> u128 {
        self.counter.load()
    }
}
