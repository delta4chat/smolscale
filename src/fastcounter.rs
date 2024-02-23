use crate::*;

/// A write-mostly, read-rarely counter
#[derive(Debug)]
pub struct FastCounter {
  counter: AtomicU128,
}
impl FastCounter {
  pub const fn new() -> Self {
    Self::from_u128(0)
  }
  pub const fn from_u128(n: u128) -> Self {
    let counter = AtomicU128::new(n);
    Self { counter }
  }

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

    for i in 0..=1000 {
      let prev = self.count();

      let maybe_new = if add {
        prev.checked_add(1)
      } else {
        prev.checked_sub(1)
      };
      let new = match maybe_new {
        Some(v) => v,
        None => {
          if add {
            log::error!("fast counter: u128 overflow! (update={op}, iterations={i})");
          }
          return None;
        },
      };

      match self
        .counter
        .compare_exchange(prev, new, Relaxed, Relaxed)
      {
        Ok(_) => {
          return Some(i);
        },
        Err(_) => {
          let mut lv = log::Level::Debug;
          if i % 100 == 0 {
            lv = log::Level::Warn;
          }
          log::log!(lv, "fast counter: compare exchange failed. (update={op}, iterations={i})");
        },
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

impl Default for FastCounter {
  fn default() -> Self {
    Self::new()
  }
}

impl From<u128> for FastCounter {
  fn from(n: u128) -> Self {
    Self::from_u128(n)
  }
}
impl From<usize> for FastCounter {
  fn from(n: usize) -> Self {
    Self::from_u128(n as u128)
  }
}

static GLOBAL_NS: Lazy<String> = Lazy::new(|| {
  let rand = fastrand::u128(..);
  let pid = std::process::id();
  format!("__GLOBAL_NAME_SPACE_{pid}_{rand}")
});

/// generates Unique ID for specified name space.
pub fn namespace_unique_id(ns: impl ToString) -> u128 {
  static CTRS: Lazy<scc::HashMap<String, FastCounter>> =
    Lazy::new(scc::HashMap::new);

  let ns = ns.to_string();
  if &ns != &*GLOBAL_NS {
    unique_id();
  }

  let ctr =
    CTRS.entry(ns).or_insert_with(FastCounter::new);
  let ctr = ctr.get();

  loop {
    let prev = ctr.count();
    let id = ctr.incr();
    if id != prev {
      return id;
    }
  }
}

pub fn named_unique_id(name: &str) -> String {
  let id = namespace_unique_id(&name);
  format!("{name}://{id}")
}

/// generates ordered per-process-uniquely ID
pub fn unique_id() -> u128 {
  namespace_unique_id(&*GLOBAL_NS)
}
