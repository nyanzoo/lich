#[cfg(feature = "tests")]
mod test;
#[cfg(feature = "tests")]
pub use test::*;

#[cfg(not(feature = "tests"))]
mod real;
#[cfg(not(feature = "tests"))]
pub use real::*;


pub trait RetryPolicy {
    fn retry(&mut self) -> bool;
}

pub struct RetryConsistent {
    count: Option<usize>,
    duration: std::time::Duration,
}

impl RetryConsistent {
    pub fn new(duration: std::time::Duration, count: Option<usize>) -> Self {
        Self { count, duration }
    }
}

impl RetryPolicy for RetryConsistent {
    fn retry(&mut self) -> bool {
        std::thread::sleep(self.duration);
        if let Some(count) = self.count.as_mut() {
            if *count == 0 {
                return false;
            }
            *count -= 1;
        }
        return true;
    }
}

pub struct RetryExponential {
    count: Option<usize>,
    duration: std::time::Duration,
    multiplier: f64,
}

impl RetryExponential {
    pub fn new(duration: std::time::Duration, multiplier: f64, count: Option<usize>) -> Self {
        Self {
            count,
            duration,
            multiplier,
        }
    }
}

impl RetryPolicy for RetryExponential {
    fn retry(&mut self) -> bool {
        std::thread::sleep(self.duration);
        if let Some(count) = self.count.as_mut() {
            if *count == 0 {
                return false;
            }
            *count -= 1;
        }
        self.duration =
            std::time::Duration::from_secs_f64(self.duration.as_secs_f64() * self.multiplier);
        return true;
    }
}