// Copyright 2022 The Go Authors and Bobby Powers. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

use std::num::NonZeroU32;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Limit defines the maximum frequency of some events.
/// Limit is represented as number of events per second.
/// A zero Limit allows no events.
pub type Limit = f64;

/// INF is the infinite rate limit; it allows all events (even if burst is zero).
pub const INF: Limit = f64::MAX;

/// INF_DURATION is the duration returned by Delay when a Reservation is not OK.
pub const INF_DURATION: Duration = Duration::from_nanos((1 << 63) - 1);

/// Every converts a minimum time interval between events to a Limit.
pub fn every(interval: Duration) -> Limit {
    if interval.is_zero() {
        return INF;
    }
    1f64 / interval.as_secs_f64()
}

pub struct Limiter {
    inner: Mutex<LimiterState>,
}

pub struct LimiterState {
    limit: Limit,
    burst: u64,
    tokens: f64,
    /// last is the last time the limiter's tokens field was updated
    last: Instant,
}

impl Limiter {
    pub fn new(rate: Limit, burst: u32) -> Limiter {
        let now = Instant::now();
        let inner = LimiterState {
            limit: rate,
            burst: burst as u64,
            tokens: burst as f64,
            last: now,
        };

        Limiter {
            inner: Mutex::new(inner),
        }
    }

    pub fn allow(&self) -> bool {
        self.allow_n(NonZeroU32::new(1).unwrap())
    }

    pub fn allow_n(&self, n: NonZeroU32) -> bool {
        self.reserve_n(Instant::now(), n)
    }

    fn reserve_n(&self, now: Instant, n: NonZeroU32) -> bool {
        let n = n.get() as u64;

        let mut inner = self.inner.lock().unwrap();

        if inner.limit == INF {
            return true;
        } else if inner.limit == 0.0 {
            let mut ok = false;
            if inner.burst >= n {
                inner.burst -= n;
                ok = true;
            }
            return ok;
        }

        let mut tokens = inner.advance(now);
        if tokens < (n as f64) {
            return false;
        }

        tokens -= n as f64;

        inner.last = now;
        inner.tokens = tokens;

        true
    }

    pub fn burst(&self) -> u64 {
        let inner = self.inner.lock().unwrap();
        inner.burst
    }

    pub fn limit(&self) -> Limit {
        let inner = self.inner.lock().unwrap();
        inner.limit
    }
}

/// duration_from_tokens is a unit conversion function from the number of tokens to the duration
/// of time it takes to accumulate them at a rate of limit tokens per second.
#[allow(dead_code)]
fn duration_from_tokens(limit: Limit, tokens: f64) -> Duration {
    if limit <= 0.0 {
        return INF_DURATION;
    }
    Duration::from_secs_f64(tokens / limit)
}

/// tokens_from_duration is a unit conversion function from a time duration to the number of tokens
/// which could be accumulated during that duration at a rate of limit tokens per second.
fn tokens_from_duration(limit: Limit, d: Duration) -> f64 {
    if limit <= 0.0 {
        return 0.0;
    }
    d.as_secs_f64() * limit
}

impl LimiterState {
    /// advance calculates and returns an updated state for lim resulting from the passage of time.
    /// lim is not changed.
    /// advance requires that lim.mu is held.
    fn advance(&self, now: Instant) -> f64 {
        let last = self.last;
        // TODO: in rust, this condition can never happen as we only use the monotonic clock
        // if now.Before(last) {
        //     last = now
        // }

        // Calculate the new number of tokens, due to time that passed.
        let elapsed = now.duration_since(last);
        let delta = tokens_from_duration(self.limit, elapsed);
        let mut tokens = self.tokens + delta;
        if tokens > (self.burst as f64) {
            tokens = self.burst as f64;
        }
        tokens
    }
}

#[cfg(test)]
fn close_enough(a: Limit, b: Limit) -> bool {
    let diff = ((a / b) - 1.0).abs();
    diff < 1e-9
}

#[test]
fn test_every() {
    struct Case {
        interval: Duration,
        lim: Limit,
    }

    let cases: Vec<Case> = vec![
        Case {
            interval: Duration::from_secs(0),
            lim: INF,
        },
        Case {
            interval: Duration::from_nanos(1),
            lim: 1e9,
        },
        Case {
            interval: Duration::from_micros(1),
            lim: 1e6,
        },
        Case {
            interval: Duration::from_millis(1),
            lim: 1e3,
        },
        Case {
            interval: Duration::from_millis(10),
            lim: 100.0,
        },
        Case {
            interval: Duration::from_millis(100),
            lim: 10.0,
        },
        Case {
            interval: Duration::from_secs(1),
            lim: 1.0,
        },
        Case {
            interval: Duration::from_secs(2),
            lim: 0.5,
        },
        Case {
            interval: Duration::from_secs_f64(2.5),
            lim: 0.4,
        },
        Case {
            interval: Duration::from_secs(4),
            lim: 0.25,
        },
        Case {
            interval: Duration::from_secs(10),
            lim: 0.1,
        },
        Case {
            interval: Duration::from_nanos(i64::MAX as u64),
            lim: 1e9 / (i64::MAX as f64),
        },
    ];

    for tc in cases.into_iter() {
        let lim = every(tc.interval);
        assert!(
            close_enough(tc.lim, lim),
            "Every({:?}) = {}, want {}",
            tc.interval,
            lim,
            tc.lim
        );
    }
}
