// Copyright 2022 The Go Authors and Bobby Powers. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

use std::io;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
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

const TIME_SHIFT: u64 = 20;
const SENTINEL_SHIFT: u64 = TIME_SHIFT - 1;
const CATCH_UNDERFLOW_SHIFT: u64 = SENTINEL_SHIFT - 1;
const TOKENS_MASK: u64 = (1 << CATCH_UNDERFLOW_SHIFT) - 1;
const MAX_TOKENS: u64 = (1 << (CATCH_UNDERFLOW_SHIFT - 1)) - 1;

// max_tries is the number of CAS loops we will go through below, and is low-ish
// to ensure we don't live-lock in some pathological scenario.  See the loop in
// reserve below for more color on why this is a fine limit.
const MAX_TRIES: u64 = 256;

// PackedState conceptually fits both "last updated" and "tokens remaining" into
// a single 64-bit value that can be read and set atomically.  The packed state
// looks like:
//
//	              1 bit: always-1 underflow catch -
//	                   1 bit: always-1 sentinel -  |
//	44-bits: microsecond-resolution duration     | | 18-bits: signed token count
//	____________________________________________ _ _ __________________
//
// The "clever" part is that we always initialize 2 bits to 1 in between the packed
// values. Immediately adjacent to the token count is a bit set to 1 ("underflow
// catch" in the diagram above).  If token count was 0 (all 18-bits are zero) and we
// decrement it (like happens in the first conditional in the loop below), the
// underflow catch bit will be distributed right and all 18-bits will now be 1 (-1
// in twos-compliment).  This ensures that race-y decrement is safe and doesn't impact
// the stored duration.  We keep an extra "sentinel" value between the underflow catch
// bit and the duration to ensure that we can tell if state has been initialized, and
// as a backstop in case the non-conditional decrements go wrong in some unknown way.
#[derive(Default)]
struct PackedStateCell {
    inner: AtomicU64,
}

impl PackedStateCell {
    fn load(&self) -> Result<UnpackedState, io::Error> {
        let state: PackedState = self.inner.load(Ordering::SeqCst);

        state.try_into()
    }
}

type PackedState = u64;

#[derive(Copy, Clone)]
struct UnpackedState {
    time_diff_micros: u64,
    tokens: i32,
}

impl UnpackedState {
    fn new(mut time_diff_micros: u64, mut tokens: i32) -> Self {
        if time_diff_micros < 0 {
            time_diff_micros = 0
        }
        if tokens < 0 {
            tokens = 0
        }

        Self {
            time_diff_micros,
            tokens,
        }
    }
}

impl Into<PackedState> for UnpackedState {
    /// into packs a microsecond-resolution time duration along with a token
    /// count into a single 64-bit value.
    fn into(self) -> PackedState {
        (self.time_diff_micros << TIME_SHIFT)
            | (0x1 << SENTINEL_SHIFT)
            | (0x1 << CATCH_UNDERFLOW_SHIFT)
            | ((self.tokens as u64) & TOKENS_MASK)
    }
}

impl TryFrom<PackedState> for UnpackedState {
    type Error = io::Error;
    fn try_from(ps: PackedState) -> Result<Self, Self::Error> {
        let tokens = {
            let tokens = (ps & TOKENS_MASK) as i32;

            // this ensures that negative values are properly represented in our widening
            // from 18 to 32 bits. Check out TestUnderflow for details.
            (tokens << (32 - CATCH_UNDERFLOW_SHIFT)) >> (32 - CATCH_UNDERFLOW_SHIFT)
        };

        let time_diff_micros = ps >> TIME_SHIFT;

        let ok = ((ps >> SENTINEL_SHIFT) & 0x1) == 0x1;
        if !ok {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "packed data corrupted",
            ));
        }

        Ok(Self {
            time_diff_micros,
            tokens,
        })
    }
}

// newPackedState packs a microsecond-resolution time duration along with a token
// count into a single 32-bit value.
fn new_packed_state(mut time_diff_micros: u64, mut tokens: i32) -> PackedState {
    if time_diff_micros < 0 {
        time_diff_micros = 0
    }
    if tokens < 0 {
        tokens = 0
    }
    (time_diff_micros << TIME_SHIFT)
        | (0x1 << SENTINEL_SHIFT)
        | (0x1 << CATCH_UNDERFLOW_SHIFT)
        | ((tokens as u64) & TOKENS_MASK)
}

pub struct Limiter {
    limit: Limit,
    base: Instant,
    reinit_mu: Mutex<()>,
    burst: AtomicI64,
    state: PackedStateCell,
}

impl Limiter {
    pub fn new(rate: Limit, burst: u32) -> Limiter {
        let now = Instant::now();

        let lim = Limiter {
            limit: rate,
            base: now,
            reinit_mu: Mutex::new(()),
            burst: AtomicI64::from(burst as i64),
            state: PackedStateCell::default(),
        };

        lim.reinit(now);

        lim
    }

    fn reinit(&self, now: Instant) {
        #[allow(unused)]
        let l = self.reinit_mu.lock().unwrap();

        if let Err(_) = self.state.load() {
            // store new packed state
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

        // let mut inner = self.inner.lock().unwrap();
        //
        // if inner.limit == INF {
        //     return true;
        // } else if inner.limit == 0.0 {
        //     let mut ok = false;
        //     if inner.burst >= n {
        //         inner.burst -= n;
        //         ok = true;
        //     }
        //     return ok;
        // }
        //
        // let (last, mut tokens) = inner.advance(now);
        // if tokens < (n as f64) {
        //     if last != inner.last {
        //         inner.last = last;
        //     }
        //     return false;
        // }
        //
        // tokens -= n as f64;
        //
        // inner.last = now;
        // inner.tokens = tokens;

        true
    }

    pub fn burst(&self) -> u64 {
        let n = self.burst.load(Ordering::SeqCst);
        if n < 0 {
            0
        } else {
            n as u64
        }
    }

    pub fn limit(&self) -> Limit {
        self.limit
    }

    pub fn tokens(&self) -> f64 {
        self.tokens_at(Instant::now())
    }

    pub fn tokens_at(&self, now: Instant) -> f64 {
        // let inner = self.inner.lock().unwrap();
        // let (_, toks) = inner.advance(now);
        // toks
        0.0
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

/// advance calculates and returns an updated state for lim resulting from the passage of time.
fn advance(
    lim: Limit,
    now: Instant,
    mut last: Instant,
    old_tokens: i32,
    burst: u32,
) -> (Instant, f64) {
    // if there is a monotonicity bug, pull last backward
    if now < last {
        last = now;
    }

    // Calculate the new number of tokens, due to time that passed.
    let elapsed = now.duration_since(last);
    let delta = tokens_from_duration(lim, elapsed);
    let mut tokens = (old_tokens as f64) + delta;
    if tokens > (burst as f64) {
        tokens = burst as f64;
    }
    (last, tokens)
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

#[cfg(test)]
const D: Duration = Duration::from_millis(100);

#[cfg(test)]
struct Times {
    t0: Instant,
    t1: Instant,
    t2: Instant,
    t3: Instant,
    t4: Instant,
    t5: Instant,
    t9: Instant,
}

#[cfg(test)]
static T: once_cell::sync::Lazy<Times> = once_cell::sync::Lazy::new(|| {
    let t0 = Instant::now();
    Times {
        t0,
        t1: t0 + (1 * D),
        t2: t0 + (2 * D),
        t3: t0 + (3 * D),
        t4: t0 + (4 * D),
        t5: t0 + (5 * D),
        t9: t0 + (9 * D),
    }
});

#[cfg(test)]
struct Allow {
    t: Instant,
    toks: f64,
    n: u32,
    ok: bool,
}

#[cfg(test)]
fn run(lim: &Limiter, allows: &[Allow]) {
    for (i, allow) in allows.iter().enumerate() {
        let toks = lim.tokens_at(allow.t);
        assert_eq!(
            allow.toks, toks,
            "step {}: lim.tokens_at({:?}) = {} want {}",
            i, allow.t, toks, allow.toks
        );
        let ok = lim.reserve_n(allow.t, NonZeroU32::new(allow.n).unwrap());
        assert_eq!(
            allow.ok, ok,
            "step {}: lim.AllowN({:?}, {}) = {} want {}",
            i, allow.t, allow.n, ok, allow.ok
        );
    }
}

#[cfg(test)]
fn req(t: Instant, toks: f64, n: u32, ok: bool) -> Allow {
    Allow { t, toks, n, ok }
}

#[test]
fn test_limiter_burst_1() {
    run(
        &Limiter::new(10.0, 1),
        &vec![
            req(T.t0, 1., 1, true),
            req(T.t0, 0., 1, false),
            req(T.t0, 0., 1, false),
            req(T.t1, 1., 1, true),
            req(T.t1, 0., 1, false),
            req(T.t1, 0., 1, false),
            req(T.t2, 1., 2, false), // burst size is 1, so n=2 always fails
            req(T.t2, 1., 1, true),
            req(T.t2, 0., 1, false),
        ],
    )
}

#[test]
fn test_limiter_burst_3() {
    run(
        &Limiter::new(10.0, 3),
        &vec![
            req(T.t0, 3., 2, true),
            req(T.t0, 1., 2, false),
            req(T.t0, 1., 1, true),
            req(T.t0, 0., 1, false),
            req(T.t1, 1., 4, false),
            req(T.t2, 2., 1, true),
            req(T.t3, 2., 1, true),
            req(T.t4, 2., 1, true),
            req(T.t4, 1., 1, true),
            req(T.t4, 0., 1, false),
            req(T.t4, 0., 1, false),
            req(T.t9, 3., 3, true),
            req(T.t9, 0., 1, false), // different from Go testcase: we don't allow 0 token requests
        ],
    )
}

#[test]
fn test_limiter_burst_jump_backwards() {
    run(
        &Limiter::new(10.0, 3),
        &vec![
            req(T.t1, 3., 1, true), // start at t1
            req(T.t0, 2., 1, true), // jump back to t0, two tokens remain
            req(T.t0, 1., 1, true),
            req(T.t0, 0., 1, false),
            req(T.t0, 0., 1, false),
            req(T.t1, 1., 1, true), // got a token
            req(T.t1, 0., 1, false),
            req(T.t1, 0., 1, false),
            req(T.t2, 1., 1, true), // got another token
            req(T.t2, 0., 1, false),
            req(T.t2, 0., 1, false),
        ],
    )
}

// Ensure that tokensFromDuration doesn't produce
// rounding errors by truncating nanoseconds.
// See golang.org/issues/34861.
#[test]
fn test_limiter_no_truncation_erors() {
    let l = Limiter::new(0.7692307692307693, 1);
    assert!(l.allow());
}
