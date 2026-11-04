// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

// `pq` comes from the server: a number that can't be factored is an error, never a panic.
#![cfg_attr(
    not(test),
    deny(
        clippy::panic,
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::indexing_slicing
    )
)]

fn gcd(mut a: u128, mut b: u128) -> u128 {
    while b != 0 {
        let (na, nb) = (b, a % b);
        a = na;
        b = nb;
    }
    a
}

/// How many steps of the pseudo-random sequence one attempt may take before it gives up.
///
/// `pq` is "the product of two different odd prime numbers [and] normally less than or equal to
/// 2^63-1" (<https://core.telegram.org/mtproto/auth_key>), so its smaller factor `p` is below
/// 2^32. Pollard's rho finds `p` after about the square root of `p` steps, at most some 2^16
/// here, and Brent's variant takes a small multiple of that, so the bound leaves a wide margin.
/// What it stops is a `pq` that has no such factor, such as a prime, which would otherwise take
/// up to `pq` steps.
const MAX_STEPS: u32 = 1 << 20;

/// Factorize the given number into its two prime factors.
///
/// The algorithm here is a faster variant of [Pollard's rho algorithm],
/// published by [Richard Brent], based on
/// <https://comeoncodeon.wordpress.com/2010/09/18/pollard-rho-brent-integer-factorization/>.
///
/// Returns `(p, q)` with `1 < p <= q` and `p * q == pq`, or `None` if no such pair was found
/// within a bounded number of steps: `pq` comes from the server, which may send a prime, or a
/// number too small to factor.
///
/// Pollard's rho algorithm: <https://en.wikipedia.org/wiki/Pollard%27s_rho_algorithm>
/// Richard Brent: <https://maths-people.anu.edu.au/~brent/pd/rpb051i.pdf>
#[allow(clippy::many_single_char_names)]
pub fn factorize(pq: u64) -> Option<(u64, u64)> {
    const ATTEMPTS: [u64; 5] = [43, 47, 53, 59, 61];
    // The smallest number with two factors above 1 is 4.
    if pq < 4 {
        return None;
    }
    if pq.is_multiple_of(2) {
        return Some((2, pq / 2));
    }
    for attempt in ATTEMPTS {
        // > Note that this algorithm may not find the factors and will return failure for composite n.
        // > In that case, use a different f(x) and try again [...] We choose f(x) = x*x + c
        // Thus by choosing a different `c` we're changing `f(x)` and can try again.
        // Prime factors are used for the attempts in the hopes they'll be more likely to work.
        let c = attempt * (pq / 103);
        if let Some(p) = factor_with_param(pq, c) {
            let q = pq / p;
            return Some((p.min(q), p.max(q)));
        }
    }
    None
}

/// Find a factor of the odd `pq` strictly between 1 and `pq`, in at most [`MAX_STEPS`] steps.
fn factor_with_param(pq: u64, c: u64) -> Option<u64> {
    let pq = pq as u128;
    let c = u128::from(c);
    fn abs_sub(a: u128, b: u128) -> u128 {
        a.max(b) - a.min(b)
    }

    let mut steps_left = MAX_STEPS;
    // One step of the sequence `y = f(y) = (y * y + c) mod pq`, or `None` once the attempt has
    // taken its steps. `y < pq < 2^64` and `c < 2^64`, so `y * y + c < 2^128` can't overflow.
    let mut step = |y: u128| {
        steps_left = steps_left.checked_sub(1)?;
        Some((y * y + c) % pq)
    };

    // Random values in the range of 1..pq, chosen by fair dice roll.
    // c is an input free to change in case the chosen value fails.
    let mut y = 3 * (pq / 7);
    // At least one step per block, or a small `pq` would never take a step.
    let m = (7 * (pq / 13)).max(1);
    let mut g = 1u128;
    let mut r = 1u128;
    let mut q = 1u128;
    let mut x = 0u128;
    let mut ys = 0u128;

    while g == 1 {
        x = y;
        for _ in 0..r {
            y = step(y)?;
        }

        let mut k = 0;
        while k < r && g == 1 {
            ys = y;
            for _ in 0..m.min(r - k) {
                y = step(y)?;
                q = (q * abs_sub(x, y)) % pq;
            }

            g = gcd(q, pq);
            k += m;
        }

        r *= 2;
    }

    if g == pq {
        loop {
            ys = step(ys)?;
            g = gcd(abs_sub(x, ys), pq);
            if g > 1 {
                break;
            }
        }
    }

    // `g` divides `pq`; a factor of 1 or `pq` itself isn't one of the two.
    (g > 1 && g < pq).then_some(g as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_factorization_1() {
        let pq = factorize(1470626929934143021);
        assert_eq!(pq, Some((1206429347, 1218991343)));
    }

    #[test]
    fn test_factorization_2() {
        let pq = factorize(2363612107535801713);
        assert_eq!(pq, Some((1518968219, 1556064227)));
    }

    #[test]
    fn test_factorization_3() {
        let pq = factorize(2804275833720261793);
        assert_eq!(pq, Some((1555252417, 1803100129)));
    }
}
