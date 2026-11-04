// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Numbers from the server that the key exchange and two-factor authentication reject, never
//! panicking on them.

use grammers_crypto::two_factor_auth::{calculate_2fa, check_p_and_g};
use grammers_crypto::{check_dh_prime, factorize, sha256};
use num_bigint::{BigInt, BigUint, Sign};

/// Telegram's current 2048-bit `dh_prime`, a safe prime for `g` = 3.
fn dh_prime() -> Vec<u8> {
    grammers_crypto::hex::from_hex(
        "c71caeb9c6b1c9048e6c522f70f13f73980d40238e3e21c14934d037563d930f\
         48198a0aa7c14058229493d22530f4dbfa336f6e0ac925139543aed44cce7c37\
         20fd51f69458705ac68cd4fe6b6b13abdc9746512969328454f18faf8c595f64\
         2477fe96bb2a941d5bcd1d4ac8cc49880708fa9b378e3c4f3a9060bee67cf9a4\
         a4a695811051907e162753b56b0f6b410dba74d8a84b2a14b3144e0ef1284754\
         fd17ed950d5965b4b9dd46582db1178d169c6bc465b0d6ff9ca3928fef5b9ae4\
         e418fc15e83ebea0f87fa9ff5eed70050ded2849f47bf959d956850ce929851f\
         0d8115f635b105ee2e4e15d04b2454bf6f4fadf034b10403119cd8e3b92fcc5b",
    )
}

#[test]
fn bad_pq_is_error() {
    // Primes have no two factors; the search gives up after a bounded number of steps.
    for pq in [(1 << 63) - 25, (1 << 61) - 1, 1_000_000_007] {
        assert_eq!(factorize(pq), None);
    }
    // Every small number either factors correctly or is refused, without looping.
    for pq in 0..2000u64 {
        match factorize(pq) {
            Some((p, q)) => assert!(1 < p && p <= q && p * q == pq, "{pq} = {p} * {q}"),
            None => assert!(pq < 4 || pq % 2 == 1, "{pq} is even"),
        }
    }
    assert_eq!(factorize(4), Some((2, 2)));
    assert_eq!(
        factorize(1470626929934143021),
        Some((1206429347, 1218991343))
    );
}

#[test]
fn bad_dh_prime_is_rejected() {
    let p = BigUint::from_bytes_be(&dh_prime());
    assert!(check_dh_prime(&p, 3));
    // Unexpected generators, and numbers that aren't safe 2048-bit primes.
    for g in [0, 1, 8, i32::MIN] {
        assert!(!check_dh_prime(&p, g));
    }
    for n in [&p + 2u32, &p >> 1, BigUint::ZERO] {
        assert!(!check_dh_prime(&n, 3));
    }
}

#[test]
fn srp_unexpected_g_is_rejected() {
    let p = dh_prime();
    assert!(check_p_and_g(&p, &3));
    for g in [0, 1, 8, 255, -3, i32::MAX] {
        assert!(!check_p_and_g(&p, &g), "g = {g}");
        assert_eq!(
            calculate_2fa(
                b"salt1",
                b"salt2",
                &p,
                &g,
                vec![5; 256],
                vec![6; 256],
                "pass"
            ),
            None,
            "g = {g}"
        );
    }
}

#[test]
fn srp_bad_parameters_are_rejected() {
    let p = dh_prime();
    let calculate = |p: &[u8], g_b: Vec<u8>, a: Vec<u8>| {
        calculate_2fa(b"salt1", b"salt2", p, &3, g_b, a, "pass")
    };
    assert!(calculate(&p, vec![5; 256], vec![6; 256]).is_some());

    // `g_b` must be between 0 and `p`, and no number may be longer than 256 bytes.
    let mut above_p = p.clone();
    above_p[255] = above_p[255].wrapping_add(1);
    for g_b in [
        vec![0; 256],
        Vec::new(),
        p.clone(),
        above_p,
        vec![0xff; 256],
        vec![1; 257],
    ] {
        assert_eq!(calculate(&p, g_b, vec![6; 256]), None);
    }
    assert_eq!(calculate(&p, vec![5; 256], vec![6; 257]), None);

    // `p` must be a 2048-bit number in 256 bytes: shorter, longer, zero (a zero modulus would
    // panic), or a small safe prime padded to 256 bytes is refused.
    for p in [
        p[1..].to_vec(),
        [p.as_slice(), &[1]].concat(),
        vec![0; 256],
        Vec::new(),
        [vec![0; 255], vec![47]].concat(),
    ] {
        assert!(!check_p_and_g(&p, &3));
        assert_eq!(calculate(&p, vec![5; 256], vec![6; 256]), None);
    }
}

/// `n`, below 2^2048, as 256 big-endian bytes.
fn pad_to_256(n: &BigInt) -> Vec<u8> {
    let bytes = n.to_bytes_be().1;
    [vec![0; 256 - bytes.len()], bytes].concat()
}

/// SRP's `x` for the password (<https://core.telegram.org/api/srp>), computed here rather than by
/// the crate: `PH2(password, salt1, salt2)`.
fn srp_x(password: &[u8], salt1: &[u8], salt2: &[u8]) -> BigInt {
    let sh = |data: &[u8], salt: &[u8]| sha256!(salt, data, salt);
    let ph1 = sh(&sh(password, salt1), salt2);
    let dk = pbkdf2::pbkdf2_hmac_array::<sha2::Sha512, 64>(&ph1, salt1, 100000);
    BigInt::from_bytes_be(Sign::Plus, &sh(&dk, salt2))
}

#[test]
fn srp_t_outside_safety_range_is_rejected() {
    // The server's `g^b` is `t = (g_b - k*v) mod p`, and it must be inside MTProto's DH safety
    // range. Far outside are the degenerate 0, 1 and `p - 1`, which make `s_a` a value anyone can
    // compute. Built here as `g_b = (k*v + t) mod p` for a chosen `t`; a `t` in the range passes
    // (`srp_bad_parameters_are_rejected`). Each call derives the password's key, which is slow in
    // a debug build, so the cases are few.
    let (salt1, salt2, password) = (b"salt1", b"salt2", b"pass");
    let p = dh_prime();
    let big_p = BigInt::from_bytes_be(Sign::Plus, &p);
    let k = BigInt::from_bytes_be(Sign::Plus, &sha256!(&p, pad_to_256(&BigInt::from(3))));
    let v = BigInt::from(3).modpow(&srp_x(password, salt1, salt2), &big_p);
    let k_v = k * v % &big_p;
    for t in [BigInt::ZERO, BigInt::ONE, &big_p - 1u8] {
        let g_b = pad_to_256(&((&k_v + &t) % &big_p));
        assert_eq!(
            calculate_2fa(salt1, salt2, &p, &3, g_b, vec![6; 256], password),
            None,
            "t = {t}"
        );
    }
}
