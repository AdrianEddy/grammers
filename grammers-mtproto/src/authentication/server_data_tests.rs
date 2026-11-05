// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Answers of the server during the key exchange that are errors, never a panic.

use grammers_crypto::{aes, generate_key_data_from_nonce};

use super::*;

const NONCE: [u8; 16] = [1; 16];
const SERVER_NONCE: [u8; 16] = [2; 16];
const NEW_NONCE: [u8; 32] = [3; 32];

/// The largest prime below 2^63, the bound the documentation gives `pq`.
const PRIME_PQ: u64 = (1 << 63) - 25;

fn step1() -> Step1 {
    Step1 { nonce: NONCE }
}

fn step2() -> Step2 {
    Step2 {
        nonce: NONCE,
        server_nonce: SERVER_NONCE,
        new_nonce: NEW_NONCE,
    }
}

fn res_pq(pq: &[u8]) -> tl::enums::ResPq {
    tl::types::ResPq {
        nonce: NONCE,
        server_nonce: SERVER_NONCE,
        pq: pq.to_vec(),
        // Telegram's production key.
        server_public_key_fingerprints: vec![-3414540481677951611],
    }
    .into()
}

fn server_dh_params(encrypted_answer: Vec<u8>) -> tl::enums::ServerDhParams {
    tl::types::ServerDhParamsOk {
        nonce: NONCE,
        server_nonce: SERVER_NONCE,
        encrypted_answer,
    }
    .into()
}

/// The server's answer to step 2, encrypted as the server does it, with `g` and `dh_prime`.
fn server_dh_inner_data(g: i32, dh_prime: Vec<u8>) -> tl::enums::ServerDhParams {
    let inner = tl::enums::ServerDhInnerData::Data(tl::types::ServerDhInnerData {
        nonce: NONCE,
        server_nonce: SERVER_NONCE,
        g,
        dh_prime,
        g_a: vec![2],
        server_time: 0,
    })
    .to_bytes();
    let mut answer = Sha1::digest(&inner).to_vec();
    answer.extend(&inner);
    answer.resize(answer.len().next_multiple_of(16), 0);
    let (key, iv) = generate_key_data_from_nonce(&SERVER_NONCE, &NEW_NONCE);
    aes::ige_encrypt(&mut answer, &key, &iv);
    server_dh_params(answer)
}

fn step3_error(response: tl::enums::ServerDhParams) -> Option<Error> {
    do_step3(step2(), response, &[0x11; 256], &[0; 16], 0).err()
}

#[test]
fn bad_pq_is_error() {
    // A prime, or a number too small, has no two factors to find; the search gives up.
    for pq in [PRIME_PQ, 0, 1, 2, 3] {
        assert_eq!(
            do_step2(
                step1(),
                res_pq(&pq.to_be_bytes()),
                &telegram_rsa_keys(),
                &NEW_NONCE,
                &[0; 224]
            )
            .err(),
            Some(Error::InvalidPq { pq })
        );
    }
    for size in [0, 7, 9] {
        assert_eq!(
            do_step2(
                step1(),
                res_pq(&vec![1; size]),
                &telegram_rsa_keys(),
                &NEW_NONCE,
                &[0; 224]
            )
            .err(),
            Some(Error::InvalidPqSize { size })
        );
    }

    // A product of two primes goes through.
    let pq = 1470626929934143021u64;
    let (request, _) = do_step2(
        step1(),
        res_pq(&pq.to_be_bytes()),
        &telegram_rsa_keys(),
        &NEW_NONCE,
        &[0; 224],
    )
    .unwrap();
    assert_eq!(request.p, 1206429347u32.to_be_bytes());
    assert_eq!(request.q, 1218991343u32.to_be_bytes());
}

#[test]
fn bad_dh_prime_is_error() {
    // Not the current prime, and not a safe 2048-bit one: even, one byte short, or empty, which
    // would be a modulus of zero.
    let mut even = CURRENT_DH_PRIME.to_vec();
    even[255] &= !1;
    for dh_prime in [even, CURRENT_DH_PRIME[1..].to_vec(), Vec::new()] {
        assert_eq!(
            step3_error(server_dh_inner_data(CURRENT_G, dh_prime)),
            Some(Error::InvalidDhPrime)
        );
    }

    // A generator other than 2 to 7.
    for g in [0, 1, 8, i32::MIN] {
        assert_eq!(
            step3_error(server_dh_inner_data(g, CURRENT_DH_PRIME.to_vec())),
            Some(Error::InvalidG(g))
        );
    }
}

#[test]
fn short_dh_answer_is_error() {
    // Too short to hold the answer's hash, though padded.
    for len in [0, 16] {
        assert_eq!(
            step3_error(server_dh_params(vec![0; len])),
            Some(Error::InvalidDhInnerData {
                error: tl::deserialize::Error::UnexpectedEof
            })
        );
    }
    assert_eq!(
        step3_error(server_dh_params(vec![0; 17])),
        Some(Error::EncryptedResponseNotPadded { len: 17 })
    );
}
