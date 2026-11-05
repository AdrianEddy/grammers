// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The handshake encrypts to the caller's RSA key that the server names by its fingerprint.

use super::*;

const NONCE: [u8; 16] = [1; 16];
const SERVER_NONCE: [u8; 16] = [2; 16];
const NEW_NONCE: [u8; 32] = [3; 32];
const RSA_RANDOM: [u8; 224] = [4; 224];

/// 1206429347 × 1218991343.
const PQ: u64 = 1470626929934143021;

/// The modulus of an RSA key made for these tests, in decimal.
const TEST_RSA_N: &str = "31061566470730259279489667733084681556760687329465786664482053075283385752695398318670954403996188080210295550160319160616597041872894207691762622979283747283485639226402698352456692782338298534733490720927201364604470915682781526321186587797060286077250259555203220248590992731187431807354201024455330793205876852867537534839988043356364719437333460963565300672398085826393289466208888352980898963016147676019135375617566069611470363866053429222394903024621590709944904411843822000898167199135593139374450988589678005432444379189352301969314906419779690143747375740419891522246405756653712906618734395560224747115139";

fn res_pq(fingerprints: &[i64]) -> tl::enums::ResPq {
    tl::types::ResPq {
        nonce: NONCE,
        server_nonce: SERVER_NONCE,
        pq: PQ.to_be_bytes().to_vec(),
        server_public_key_fingerprints: fingerprints.to_vec(),
    }
    .into()
}

fn step2_with(
    fingerprints: &[i64],
    keys: &[rsa::Key],
) -> Result<(tl::functions::ReqDhParams, Step2), Error> {
    do_step2(
        Step1 { nonce: NONCE },
        res_pq(fingerprints),
        keys,
        &NEW_NONCE,
        &RSA_RANDOM,
    )
}

#[test]
fn step2_encrypts_to_the_supplied_key() {
    let key = rsa::Key::new(TEST_RSA_N, "65537").unwrap();
    let fingerprint = key.fingerprint();

    // The server lists a key the client doesn't have, then the supplied one.
    let (request, _) = step2_with(&[1, fingerprint], std::slice::from_ref(&key)).unwrap();
    assert_eq!(request.public_key_fingerprint, fingerprint);
    // Encrypted to that key: what the server's private key decrypts.
    let inner = tl::enums::PQInnerData::Data(tl::types::PQInnerData {
        pq: PQ.to_be_bytes().to_vec(),
        p: 1206429347u32.to_be_bytes().to_vec(),
        q: 1218991343u32.to_be_bytes().to_vec(),
        nonce: NONCE,
        server_nonce: SERVER_NONCE,
        new_nonce: NEW_NONCE,
    })
    .to_bytes();
    assert_eq!(
        request.encrypted_data,
        rsa::encrypt_hashed(&inner, &key, &RSA_RANDOM)
    );

    // Telegram's keys don't include it.
    assert_eq!(
        step2_with(&[1, fingerprint], &telegram_rsa_keys()).err(),
        Some(Error::UnknownFingerprints {
            fingerprints: vec![1, fingerprint],
        })
    );
}

#[test]
fn handshake_takes_the_servers_first_known_key() {
    let mut keys = telegram_rsa_keys();
    keys.push(rsa::Key::new(TEST_RSA_N, "65537").unwrap());
    let fingerprints: Vec<i64> = keys.iter().rev().map(rsa::Key::fingerprint).collect();

    let (request, _) = step2_with(&fingerprints, &keys).unwrap();
    assert_eq!(request.public_key_fingerprint, fingerprints[0]);
}

#[test]
fn telegram_rsa_keys_have_their_fingerprints() {
    // The fingerprints Telegram's production and test datacenters send in `resPQ`.
    let fingerprints: Vec<i64> = telegram_rsa_keys()
        .iter()
        .map(rsa::Key::fingerprint)
        .collect();
    assert_eq!(fingerprints, [-3414540481677951611, -5595554452916591101]);
}
