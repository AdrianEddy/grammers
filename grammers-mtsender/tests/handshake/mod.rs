// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The server's half of MTProto's authorization key handshake
//! (<https://core.telegram.org/mtproto/auth_key>), for the tests whose connections make a new key:
//! `req_pq_multi` gets `resPQ`, `req_DH_params` (RSA_PAD, decrypted with the private half of
//! [`TestRsaKey`]) gets `server_DH_params_ok`, and `set_client_DH_params` gets `dh_gen_ok` and
//! makes the key. It is deterministic: its nonce, its Diffie-Hellman secret and its padding come
//! from fixed seeds.

use grammers_crypto::{AuthKey, aes, generate_key_data_from_nonce, rsa};
use grammers_tl_types::{self as tl, Cursor, Deserializable, Serializable};
use num_bigint::BigUint;
use sha1::Sha1;
use sha2::{Digest, Sha256};

/// Telegram's 2048-bit safe prime for `g = 3`, the one clients know.
const DH_PRIME: [u8; 256] = [
    0xC7, 0x1C, 0xAE, 0xB9, 0xC6, 0xB1, 0xC9, 0x04, 0x8E, 0x6C, 0x52, 0x2F, 0x70, 0xF1, 0x3F, 0x73,
    0x98, 0x0D, 0x40, 0x23, 0x8E, 0x3E, 0x21, 0xC1, 0x49, 0x34, 0xD0, 0x37, 0x56, 0x3D, 0x93, 0x0F,
    0x48, 0x19, 0x8A, 0x0A, 0xA7, 0xC1, 0x40, 0x58, 0x22, 0x94, 0x93, 0xD2, 0x25, 0x30, 0xF4, 0xDB,
    0xFA, 0x33, 0x6F, 0x6E, 0x0A, 0xC9, 0x25, 0x13, 0x95, 0x43, 0xAE, 0xD4, 0x4C, 0xCE, 0x7C, 0x37,
    0x20, 0xFD, 0x51, 0xF6, 0x94, 0x58, 0x70, 0x5A, 0xC6, 0x8C, 0xD4, 0xFE, 0x6B, 0x6B, 0x13, 0xAB,
    0xDC, 0x97, 0x46, 0x51, 0x29, 0x69, 0x32, 0x84, 0x54, 0xF1, 0x8F, 0xAF, 0x8C, 0x59, 0x5F, 0x64,
    0x24, 0x77, 0xFE, 0x96, 0xBB, 0x2A, 0x94, 0x1D, 0x5B, 0xCD, 0x1D, 0x4A, 0xC8, 0xCC, 0x49, 0x88,
    0x07, 0x08, 0xFA, 0x9B, 0x37, 0x8E, 0x3C, 0x4F, 0x3A, 0x90, 0x60, 0xBE, 0xE6, 0x7C, 0xF9, 0xA4,
    0xA4, 0xA6, 0x95, 0x81, 0x10, 0x51, 0x90, 0x7E, 0x16, 0x27, 0x53, 0xB5, 0x6B, 0x0F, 0x6B, 0x41,
    0x0D, 0xBA, 0x74, 0xD8, 0xA8, 0x4B, 0x2A, 0x14, 0xB3, 0x14, 0x4E, 0x0E, 0xF1, 0x28, 0x47, 0x54,
    0xFD, 0x17, 0xED, 0x95, 0x0D, 0x59, 0x65, 0xB4, 0xB9, 0xDD, 0x46, 0x58, 0x2D, 0xB1, 0x17, 0x8D,
    0x16, 0x9C, 0x6B, 0xC4, 0x65, 0xB0, 0xD6, 0xFF, 0x9C, 0xA3, 0x92, 0x8F, 0xEF, 0x5B, 0x9A, 0xE4,
    0xE4, 0x18, 0xFC, 0x15, 0xE8, 0x3E, 0xBE, 0xA0, 0xF8, 0x7F, 0xA9, 0xFF, 0x5E, 0xED, 0x70, 0x05,
    0x0D, 0xED, 0x28, 0x49, 0xF4, 0x7B, 0xF9, 0x59, 0xD9, 0x56, 0x85, 0x0C, 0xE9, 0x29, 0x85, 0x1F,
    0x0D, 0x81, 0x15, 0xF6, 0x35, 0xB1, 0x05, 0xEE, 0x2E, 0x4E, 0x15, 0xD0, 0x4B, 0x24, 0x54, 0xBF,
    0x6F, 0x4F, 0xAD, 0xF0, 0x34, 0xB1, 0x04, 0x03, 0x11, 0x9C, 0xD8, 0xE3, 0xB9, 0x2F, 0xCC, 0x5B,
];

const G: i32 = 3;

/// The factors of the `pq` the server sends, the specification's worked example
/// (`pq = 0x17ED48941A08F981`): the client factorizes `pq` as its proof of work.
const P: u32 = 0x494C_553B;
const Q: u32 = 0x5391_1073;

/// The primes of [`TestRsaKey`], big-endian: a 2048-bit key for tests only, whose private half is
/// public here. For `x` in `p`, `q`: the smallest prime at or above the 1024-bit number
/// `SHA-512("tg-fake test RSA key x.0") ‖ SHA-512("tg-fake test RSA key x.1")` with its top two bits
/// set, such that 65537 doesn't divide the prime minus one.
const TEST_KEY_P: [u8; 128] = [
    0xF4, 0x5B, 0x9F, 0x25, 0x45, 0xC8, 0xE0, 0x6D, 0xD4, 0x89, 0xA3, 0x02, 0x9C, 0x88, 0x23, 0xAA,
    0xBB, 0x69, 0x39, 0xC1, 0xA3, 0xD7, 0xE0, 0x3D, 0x21, 0x02, 0x99, 0xB3, 0xD6, 0x29, 0x73, 0x65,
    0xF5, 0x24, 0xAC, 0xAD, 0xBC, 0xDD, 0x7F, 0xD0, 0xA5, 0x02, 0x91, 0x03, 0x82, 0x04, 0x5E, 0xDF,
    0xCB, 0x31, 0xAF, 0x3C, 0xD0, 0x70, 0x89, 0xB6, 0xEE, 0xED, 0xAB, 0xC8, 0x10, 0x76, 0x5E, 0x75,
    0xB4, 0x67, 0x2C, 0xBA, 0xF7, 0x47, 0x58, 0x08, 0x66, 0x89, 0x77, 0x2F, 0xD5, 0xF7, 0xC3, 0xA2,
    0x03, 0xAB, 0x1B, 0x70, 0xD4, 0x0B, 0xBA, 0x71, 0x1D, 0x8B, 0xA5, 0x5F, 0x75, 0x43, 0x04, 0xAC,
    0x96, 0x58, 0x06, 0xD8, 0xA8, 0xA2, 0x3A, 0x9A, 0x9C, 0x90, 0xFB, 0xEA, 0x78, 0xE3, 0xCE, 0xC5,
    0xDC, 0xD0, 0x5E, 0x70, 0x57, 0xE4, 0x9E, 0xCA, 0x7A, 0x1D, 0xE6, 0x0A, 0x59, 0xF1, 0x3B, 0x4F,
];
const TEST_KEY_Q: [u8; 128] = [
    0xCE, 0x1A, 0x56, 0x11, 0xA6, 0xD4, 0xBF, 0x15, 0x99, 0x3B, 0xF1, 0x13, 0xF1, 0x1C, 0x35, 0xCC,
    0xB6, 0x4B, 0x20, 0x8D, 0xB4, 0x4F, 0x40, 0xB0, 0x7E, 0xDD, 0xE3, 0xF6, 0x32, 0xE2, 0xFE, 0x95,
    0xF8, 0x69, 0x6E, 0xCC, 0x4E, 0x4E, 0x4F, 0xB1, 0x65, 0xE4, 0x75, 0x5D, 0x6C, 0x6D, 0xE5, 0xCE,
    0x00, 0xB5, 0x25, 0x15, 0x0E, 0x1C, 0x61, 0x0A, 0x55, 0x33, 0x61, 0x8D, 0x74, 0xF7, 0x54, 0x55,
    0x11, 0x08, 0x33, 0x54, 0xCC, 0x2C, 0x7B, 0xE4, 0xB9, 0x92, 0xCA, 0xF2, 0x6A, 0x53, 0xE0, 0x3D,
    0x14, 0xB1, 0xBD, 0xB7, 0xF1, 0xEA, 0xA6, 0x86, 0x05, 0x1B, 0x14, 0x79, 0x4A, 0x13, 0x06, 0xE9,
    0x97, 0xB9, 0x61, 0x97, 0x85, 0x83, 0x60, 0xB9, 0x31, 0x11, 0x16, 0x87, 0x85, 0x46, 0x2D, 0x6B,
    0xCD, 0xA7, 0xB5, 0x5D, 0x60, 0x48, 0x08, 0x62, 0x3D, 0x27, 0xC2, 0x04, 0x51, 0xA1, 0xB6, 0x1B,
];

/// A 2048-bit RSA key with `e = 65537` whose private half the server holds.
pub struct TestRsaKey {
    n: BigUint,
    d: BigUint,
}

impl TestRsaKey {
    pub fn new() -> Self {
        let one = BigUint::from(1u8);
        let p = BigUint::from_bytes_be(&TEST_KEY_P);
        let q = BigUint::from_bytes_be(&TEST_KEY_Q);
        let n = &p * &q;
        let d = BigUint::from(65_537u32)
            .modinv(&((&p - &one) * (&q - &one)))
            .unwrap();
        Self { n, d }
    }

    /// The public half, as a client takes it in `ConnectionParams::server_rsa_keys`.
    pub fn public(&self) -> rsa::Key {
        rsa::Key::new(&self.n.to_str_radix(10), "65537").unwrap()
    }

    /// `c^d mod n`, as 256 big-endian bytes.
    fn decrypt(&self, ciphertext: &[u8]) -> [u8; 256] {
        let m = BigUint::from_bytes_be(ciphertext)
            .modpow(&self.d, &self.n)
            .to_bytes_be();
        let mut out = [0; 256];
        out[256 - m.len()..].copy_from_slice(&m);
        out
    }
}

/// `n` bytes of a deterministic stream: SHA-256 of `seed` and a counter, block after block.
fn stream(seed: &[u8], n: usize) -> Vec<u8> {
    (0u32..)
        .flat_map(|i| {
            Sha256::new()
                .chain_update(seed)
                .chain_update(i.to_le_bytes())
                .finalize()
        })
        .take(n)
        .collect()
}

/// `2^(2048 - 64) < x < dh_prime - 2^(2048 - 64)`, the specification's range for `g_a` and `g_b`.
fn in_safe_range(x: &BigUint, prime: &BigUint) -> bool {
    let margin = BigUint::from(1u8) << (2048 - 64);
    *x > margin && *x < prime - &margin
}

/// RSA_PAD undone: `temp_key_xor ‖ aes_encrypted` gives `temp_key`, then `data_with_hash`, whose
/// hash is checked, then `data_with_padding`.
fn rsa_unpad(mut decrypted: [u8; 256]) -> Vec<u8> {
    let (temp_key_xor, aes_encrypted) = decrypted.split_at_mut(32);
    let mut temp_key: [u8; 32] = temp_key_xor.try_into().unwrap();
    for (k, h) in temp_key.iter_mut().zip(Sha256::digest(&*aes_encrypted)) {
        *k ^= h;
    }
    aes::ige_decrypt(aes_encrypted, &temp_key, &[0; 32]);
    let (reversed, hash) = aes_encrypted.split_at(192);
    let data: Vec<u8> = reversed.iter().rev().copied().collect();
    let expected = Sha256::new()
        .chain_update(temp_key)
        .chain_update(&data)
        .finalize();
    assert_eq!(expected.as_slice(), hash, "RSA_PAD's hash");
    data
}

/// Where the handshake of one connection stands.
enum State {
    Pq,
    Dh {
        nonce: [u8; 16],
    },
    ClientDh {
        nonce: [u8; 16],
        new_nonce: [u8; 32],
        a: BigUint,
    },
}

/// What the server answers a handshake message with.
pub enum Step {
    /// The next answer.
    Reply(Vec<u8>),
    /// `dh_gen_ok`, and the key the handshake made.
    Done(Vec<u8>, Box<[u8; 256]>),
}

/// The server's half of one connection's handshake.
pub struct Handshake<'k> {
    key: &'k TestRsaKey,
    state: State,
}

impl<'k> Handshake<'k> {
    pub fn new(key: &'k TestRsaKey) -> Self {
        Self {
            key,
            state: State::Pq,
        }
    }

    /// The server's nonce, the same in every handshake.
    const SERVER_NONCE: [u8; 16] = [9; 16];

    /// Answer the client's next handshake message, `body`. Panics on one out of order, or one
    /// whose checks fail: the client under test sends neither.
    pub fn step(&mut self, body: &[u8]) -> Step {
        let server_nonce = Self::SERVER_NONCE;
        match std::mem::replace(&mut self.state, State::Pq) {
            State::Pq => {
                let request = tl::functions::ReqPqMulti::from_boxed_bytes(body).unwrap();
                let pq = u64::from(P) * u64::from(Q);
                self.state = State::Dh {
                    nonce: request.nonce,
                };
                let fingerprint = self.key.public().fingerprint();
                Step::Reply(
                    tl::enums::ResPq::Pq(tl::types::ResPq {
                        nonce: request.nonce,
                        server_nonce,
                        pq: pq.to_be_bytes().to_vec(),
                        server_public_key_fingerprints: vec![fingerprint],
                    })
                    .to_bytes(),
                )
            }
            State::Dh { nonce } => {
                let request = tl::functions::ReqDhParams::from_boxed_bytes(body).unwrap();
                assert_eq!((request.nonce, request.server_nonce), (nonce, server_nonce));
                assert_eq!(
                    (&request.p[..], &request.q[..]),
                    (&P.to_be_bytes()[..], &Q.to_be_bytes()[..])
                );
                let data = rsa_unpad(self.key.decrypt(&request.encrypted_data));
                let new_nonce = match tl::enums::PQInnerData::from_bytes(&data).unwrap() {
                    tl::enums::PQInnerData::Data(inner) => inner.new_nonce,
                    tl::enums::PQInnerData::Dc(inner) => inner.new_nonce,
                    _ => panic!("a permanent key's inner data"),
                };
                let prime = BigUint::from_bytes_be(&DH_PRIME);
                let (a, g_a) = (0u8..)
                    .map(|i| {
                        let a = BigUint::from_bytes_be(&stream(&[b'a', i], 256));
                        let g_a = BigUint::from(G.unsigned_abs()).modpow(&a, &prime);
                        (a, g_a)
                    })
                    .find(|(_, g_a)| in_safe_range(g_a, &prime))
                    .unwrap();
                let inner = tl::enums::ServerDhInnerData::Data(tl::types::ServerDhInnerData {
                    nonce,
                    server_nonce,
                    g: G,
                    dh_prime: DH_PRIME.to_vec(),
                    g_a: g_a.to_bytes_be(),
                    server_time: std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i32,
                })
                .to_bytes();
                // answer_with_hash := SHA1(answer) + answer + padding, whole AES blocks.
                let mut answer = Sha1::digest(&inner).to_vec();
                answer.extend(&inner);
                let padding = (16 - answer.len() % 16) % 16;
                answer.extend(stream(b"padding", padding));
                let (key, iv) = generate_key_data_from_nonce(&server_nonce, &new_nonce);
                aes::ige_encrypt(&mut answer, &key, &iv);
                self.state = State::ClientDh {
                    nonce,
                    new_nonce,
                    a,
                };
                Step::Reply(
                    tl::enums::ServerDhParams::Ok(tl::types::ServerDhParamsOk {
                        nonce,
                        server_nonce,
                        encrypted_answer: answer,
                    })
                    .to_bytes(),
                )
            }
            State::ClientDh {
                nonce,
                new_nonce,
                a,
            } => {
                let request = tl::functions::SetClientDhParams::from_boxed_bytes(body).unwrap();
                assert_eq!((request.nonce, request.server_nonce), (nonce, server_nonce));
                let mut data = request.encrypted_data;
                let (key, iv) = generate_key_data_from_nonce(&server_nonce, &new_nonce);
                aes::ige_decrypt(&mut data, &key, &iv);
                let (hash, rest) = data.split_at(20);
                let mut cursor = Cursor::from_slice(rest);
                let tl::enums::ClientDhInnerData::Data(inner) =
                    tl::enums::ClientDhInnerData::deserialize(&mut cursor).unwrap();
                assert_eq!(Sha1::digest(&rest[..cursor.pos()]).as_slice(), hash);
                let prime = BigUint::from_bytes_be(&DH_PRIME);
                let g_b = BigUint::from_bytes_be(&inner.g_b);
                assert!(in_safe_range(&g_b, &prime), "g_b in range");
                let shared = g_b.modpow(&a, &prime).to_bytes_be();
                let mut auth_key = [0; 256];
                auth_key[256 - shared.len()..].copy_from_slice(&shared);
                let new_nonce_hash1 =
                    AuthKey::from_bytes(auth_key).calc_new_nonce_hash(&new_nonce, 1);
                let answer = tl::enums::SetClientDhParamsAnswer::DhGenOk(tl::types::DhGenOk {
                    nonce,
                    server_nonce,
                    new_nonce_hash1,
                });
                Step::Done(answer.to_bytes(), Box::new(auth_key))
            }
        }
    }
}
