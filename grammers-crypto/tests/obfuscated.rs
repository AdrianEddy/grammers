// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The obfuscated transport's keystreams, one direction at a time and both in the client's
//! cipher: AES-256-CTR keyed by bytes 8 to 56 of the connection's opening, the client's as they
//! are and the server's reversed.

use grammers_crypto::hex::from_hex;
use grammers_crypto::{ObfuscatedCipher, ObfuscatedKeystream};

/// An opening: bytes 3, 10, 17, … (`(7i + 3) mod 256`).
fn init() -> [u8; 64] {
    std::array::from_fn(|i| (i * 7 + 3) as u8)
}

/// The first 48 bytes of each direction's keystream for [`init`], three AES blocks, computed
/// apart from this crate (Python's `cryptography`, AES-256 in CTR mode).
const CLIENT_TO_SERVER: &str = "abca9556f0eca071d62d823944201058bc2f3b2f79ef33f75b877d8cea935b703054a3f94f31a2b5a6018e9d9952b421";
const SERVER_TO_CLIENT: &str = "2064aaf65bc017aa009c2ef46694423c1e6435fd2e43314da16b9b992b6443b18e035bf487dcdfbc55fac9bd89bc0080";

/// `keystream` applied to 48 zero bytes, in pieces that cross the blocks' bounds.
fn applied(mut keystream: ObfuscatedKeystream) -> Vec<u8> {
    let mut bytes = vec![0; 48];
    let (first, rest) = bytes.split_at_mut(5);
    keystream.apply(first);
    let (second, third) = rest.split_at_mut(20);
    keystream.apply(second);
    keystream.apply(third);
    bytes
}

#[test]
fn each_direction_is_its_known_keystream() {
    let init = init();
    assert_eq!(
        applied(ObfuscatedKeystream::client_to_server(&init)),
        from_hex(CLIENT_TO_SERVER)
    );
    assert_eq!(
        applied(ObfuscatedKeystream::server_to_client(&init)),
        from_hex(SERVER_TO_CLIENT)
    );
}

#[test]
fn client_cipher_is_both_directions() {
    let init = init();
    let mut cipher = ObfuscatedCipher::new(&init);
    let mut sent = [0; 48];
    cipher.encrypt(&mut sent);
    let mut received = [0; 48];
    cipher.decrypt(&mut received);
    assert_eq!(sent.as_slice(), from_hex(CLIENT_TO_SERVER));
    assert_eq!(received.as_slice(), from_hex(SERVER_TO_CLIENT));
}

#[test]
fn server_reads_the_client_with_one_direction_each() {
    let init = init();
    let mut client = ObfuscatedCipher::new(&init);
    let mut from_client = ObfuscatedKeystream::client_to_server(&init);
    let mut to_client = ObfuscatedKeystream::server_to_client(&init);

    let mut request = *b"a request, then its answer";
    client.encrypt(&mut request);
    from_client.apply(&mut request);
    assert_eq!(&request, b"a request, then its answer");

    let mut answer = *b"the answer";
    to_client.apply(&mut answer);
    client.decrypt(&mut answer);
    assert_eq!(&answer, b"the answer");
}
