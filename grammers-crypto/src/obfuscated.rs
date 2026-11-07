// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[allow(deprecated)] // see https://github.com/RustCrypto/block-ciphers/issues/509
use aes::cipher::{KeyIvInit, StreamCipher};

/// One direction of the AES-256-CTR keystream that the obfuscated transport encrypts with, keyed
/// from the 64 bytes that open the connection. Applying it encrypts and decrypts alike.
///
/// A client takes both directions in an [`ObfuscatedCipher`]. A server, a proxy or a reader of
/// recorded traffic takes only the ones it needs: a server reads the client's bytes with
/// [`ObfuscatedKeystream::client_to_server`] and encrypts its own with
/// [`ObfuscatedKeystream::server_to_client`].
pub struct ObfuscatedKeystream(ctr::Ctr128BE<aes::Aes256>);

impl ObfuscatedKeystream {
    /// The keystream of the bytes the client sends: keyed by bytes 8 to 40 of `init`, from the
    /// counter in bytes 40 to 56.
    pub fn client_to_server(init: &[u8; 64]) -> Self {
        Self::keyed(init)
    }

    /// The keystream of the bytes the server sends: keyed the same way by `init` reversed.
    pub fn server_to_client(init: &[u8; 64]) -> Self {
        let mut reversed = *init;
        reversed.reverse();
        Self::keyed(&reversed)
    }

    fn keyed(material: &[u8; 64]) -> Self {
        // Fixed ranges of 64 bytes: these can't fail.
        #[allow(deprecated)] // see https://github.com/RustCrypto/block-ciphers/issues/509
        Self(ctr::Ctr128BE::<aes::Aes256>::new(
            material[8..40].try_into().unwrap(),
            material[40..56].try_into().unwrap(),
        ))
    }

    /// Applies the next bytes of the keystream to `buffer`, which encrypts and decrypts alike.
    pub fn apply(&mut self, buffer: &mut [u8]) {
        self.0.apply_keystream(buffer);
    }
}

/// This implements the AES-256-CTR cipher used by Telegram to encrypt data
/// when using the obfuscated transport.
///
/// It is not intended to be used directly.
pub struct ObfuscatedCipher {
    rx: ObfuscatedKeystream,
    tx: ObfuscatedKeystream,
}

impl ObfuscatedCipher {
    pub fn new(init: &[u8; 64]) -> Self {
        Self {
            rx: ObfuscatedKeystream::server_to_client(init),
            tx: ObfuscatedKeystream::client_to_server(init),
        }
    }

    pub fn encrypt(&mut self, buffer: &mut [u8]) {
        self.tx.apply(buffer);
    }

    pub fn decrypt(&mut self, buffer: &mut [u8]) {
        self.rx.apply(buffer);
    }
}
