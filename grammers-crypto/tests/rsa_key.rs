// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! A server's RSA public key: its fingerprint, and the keys the handshake can't use.

use grammers_crypto::rsa::Key;

/// The modulus of the key of Telegram's test datacenters.
const TELEGRAM_TEST_N: &str = "25342889448840415564971689590713473206898847759084779052582026594546022463853940585885215951168491965708222649399180603818074200620463776135424884632162512403163793083921641631564740959529419359595852941166848940585952337613333022396096584117954892216031229237302943701877588456738335398602461675225081791820393153757504952636234951323237820036543581047826906120927972487366805292115792231423684261262330394324750785450942589751755390156647751460719351439969059949569615302809050721500330239005077889855323917509948255722081644689442127297605422579707142646660768825302832201908302295573257427896031830742328565032949";

#[test]
fn fingerprint_is_the_one_telegram_lists() {
    // The fingerprint Telegram's test datacenters send in `resPQ` for this key.
    let key = Key::new(TELEGRAM_TEST_N, "65537").unwrap();
    assert_eq!(key.fingerprint(), -5595554452916591101);
}

#[test]
fn key_needs_a_256_byte_modulus_and_a_valid_exponent() {
    // 2^2040 is the smallest number of 256 bytes, 2^2048 the smallest of 257.
    let smallest = num_bigint::BigUint::from(1u8) << 2040u32;
    let next_size = num_bigint::BigUint::from(1u8) << 2048u32;
    let decimal = |n: &num_bigint::BigUint| n.to_str_radix(10);
    assert!(Key::new(&decimal(&smallest), "65537").is_some());
    assert!(Key::new(&decimal(&(&next_size - 1u8)), "65537").is_some());
    assert!(Key::new(&decimal(&(&smallest - 1u8)), "65537").is_none());
    assert!(Key::new(&decimal(&next_size), "65537").is_none());

    for e in ["0", "1", TELEGRAM_TEST_N] {
        assert!(Key::new(TELEGRAM_TEST_N, e).is_none(), "e = {e}");
    }
    assert!(Key::new(TELEGRAM_TEST_N, "3").is_some());
    assert!(Key::new("", "65537").is_none());
    assert!(Key::new(TELEGRAM_TEST_N, "0x10001").is_none());
}
