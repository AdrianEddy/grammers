// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The server's datacenter options, stored without panicking on ones that can't be used.

use grammers_session::storages::MemorySession;

use super::*;

fn dc_option(id: i32, ipv6: bool, ip_address: &str, port: i32) -> tl::enums::DcOption {
    tl::types::DcOption {
        ipv6,
        media_only: false,
        tcpo_only: false,
        cdn: false,
        r#static: true,
        this_port_only: false,
        id,
        ip_address: ip_address.into(),
        port,
        secret: None,
    }
    .into()
}

#[tokio::test]
async fn invalid_dc_address_is_skipped() {
    let session = Arc::new(MemorySession::default());
    let pool = SenderPool::new(Arc::clone(&session), 1);
    let stored = |id| session.dc_option(id).unwrap().unwrap();
    let before = [3, 4, 5].map(stored);

    pool.runner
        .update_dc_options(&[
            dc_option(2, false, "192.0.2.2", 443),
            dc_option(2, true, "2001:db8::2", 443),
            // Not an address; an IPv4 address where an IPv6 one is due; ports outside 0..=65535.
            dc_option(3, false, "not an address", 443),
            dc_option(3, true, "", 443),
            dc_option(4, true, "192.0.2.4", 443),
            dc_option(5, false, "192.0.2.5", 65536),
            dc_option(5, true, "2001:db8::5", -1),
        ])
        .await
        .unwrap();

    let dc2 = stored(2);
    assert_eq!(dc2.ipv4, "192.0.2.2:443".parse().unwrap());
    assert_eq!(dc2.ipv6, "[2001:db8::2]:443".parse().unwrap());
    assert_eq!([3, 4, 5].map(stored), before);
}
