// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! The dial target of a datacenter option: only the addresses it really has.

use std::net::{Ipv4Addr, Ipv6Addr};

use super::*;

fn dc_option(ipv4: &str, ipv6: &str) -> DcOption {
    DcOption {
        id: 4,
        ipv4: ipv4.parse().unwrap(),
        ipv6: ipv6.parse().unwrap(),
        auth_key: None,
    }
}

#[test]
fn dial_target_has_both_families_ipv4_first() {
    let target = DialTarget::new(
        &dc_option("192.0.2.4:443", "[2001:db8::4]:443"),
        ConnKind::Main,
    );
    assert_eq!(
        target,
        DialTarget {
            dc_id: 4,
            kind: ConnKind::Main,
            ipv4: Some("192.0.2.4:443".parse().unwrap()),
            ipv6: Some("[2001:db8::4]:443".parse().unwrap()),
        }
    );
    assert_eq!(
        target.addresses().collect::<Vec<_>>(),
        ["192.0.2.4:443", "[2001:db8::4]:443"].map(|a| a.parse::<SocketAddr>().unwrap())
    );
}

#[test]
fn dial_target_leaves_out_addresses_it_does_not_have() {
    // The session's option for a datacenter it knows only an IPv6 address of, and one it knows
    // only an IPv4 address of: the IPv6 address embeds the IPv4 one.
    let only_ipv6 = DialTarget::new(
        &dc_option("0.0.0.0:0", "[2001:db8::4]:443"),
        ConnKind::Media,
    );
    assert_eq!(only_ipv6.kind, ConnKind::Media);
    assert_eq!(only_ipv6.ipv4, None);
    assert_eq!(
        only_ipv6.addresses().collect::<Vec<_>>(),
        ["[2001:db8::4]:443".parse::<SocketAddr>().unwrap()]
    );

    let mapped = SocketAddrV6::new(Ipv4Addr::new(192, 0, 2, 4).to_ipv6_mapped(), 443, 0, 0);
    let only_ipv4 = DialTarget::new(
        &dc_option("192.0.2.4:443", &mapped.to_string()),
        ConnKind::Main,
    );
    assert_eq!(only_ipv4.ipv6, None);
    assert_eq!(only_ipv4.addresses().count(), 1);

    let none = DialTarget::new(
        &dc_option(
            "0.0.0.0:0",
            &SocketAddrV6::new(Ipv6Addr::UNSPECIFIED, 0, 0, 0).to_string(),
        ),
        ConnKind::Cdn,
    );
    assert_eq!(none.addresses().count(), 0);
}
