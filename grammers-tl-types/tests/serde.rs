// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! With serde, binary is bytes, where serde's derive would write sequences of numbers: `bytes`,
//! `int128` and `int256`, plain and optional, and `Vector`s of them, plain and optional; an
//! optional one reads an absent key as `None`, as any other optional field does. Every binary
//! field of the schemas is marked, so a layer that brings another shape of binary fails here
//! instead of writing numbers.
#![cfg(all(feature = "impl-serde", feature = "tl-api", feature = "tl-mtproto"))]

use grammers_tl_types::{enums, functions, types};
use serde_test::{Token, assert_de_tokens, assert_tokens};

/// The generated structs, as the build script wrote them for this test's features.
const GENERATED: [(&str, &str); 2] = [
    (
        "types",
        include_str!(concat!(env!("OUT_DIR"), "/generated_types.rs")),
    ),
    (
        "functions",
        include_str!(concat!(env!("OUT_DIR"), "/generated_functions.rs")),
    ),
];

#[test]
fn every_binary_field_is_marked() {
    for (file, code) in GENERATED {
        let mut previous = "";
        let mut binary = 0;
        for line in code.lines() {
            let field = line
                .trim_start()
                .strip_prefix("pub ")
                .and_then(|field| field.strip_suffix(','))
                .and_then(|field| field.split_once(": "));
            if let Some((name, ty)) = field
                && ty.contains("u8")
            {
                binary += 1;
                // Unmarked, binary would be serialized as numbers; and serde's derive reads the
                // absent key of a field `with` a module as `None` only if it has `default` too.
                let marked = if ty.starts_with("Option<") {
                    "#[serde(default, with = "
                } else {
                    "#[serde(with = "
                };
                assert!(
                    previous.trim_start().starts_with(marked),
                    "{file}: `{name}: {ty}` isn't marked `{marked}…`"
                );
            }
            previous = line;
        }
        assert!(binary > 0, "{file} has no binary field");
    }
}

#[test]
fn int128_and_bytes_are_bytes() {
    let res_pq = types::ResPq {
        nonce: [1; 16],
        server_nonce: [2; 16],
        pq: vec![3, 4],
        server_public_key_fingerprints: vec![5],
    };
    assert_tokens(
        &res_pq,
        &[
            Token::Struct {
                name: "ResPq",
                len: 4,
            },
            Token::Str("nonce"),
            Token::Bytes(&[1; 16]),
            Token::Str("server_nonce"),
            Token::Bytes(&[2; 16]),
            Token::Str("pq"),
            Token::Bytes(&[3, 4]),
            // A vector of numbers stays one.
            Token::Str("server_public_key_fingerprints"),
            Token::Seq { len: Some(1) },
            Token::I64(5),
            Token::SeqEnd,
            Token::StructEnd,
        ],
    );
}

#[test]
fn int256_is_bytes() {
    let inner = types::PQInnerData {
        pq: vec![1],
        p: vec![2],
        q: vec![3],
        nonce: [4; 16],
        server_nonce: [5; 16],
        new_nonce: [6; 32],
    };
    assert_tokens(
        &inner,
        &[
            Token::Struct {
                name: "PQInnerData",
                len: 6,
            },
            Token::Str("pq"),
            Token::Bytes(&[1]),
            Token::Str("p"),
            Token::Bytes(&[2]),
            Token::Str("q"),
            Token::Bytes(&[3]),
            Token::Str("nonce"),
            Token::Bytes(&[4; 16]),
            Token::Str("server_nonce"),
            Token::Bytes(&[5; 16]),
            Token::Str("new_nonce"),
            Token::Bytes(&[6; 32]),
            Token::StructEnd,
        ],
    );
}

#[test]
fn optional_int256_is_bytes() {
    let join = |public_key| functions::phone::JoinGroupCall {
        muted: false,
        video_stopped: false,
        call: enums::InputGroupCall::Call(types::InputGroupCall {
            id: 1,
            access_hash: 2,
        }),
        join_as: enums::InputPeer::PeerSelf,
        invite_hash: None,
        public_key,
        block: None,
        params: enums::DataJson::Json(types::DataJson { data: "{}".into() }),
    };
    // The call's tokens, with `public_key`'s key and value as given.
    let tokens = |public_key: &[Token]| {
        let mut tokens = vec![
            Token::Struct {
                name: "JoinGroupCall",
                len: if public_key.is_empty() { 7 } else { 8 },
            },
            Token::Str("muted"),
            Token::Bool(false),
            Token::Str("video_stopped"),
            Token::Bool(false),
            Token::Str("call"),
            Token::NewtypeVariant {
                name: "InputGroupCall",
                variant: "Call",
            },
            Token::Struct {
                name: "InputGroupCall",
                len: 2,
            },
            Token::Str("id"),
            Token::I64(1),
            Token::Str("access_hash"),
            Token::I64(2),
            Token::StructEnd,
            Token::Str("join_as"),
            Token::UnitVariant {
                name: "InputPeer",
                variant: "PeerSelf",
            },
            Token::Str("invite_hash"),
            Token::None,
        ];
        tokens.extend_from_slice(public_key);
        tokens.extend([
            Token::Str("block"),
            Token::None,
            Token::Str("params"),
            Token::NewtypeVariant {
                name: "DataJson",
                variant: "Json",
            },
            Token::Struct {
                name: "DataJson",
                len: 1,
            },
            Token::Str("data"),
            Token::Str("{}"),
            Token::StructEnd,
            Token::StructEnd,
        ]);
        tokens
    };
    assert_tokens(
        &join(Some([7; 32])),
        &tokens(&[
            Token::Str("public_key"),
            Token::Some,
            Token::Bytes(&[7; 32]),
        ]),
    );
    assert_tokens(
        &join(None),
        &tokens(&[Token::Str("public_key"), Token::None]),
    );
    // Absent, as a format that leaves out a null value's key writes it.
    assert_de_tokens(&join(None), &tokens(&[]));
}

#[test]
fn vector_of_bytes_is_a_sequence_of_bytes() {
    let vote = functions::messages::SendVote {
        peer: enums::InputPeer::Empty,
        msg_id: 1,
        options: vec![vec![0], vec![1, 2]],
    };
    assert_tokens(
        &vote,
        &[
            Token::Struct {
                name: "SendVote",
                len: 3,
            },
            Token::Str("peer"),
            Token::UnitVariant {
                name: "InputPeer",
                variant: "Empty",
            },
            Token::Str("msg_id"),
            Token::I32(1),
            Token::Str("options"),
            Token::Seq { len: Some(2) },
            Token::Bytes(&[0]),
            Token::Bytes(&[1, 2]),
            Token::SeqEnd,
            Token::StructEnd,
        ],
    );
}

#[test]
fn optional_vector_of_bytes_is_a_sequence_of_bytes() {
    let settings = |logout_tokens| types::CodeSettings {
        allow_flashcall: false,
        current_number: false,
        allow_app_hash: false,
        allow_missed_call: false,
        allow_firebase: false,
        unknown_number: false,
        logout_tokens,
        token: None,
        app_sandbox: None,
    };
    // The settings' tokens, with `logout_tokens`' key and value as given.
    let tokens = |logout_tokens: &[Token]| {
        let mut tokens = vec![Token::Struct {
            name: "CodeSettings",
            len: if logout_tokens.is_empty() { 8 } else { 9 },
        }];
        for flag in [
            "allow_flashcall",
            "current_number",
            "allow_app_hash",
            "allow_missed_call",
            "allow_firebase",
            "unknown_number",
        ] {
            tokens.extend([Token::Str(flag), Token::Bool(false)]);
        }
        tokens.extend_from_slice(logout_tokens);
        tokens.extend([
            Token::Str("token"),
            Token::None,
            Token::Str("app_sandbox"),
            Token::None,
            Token::StructEnd,
        ]);
        tokens
    };
    assert_tokens(
        &settings(Some(vec![vec![1, 2, 3], vec![]])),
        &tokens(&[
            Token::Str("logout_tokens"),
            Token::Some,
            Token::Seq { len: Some(2) },
            Token::Bytes(&[1, 2, 3]),
            Token::Bytes(&[]),
            Token::SeqEnd,
        ]),
    );
    assert_tokens(
        &settings(Some(Vec::new())),
        &tokens(&[
            Token::Str("logout_tokens"),
            Token::Some,
            Token::Seq { len: Some(0) },
            Token::SeqEnd,
        ]),
    );
    assert_tokens(
        &settings(None),
        &tokens(&[Token::Str("logout_tokens"), Token::None]),
    );
    // Absent, as a format that leaves out a null value's key writes it.
    assert_de_tokens(&settings(None), &tokens(&[]));
}
