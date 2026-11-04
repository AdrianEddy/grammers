// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::io;

use grammers_tl_gen::{Config, Outputs, generate_rust_code};
use grammers_tl_parser::parse_tl_file;
use grammers_tl_parser::tl::Definition;

const LAYER: i32 = 0;

fn get_definitions(contents: &str) -> Vec<Definition> {
    parse_tl_file(contents).map(|d| d.unwrap()).collect()
}

fn gen_rust_code(definitions: &[Definition]) -> io::Result<(String, String, String, String)> {
    let mut outputs = Outputs {
        common: Vec::new(),
        types: Vec::new(),
        functions: Vec::new(),
        enums: Vec::new(),
    };

    generate_rust_code(
        &mut outputs,
        definitions,
        LAYER,
        &Config {
            gen_name_for_id: false,
            deserializable_functions: true,
            impl_debug: true,
            impl_from_enum: true,
            impl_from_type: true,
            impl_serde: true,
        },
    )?;

    Ok((
        String::from_utf8(outputs.common).unwrap(),
        String::from_utf8(outputs.types).unwrap(),
        String::from_utf8(outputs.functions).unwrap(),
        String::from_utf8(outputs.enums).unwrap(),
    ))
}

#[test]
fn generic_functions_use_generic_parameters() -> io::Result<()> {
    let definitions = get_definitions(
        "
        ---functions---
        invokeWithLayer#da9b0d0d {X:Type} layer:int query:!X = X;
    ",
    );
    let (_, _, functions, _) = gen_rust_code(&definitions)?;
    eprintln!("{functions}");

    assert!(functions.contains("pub struct InvokeWithLayer<X>"));
    assert!(functions.contains("pub query: X,"));
    assert!(functions.contains("impl<X> crate::Identifiable for InvokeWithLayer<X>"));
    assert!(
        functions
            .contains("impl<X: crate::Serializable> crate::Serializable for InvokeWithLayer<X>")
    );
    assert!(
        functions.contains(
            "impl<X: crate::Deserializable> crate::Deserializable for InvokeWithLayer<X>"
        )
    );
    assert!(
        functions.contains("impl<X: crate::RemoteCall> crate::RemoteCall for InvokeWithLayer<X>")
    );
    assert!(functions.contains("type Return = X::Return;"));
    Ok(())
}

#[test]
fn recursive_types_direct_boxed() -> io::Result<()> {
    let definitions = get_definitions(
        "
        textBold#6724abc4 text:RichText = RichText;
    ",
    );
    let (_, _, _, enums) = gen_rust_code(&definitions)?;
    eprintln!("{enums}");
    assert!(enums.contains("TextBold(Box<crate::types::TextBold>)"));
    assert!(enums.contains("RichText::TextBold(Box::new("));
    assert!(enums.contains("Self::TextBold(Box::new("));
    Ok(())
}

#[test]
fn recursive_types_indirect_boxed() -> io::Result<()> {
    let definitions = get_definitions(
        "
        messageExtendedMedia#ee479c64 media:MessageMedia = MessageExtendedMedia;
        messageMediaInvoice#f6a548d3 flags:# extended_media:flags.4?MessageExtendedMedia = MessageMedia;
    ",
    );
    let (_, _, _, enums) = gen_rust_code(&definitions)?;
    eprintln!("{enums}");
    assert!(enums.contains("Media(Box<crate::types::MessageExtendedMedia>),"));
    assert!(enums.contains(
        "crate::types::MessageExtendedMedia::deserialize(buf).map(|x| Self::Media(Box::new(x)))"
    ));
    assert!(enums.contains("MessageExtendedMedia::Media(Box::new("));
    assert!(enums.contains("Invoice(Box<crate::types::MessageMediaInvoice>),"));
    assert!(enums.contains(
        "crate::types::MessageMediaInvoice::deserialize(buf).map(|x| Self::Invoice(Box::new(x)))"
    ));
    assert!(enums.contains("MessageMedia::Invoice(Box::new("));
    Ok(())
}

#[test]
fn recursive_types_indirect_no_hang() -> io::Result<()> {
    let definitions = get_definitions(
        "
        inputUserFromMessage#1da448e2 peer:InputPeer msg_id:int user_id:long = InputUser;
        inputPeerUserFromMessage#a87b0a1c peer:InputPeer msg_id:int user_id:long = InputPeer;
    ",
    );
    let _ = gen_rust_code(&definitions)?;
    Ok(())
}

#[test]
fn recursive_types_vec_indirect_not_boxed() -> io::Result<()> {
    let definitions = get_definitions(
        "
        jsonObjectValue#c0de1bd9 key:string value:JSONValue = JSONObjectValue;

        jsonArray#f7444763 value:Vector<JSONValue> = JSONValue;
        jsonObject#99c1d49d value:Vector<JSONObjectValue> = JSONValue;
    ",
    );
    let (_, _, _, enums) = gen_rust_code(&definitions)?;
    eprintln!("{enums}");
    assert!(enums.contains("JsonObjectValue(crate::types::JsonObjectValue)"));
    assert!(enums.contains("JsonArray(crate::types::JsonArray)"));
    assert!(enums.contains("JsonObject(crate::types::JsonObject)"));
    Ok(())
}

/// The `impl Deserializable` block for `name` in generated code.
fn deserialize_impl<'a>(code: &'a str, name: &str) -> &'a str {
    let start = code
        .find(&format!("impl crate::Deserializable for {name} {{"))
        .unwrap_or_else(|| panic!("no impl Deserializable for {name}"));
    let end = code[start + 1..]
        .find("\nimpl")
        .map_or(code.len(), |end| start + 1 + end);
    &code[start..end]
}

#[test]
fn boxed_types_deserialize_nested() -> io::Result<()> {
    let definitions = get_definitions(
        "
        inputPeerEmpty#7f3b18ea = InputPeer;
        jsonNull#3f6d7b68 = JSONValue;
        jsonArray#f7444763 value:Vector<JSONValue> = JSONValue;
    ",
    );
    let (_, types, _, enums) = gen_rust_code(&definitions)?;
    eprintln!("{enums}");
    // Every boxed type, whether it recurses or not, and whether through a `Vector` or a `Box`.
    for name in ["InputPeer", "Jsonvalue"] {
        assert!(
            deserialize_impl(&enums, name).contains("buf.nested(|buf| {"),
            "{name}"
        );
    }
    assert!(!types.contains("buf.nested("));
    Ok(())
}

#[test]
fn bare_recursive_types_deserialize_nested() -> io::Result<()> {
    let definitions = get_definitions(
        "
        node#11111111 children:vector<node> = Node;
        tree#22222222 root:node = Tree;
        left#33333333 right:Vector<right> = Left;
        right#44444444 flags:# left:flags.0?left = Right;
        future_salt#0949d9dc valid_since:int valid_until:int salt:long = FutureSalt;
        future_salts#ae500895 req_msg_id:long now:int salts:vector<future_salt> = FutureSalts;
    ",
    );
    let (_, types, _, _) = gen_rust_code(&definitions)?;
    eprintln!("{types}");
    // A cycle of bare references passes through no boxed type, so its types are guarded themselves.
    for name in ["Node", "Left", "Right"] {
        assert!(
            deserialize_impl(&types, name).contains("buf.nested(|buf| {"),
            "{name}"
        );
    }
    // Bare references that don't lead back are not.
    for name in ["Tree", "FutureSalt", "FutureSalts"] {
        assert!(
            !deserialize_impl(&types, name).contains("buf.nested("),
            "{name}"
        );
    }
    Ok(())
}

#[test]
fn generic_bytes_with_serde_bytes() -> io::Result<()> {
    let definitions = get_definitions(
        r#"
        chatPhotoEmpty#37c1011c = ChatPhoto;
        chatPhoto#1c6e1c11 flags:# has_video:flags.0?true photo_id:long stripped_thumb:flags.1?bytes dc_id:int = ChatPhoto;
        "#,
    );

    let (_, types, _, _) = gen_rust_code(&definitions)?;
    eprintln!("{types}");
    assert!(types.contains(r#"#[serde(with = "serde_bytes")]"#));
    assert!(types.contains("pub stripped_thumb: Option<Vec<u8>>,"));
    Ok(())
}
