// Copyright 2020 - developers of the `grammers` project.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// https://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or https://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Serde for a `Vector` of binary (`Vector<bytes>`, of `int128` or of `int256`), plain or
//! optional: a sequence of byte strings, each as [`serde_bytes`] writes one, where serde's derive
//! would write a sequence of sequences of numbers. The generated fields name it in
//! `#[serde(with = "crate::serde_bytes_vec")]`.

use serde::de::{Deserialize, Deserializer};
use serde::ser::{Serialize, Serializer};

/// What [`serialize`] takes: a `Vec` of binary items, or an optional one.
pub(crate) trait SerializeField {
    fn serialize_field<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error>;
}

/// What [`deserialize`] gives: a `Vec` of binary items, or an optional one.
pub(crate) trait DeserializeField<'de>: Sized {
    fn deserialize_field<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error>;
}

pub(crate) fn serialize<F, S>(field: &F, serializer: S) -> Result<S::Ok, S::Error>
where
    F: SerializeField,
    S: Serializer,
{
    field.serialize_field(serializer)
}

pub(crate) fn deserialize<'de, F, D>(deserializer: D) -> Result<F, D::Error>
where
    F: DeserializeField<'de>,
    D: Deserializer<'de>,
{
    F::deserialize_field(deserializer)
}

impl<T: serde_bytes::Serialize> SerializeField for Vec<T> {
    fn serialize_field<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Items(self).serialize(serializer)
    }
}

impl<T: serde_bytes::Serialize> SerializeField for Option<Vec<T>> {
    fn serialize_field<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Some(items) => serializer.serialize_some(&Items(items)),
            None => serializer.serialize_none(),
        }
    }
}

impl<'de, T: serde_bytes::Deserialize<'de>> DeserializeField<'de> for Vec<T> {
    fn deserialize_field<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Vec::<FromBytes<T>>::deserialize(deserializer).map(into_items)
    }
}

impl<'de, T: serde_bytes::Deserialize<'de>> DeserializeField<'de> for Option<Vec<T>> {
    fn deserialize_field<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Option::<Vec<FromBytes<T>>>::deserialize(deserializer).map(|items| items.map(into_items))
    }
}

/// Binary items, serialized as a sequence of byte strings.
struct Items<'a, T>(&'a [T]);

impl<T: serde_bytes::Serialize> Serialize for Items<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_seq(self.0.iter().map(AsBytes))
    }
}

/// One binary item, serialized as a byte string.
struct AsBytes<'a, T>(&'a T);

impl<T: serde_bytes::Serialize> Serialize for AsBytes<'_, T> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serde_bytes::serialize(self.0, serializer)
    }
}

/// One binary item, deserialized from a byte string. Transparent, so a `Vec` of them becomes a
/// `Vec` of items in place.
#[repr(transparent)]
struct FromBytes<T>(T);

impl<'de, T: serde_bytes::Deserialize<'de>> Deserialize<'de> for FromBytes<T> {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        serde_bytes::deserialize(deserializer).map(Self)
    }
}

fn into_items<T>(items: Vec<FromBytes<T>>) -> Vec<T> {
    items.into_iter().map(|FromBytes(item)| item).collect()
}
