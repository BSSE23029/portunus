//! Zero-copy Serde cursors for bencode lists and dictionaries.
//!
//! Each cursor advances once, retains only the state needed by Serde's access
//! protocol, and decorates child failures with their list index or dictionary
//! key. Dictionary keys remain arbitrary bytes unless a target explicitly asks
//! for UTF-8. This module owns traversal mechanics only; it does not parse input,
//! interpret schemas, or allocate successful scalar values.

use crate::{
    deserialize::{DeserializeError, DeserializePath, NodeDeserializer},
    Value,
};
use serde::de::{self, DeserializeSeed, MapAccess, SeqAccess, Visitor};
use std::{collections::btree_map, slice};

pub struct ListAccess<'value, 'de> {
    values: slice::Iter<'value, Value<'de>>,
    index: usize,
}

impl<'value, 'de> ListAccess<'value, 'de> {
    // Inputs: a shared ordered value slice.
    // Outputs: a zero-copy Serde sequence cursor at index zero.
    // Logic: retain only iterator and current index for error context.
    pub fn new(values: &'value [Value<'de>]) -> Self {
        Self {
            values: values.iter(),
            index: 0,
        }
    }
}

impl<'de> SeqAccess<'de> for ListAccess<'_, 'de> {
    type Error = DeserializeError;

    // Inputs: mutable cursor and target seed for the next optional element.
    // Outputs: decoded element, end-of-sequence, or index-qualified error.
    // Logic: increment after selecting the node and prepend its index on failure.
    fn next_element_seed<T: DeserializeSeed<'de>>(
        &mut self,
        seed: T,
    ) -> Result<Option<T::Value>, Self::Error> {
        let Some(value) = self.values.next() else {
            return Ok(None);
        };
        let index = self.index;
        self.index += 1;
        seed.deserialize(NodeDeserializer { value })
            .map(Some)
            .map_err(|error| error.prepend(DeserializePath::Index(index)))
    }

    // Inputs: shared sequence cursor.
    // Outputs: exact remaining element count.
    // Logic: expose the slice iterator's bounded length for target preallocation.
    fn size_hint(&self) -> Option<usize> {
        Some(self.values.len())
    }
}

pub struct DictionaryAccess<'value, 'de> {
    values: btree_map::Iter<'value, &'de [u8], Value<'de>>,
    pending: Option<(&'de [u8], &'value Value<'de>)>,
}

impl<'value, 'de> DictionaryAccess<'value, 'de> {
    // Inputs: a shared raw-byte-keyed dictionary.
    // Outputs: a zero-copy Serde map cursor with no pending entry.
    // Logic: preserve canonical key order and exact key/value pairing.
    pub fn new(values: &'value std::collections::BTreeMap<&'de [u8], Value<'de>>) -> Self {
        Self {
            values: values.iter(),
            pending: None,
        }
    }
}

impl<'de> MapAccess<'de> for DictionaryAccess<'_, 'de> {
    type Error = DeserializeError;

    // Inputs: mutable map cursor and a seed for the next optional key.
    // Outputs: decoded key, end-of-map, or key-qualified conversion error.
    // Logic: retain the exact associated value for the required following call.
    fn next_key_seed<K: DeserializeSeed<'de>>(
        &mut self,
        seed: K,
    ) -> Result<Option<K::Value>, Self::Error> {
        let Some((key, value)) = self.values.next() else {
            return Ok(None);
        };
        let key = *key;
        self.pending = Some((key, value));
        seed.deserialize(KeyDeserializer { key })
            .map(Some)
            .map_err(|error| error.prepend(DeserializePath::Key(key.to_vec())))
    }

    // Inputs: mutable cursor after a successful key and target value seed.
    // Outputs: decoded associated value or key-qualified protocol/conversion error.
    // Logic: consume the pending pair so entries cannot cross or repeat.
    fn next_value_seed<V: DeserializeSeed<'de>>(
        &mut self,
        seed: V,
    ) -> Result<V::Value, Self::Error> {
        let (key, value) = self
            .pending
            .take()
            .ok_or_else(|| DeserializeError::new("map value requested before key"))?;
        seed.deserialize(NodeDeserializer { value })
            .map_err(|error| error.prepend(DeserializePath::Key(key.to_vec())))
    }

    // Inputs: shared map cursor.
    // Outputs: exact number of entries not yet offered as keys.
    // Logic: pending values belong to an already-reported key and are excluded.
    fn size_hint(&self) -> Option<usize> {
        Some(self.values.len())
    }
}

#[derive(Clone, Copy)]
struct KeyDeserializer<'de> {
    key: &'de [u8],
}

impl<'de> de::Deserializer<'de> for KeyDeserializer<'de> {
    type Error = DeserializeError;

    // Inputs: one raw dictionary key and target visitor.
    // Outputs: borrowed bytes accepted by generic byte-key targets.
    // Logic: preserve bencode's byte-native key model by default.
    fn deserialize_any<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_borrowed_bytes(self.key)
    }

    // Inputs: one raw key requested as bytes.
    // Outputs: its original borrowed slice.
    // Logic: avoid all key copies on successful binary-map decoding.
    fn deserialize_bytes<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        visitor.visit_borrowed_bytes(self.key)
    }

    // Inputs: one raw key requested as UTF-8 identifier text.
    // Outputs: borrowed text or validation error.
    // Logic: struct field names require text even though generic map keys do not.
    fn deserialize_identifier<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    // Inputs: one raw key requested as UTF-8 text.
    // Outputs: borrowed string or validation error.
    // Logic: validate only for textual target keys.
    fn deserialize_str<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        let text = std::str::from_utf8(self.key)
            .map_err(|error| DeserializeError::new(format!("invalid UTF-8 key: {error}")))?;
        visitor.visit_borrowed_str(text)
    }

    // Inputs: one raw key requested as an owned-capable byte target.
    // Outputs: visitor-selected representation.
    // Logic: still offer borrowed data so ownership remains target-controlled.
    fn deserialize_byte_buf<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_bytes(visitor)
    }

    // Inputs: one raw key requested as an owned-capable string target.
    // Outputs: visitor-selected text or UTF-8 validation error.
    // Logic: reuse the borrowed textual path before any target allocation.
    fn deserialize_string<V: Visitor<'de>>(self, visitor: V) -> Result<V::Value, Self::Error> {
        self.deserialize_str(visitor)
    }

    serde::forward_to_deserialize_any! {
        bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char unit unit_struct
        option newtype_struct seq tuple tuple_struct map struct enum ignored_any
    }
}
