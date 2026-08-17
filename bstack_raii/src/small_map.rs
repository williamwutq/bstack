//! [`SmallStringMap`] — a `Vec<(String, T)>` dressed as a map.
//!
//! An **insertion-ordered**, linear-scan map keyed by `String`. For the small,
//! build-once-read-a-few-times collections this crate produces — a block's fields, a
//! moved-out object's parts — a `Vec` of pairs beats a `HashMap`: no hashing, no
//! allocation per entry, cache-friendly, and it keeps declaration order. The point of
//! this type is to give that `Vec` the ergonomics people expect — `get` / `insert` /
//! `remove` / iteration and the full **`entry` API** — so callers never open-code a
//! `.iter().find(|(k, _)| …)` again.
//!
//! Lookups are `O(n)`; use it only where `n` is small.

use std::mem;

/// An insertion-ordered map from `String` keys to `T`, backed by a `Vec` of pairs.
/// See the [module docs](self).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SmallStringMap<T> {
    entries: Vec<(String, T)>,
}

impl<T> SmallStringMap<T> {
    /// A new empty map.
    pub const fn new() -> Self {
        Self {
            entries: Vec::new(),
        }
    }

    /// A new empty map with room for `capacity` entries.
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            entries: Vec::with_capacity(capacity),
        }
    }

    /// Wrap an existing `Vec` of pairs (order preserved; earlier duplicates win on
    /// lookup, as insertion never dedups a pre-built vector).
    pub fn from_vec(entries: Vec<(String, T)>) -> Self {
        Self { entries }
    }

    /// Consume into the underlying `Vec` of pairs (insertion order).
    pub fn into_vec(self) -> Vec<(String, T)> {
        self.entries
    }

    /// The entries as a slice, in insertion order.
    pub fn as_slice(&self) -> &[(String, T)] {
        &self.entries
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether the map is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The allocated capacity.
    pub fn capacity(&self) -> usize {
        self.entries.capacity()
    }

    /// Remove all entries, keeping the allocation.
    pub fn clear(&mut self) {
        self.entries.clear();
    }

    /// The index of `key`, if present.
    fn position(&self, key: &str) -> Option<usize> {
        self.entries.iter().position(|(k, _)| k == key)
    }

    /// Whether `key` is present.
    pub fn contains_key(&self, key: &str) -> bool {
        self.position(key).is_some()
    }

    /// A shared reference to the value for `key`.
    pub fn get(&self, key: &str) -> Option<&T> {
        self.entries.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    /// A mutable reference to the value for `key`.
    pub fn get_mut(&mut self, key: &str) -> Option<&mut T> {
        self.entries
            .iter_mut()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    /// The stored key + value for `key` (the key as actually stored).
    pub fn get_key_value(&self, key: &str) -> Option<(&str, &T)> {
        self.entries
            .iter()
            .find(|(k, _)| k == key)
            .map(|(k, v)| (k.as_str(), v))
    }

    /// Insert `value` for `key`, returning the previous value if the key was present
    /// (its position is kept); a new key is appended at the end.
    pub fn insert(&mut self, key: impl Into<String>, value: T) -> Option<T> {
        let key = key.into();
        match self.position(&key) {
            Some(i) => Some(mem::replace(&mut self.entries[i].1, value)),
            None => {
                self.entries.push((key, value));
                None
            }
        }
    }

    /// Remove and return the value for `key`, **preserving the order** of the rest
    /// (an `O(n)` shift).
    pub fn remove(&mut self, key: &str) -> Option<T> {
        let i = self.position(key)?;
        Some(self.entries.remove(i).1)
    }

    /// Remove and return the value for `key` **without preserving order** (an `O(1)`
    /// swap with the last entry).
    pub fn swap_remove(&mut self, key: &str) -> Option<T> {
        let i = self.position(key)?;
        Some(self.entries.swap_remove(i).1)
    }

    /// Iterate `(key, &value)` in insertion order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &T)> {
        self.entries.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Iterate `(key, &mut value)` in insertion order.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (&str, &mut T)> {
        self.entries.iter_mut().map(|(k, v)| (k.as_str(), v))
    }

    /// Iterate the keys in insertion order.
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.entries.iter().map(|(k, _)| k.as_str())
    }

    /// Iterate the values in insertion order.
    pub fn values(&self) -> impl Iterator<Item = &T> {
        self.entries.iter().map(|(_, v)| v)
    }

    /// Mutably iterate the values in insertion order.
    pub fn values_mut(&mut self) -> impl Iterator<Item = &mut T> {
        self.entries.iter_mut().map(|(_, v)| v)
    }

    /// Get the [`Entry`] for `key` — the in-place-or-insert API.
    pub fn entry(&mut self, key: impl Into<String>) -> Entry<'_, T> {
        let key = key.into();
        match self.position(&key) {
            Some(index) => Entry::Occupied(OccupiedEntry { map: self, index }),
            None => Entry::Vacant(VacantEntry { map: self, key }),
        }
    }
}

impl<T> std::ops::Index<&str> for SmallStringMap<T> {
    type Output = T;
    fn index(&self, key: &str) -> &T {
        self.get(key).expect("no entry found for key")
    }
}

impl<T> FromIterator<(String, T)> for SmallStringMap<T> {
    /// Collect pairs into a map; a repeated key keeps its first position but takes the
    /// last value (`insert` semantics).
    fn from_iter<I: IntoIterator<Item = (String, T)>>(iter: I) -> Self {
        let mut map = Self::new();
        for (k, v) in iter {
            map.insert(k, v);
        }
        map
    }
}

impl<T> Extend<(String, T)> for SmallStringMap<T> {
    fn extend<I: IntoIterator<Item = (String, T)>>(&mut self, iter: I) {
        for (k, v) in iter {
            self.insert(k, v);
        }
    }
}

impl<T> IntoIterator for SmallStringMap<T> {
    type Item = (String, T);
    type IntoIter = std::vec::IntoIter<(String, T)>;
    fn into_iter(self) -> Self::IntoIter {
        self.entries.into_iter()
    }
}

impl<T> From<Vec<(String, T)>> for SmallStringMap<T> {
    fn from(entries: Vec<(String, T)>) -> Self {
        Self::from_vec(entries)
    }
}

// -- The entry API ---------------------------------------------------------

/// A view into a single entry of a [`SmallStringMap`], present or not — the return
/// of [`SmallStringMap::entry`].
pub enum Entry<'a, T> {
    Occupied(OccupiedEntry<'a, T>),
    Vacant(VacantEntry<'a, T>),
}

/// An occupied [`Entry`].
pub struct OccupiedEntry<'a, T> {
    map: &'a mut SmallStringMap<T>,
    index: usize,
}

/// A vacant [`Entry`], holding the key it would insert under.
pub struct VacantEntry<'a, T> {
    map: &'a mut SmallStringMap<T>,
    key: String,
}

impl<'a, T> Entry<'a, T> {
    /// Ensure a value is present (inserting `default` if vacant) and return a mutable
    /// reference to it.
    pub fn or_insert(self, default: T) -> &'a mut T {
        match self {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(default),
        }
    }

    /// Like [`or_insert`](Self::or_insert), computing the default lazily.
    pub fn or_insert_with<F: FnOnce() -> T>(self, default: F) -> &'a mut T {
        match self {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(default()),
        }
    }

    /// Like [`or_insert_with`](Self::or_insert_with), but the closure is handed the
    /// key.
    pub fn or_insert_with_key<F: FnOnce(&str) -> T>(self, default: F) -> &'a mut T {
        match self {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => {
                let value = default(&e.key);
                e.insert(value)
            }
        }
    }

    /// Ensure a value is present (inserting `T::default()` if vacant).
    pub fn or_default(self) -> &'a mut T
    where
        T: Default,
    {
        self.or_insert_with(T::default)
    }

    /// Run `f` on the value if the entry is occupied, then return the entry (for
    /// chaining with `or_insert*`).
    pub fn and_modify<F: FnOnce(&mut T)>(mut self, f: F) -> Self {
        if let Entry::Occupied(e) = &mut self {
            f(e.get_mut());
        }
        self
    }

    /// The key this entry is (or would be) stored under.
    pub fn key(&self) -> &str {
        match self {
            Entry::Occupied(e) => e.key(),
            Entry::Vacant(e) => e.key(),
        }
    }
}

impl<'a, T> OccupiedEntry<'a, T> {
    /// The stored key.
    pub fn key(&self) -> &str {
        &self.map.entries[self.index].0
    }

    /// A shared reference to the value.
    pub fn get(&self) -> &T {
        &self.map.entries[self.index].1
    }

    /// A mutable reference to the value.
    pub fn get_mut(&mut self) -> &mut T {
        &mut self.map.entries[self.index].1
    }

    /// Consume the entry into a mutable reference tied to the map's borrow.
    pub fn into_mut(self) -> &'a mut T {
        &mut self.map.entries[self.index].1
    }

    /// Replace the value, returning the old one.
    pub fn insert(&mut self, value: T) -> T {
        mem::replace(self.get_mut(), value)
    }

    /// Remove the entry, returning its value (order-preserving).
    pub fn remove(self) -> T {
        self.map.entries.remove(self.index).1
    }
}

impl<'a, T> VacantEntry<'a, T> {
    /// The key that would be inserted.
    pub fn key(&self) -> &str {
        &self.key
    }

    /// Take back the owned key.
    pub fn into_key(self) -> String {
        self.key
    }

    /// Insert `value` under the entry's key, returning a mutable reference to it.
    pub fn insert(self, value: T) -> &'a mut T {
        self.map.entries.push((self.key, value));
        &mut self.map.entries.last_mut().unwrap().1
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn insert_get_and_overwrite() {
        let mut m: SmallStringMap<i32> = SmallStringMap::new();
        assert!(m.is_empty());
        assert_eq!(m.insert("a", 1), None);
        assert_eq!(m.insert("b", 2), None);
        assert_eq!(m.insert("a", 10), Some(1)); // overwrite returns old, keeps place
        assert_eq!(m.get("a"), Some(&10));
        assert_eq!(m.get("missing"), None);
        assert_eq!(m.len(), 2);
        assert!(m.contains_key("b"));
        // insertion order preserved (a before b)
        assert_eq!(m.keys().collect::<Vec<_>>(), ["a", "b"]);
    }

    #[test]
    fn entry_or_insert_and_modify() {
        let mut counts: SmallStringMap<i32> = SmallStringMap::new();
        for word in ["x", "y", "x", "x", "y"] {
            *counts.entry(word).or_insert(0) += 1;
        }
        assert_eq!(counts.get("x"), Some(&3));
        assert_eq!(counts.get("y"), Some(&2));

        // and_modify + or_insert chain
        counts.entry("x").and_modify(|c| *c += 100).or_insert(1);
        counts.entry("z").and_modify(|c| *c += 100).or_insert(1);
        assert_eq!(counts.get("x"), Some(&103));
        assert_eq!(counts.get("z"), Some(&1));

        // or_default / or_insert_with_key
        assert_eq!(*counts.entry("w").or_default(), 0);
        let v = counts.entry("keyed").or_insert_with_key(|k| k.len() as i32);
        assert_eq!(*v, 5);
    }

    #[test]
    fn remove_preserves_order() {
        let mut m: SmallStringMap<&str> = [
            ("a".to_string(), "1"),
            ("b".to_string(), "2"),
            ("c".to_string(), "3"),
        ]
        .into_iter()
        .collect();
        assert_eq!(m.remove("b"), Some("2"));
        assert_eq!(m.remove("b"), None);
        assert_eq!(m.keys().collect::<Vec<_>>(), ["a", "c"]);
    }

    #[test]
    fn index_and_iteration() {
        let mut m: SmallStringMap<i32> = SmallStringMap::new();
        m.insert("one", 1);
        m.insert("two", 2);
        assert_eq!(m["two"], 2);
        for v in m.values_mut() {
            *v *= 10;
        }
        assert_eq!(m.iter().collect::<Vec<_>>(), [("one", &10), ("two", &20)]);
        assert_eq!(
            m.into_iter().collect::<Vec<_>>(),
            [("one".to_string(), 10), ("two".to_string(), 20)]
        );
    }
}
