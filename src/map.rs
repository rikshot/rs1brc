use std::hash::Hasher;

use rustc_hash::FxHasher;

#[derive(Clone)]
pub struct Entry<T> {
    hash: u64,
    pub key: String,
    pub value: T,
}

pub struct Map<T, const N: usize> {
    pub table: Vec<Option<Entry<T>>>,
}

impl<T: Clone + Copy, const N: usize> Map<T, N> {
    pub fn new() -> Self {
        Self {
            table: vec![None; N],
        }
    }

    fn find_slot(&self, key: &str) -> (u64, usize) {
        let mut hasher = FxHasher::default();
        hasher.write(key.as_bytes());
        let hash = hasher.finish();
        let mut slot = hash as usize % (N - 1);
        loop {
            if let Some(entry) = &self.table[slot] {
                if entry.hash == hash {
                    return (hash, slot);
                }
                slot = (slot + 1) % (N - 1);
            } else {
                return (hash, slot);
            }
        }
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut T> {
        let (_, slot) = self.find_slot(key);
        if let Some(entry) = &mut self.table[slot] {
            return Some(&mut entry.value);
        }
        None
    }

    pub fn set(&mut self, key: &str, value: &T) {
        let (hash, slot) = self.find_slot(key);
        if let Some(entry) = &mut self.table[slot] {
            entry.value = *value;
        } else {
            self.table[slot] = Some(Entry {
                hash,
                key: key.to_string(),
                value: *value,
            });
        }
    }
}
