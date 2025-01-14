#[derive(Clone)]
pub struct Entry<T> {
    hash: u64,
    pub key: String,
    pub value: T,
}

pub struct Map<T, const N: usize> {
    pub table: Vec<Option<Entry<T>>>,
}

#[inline]
const fn hash(string: &str) -> u64 {
    let bytes = string.as_bytes();
    let length = bytes.len();
    let mut hash = [0u8; 8];
    hash[0] = bytes[0];
    hash[1] = bytes[1];
    hash[2] = bytes[2];
    hash[7] = bytes[length - 1];
    hash[6] = bytes[length - 2];
    hash[5] = bytes[length - 3];
    u64::from_ne_bytes(hash)
}

impl<T: Clone + Copy, const N: usize> Map<T, N> {
    pub fn new() -> Self {
        Self {
            table: vec![None; N],
        }
    }

    #[inline]
    fn find_slot(&self, key: &str) -> (u64, usize) {
        let hash = hash(key);
        let mut slot = hash as usize % (N - 1);
        loop {
            if let Some(entry) = unsafe { self.table.get_unchecked(slot) } {
                if entry.hash == hash {
                    return (hash, slot);
                }
                slot = (slot + 1) % (N - 1);
            } else {
                return (hash, slot);
            }
        }
    }

    #[inline]
    pub fn get_mut(&mut self, key: &str) -> Option<&mut T> {
        let (_, slot) = self.find_slot(key);
        if let Some(entry) = unsafe { self.table.get_unchecked_mut(slot) } {
            return Some(&mut entry.value);
        }
        None
    }

    pub fn set(&mut self, key: &str, value: &T) {
        let (hash, slot) = self.find_slot(key);
        if let Some(entry) = unsafe { self.table.get_unchecked_mut(slot) } {
            entry.value = *value;
        } else {
            *unsafe { self.table.get_unchecked_mut(slot) } = Some(Entry {
                hash,
                key: key.to_string(),
                value: *value,
            });
        }
    }
}
