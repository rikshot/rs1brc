use std::str::from_utf8_unchecked;

use crate::map::Map;
use crate::Temperature;

const fn create_mask(byte: u8) -> u64 {
    (!0u64 / 0xFF) * byte as u64
}

const fn has_zero(value: u64) -> u64 {
    value.wrapping_sub(create_mask(0x01)) & !(value) & create_mask(0x80)
}

const fn bytes_from_end(value: u64, mask: u64) -> u32 {
    (has_zero(value ^ mask).trailing_zeros() - 4) / 8
}

const fn get_temp_branchless(end: u64) -> (usize, i32) {
    let split = bytes_from_end(end, create_mask(b';'));
    let negative = (has_zero(end ^ create_mask(b'-')) >> (((split - 1) * 8) + 7)) as i32;
    let mask = !(create_mask(0xFF) << (split * 8) >> (negative * 8));
    let end = end & mask;
    let ones = (end & 0xFF) as i32;
    let tens = (((end >> 16) & 0xFF) as i32) * 10;
    let has_hundreds = (((end >> 24) & 0xFF) as i32) >> 5;
    let hundreds = (((end >> 24) & 0xFF) as i32) * 100;
    let temp = ones - 48 + tens - 480 + hundreds - (has_hundreds * 4800);
    (split as usize, (temp ^ -negative) + negative)
}

pub type ResultMap = Map<Temperature, 8000>;

pub fn parser(chunk: &[u8]) -> ResultMap {
    let mut results = ResultMap::new();

    let mut start = 0;
    memchr::memchr_iter(b'\n', chunk).for_each(|mid| {
        let line = &chunk[start..mid];
        let length = mid - start;
        let end = if let Some(end) = line.last_chunk::<8>() {
            u64::from_be_bytes(*end)
        } else {
            u64::from_be_bytes(
                *[vec![0u8; 8 - length], line.to_vec()]
                    .concat()
                    .last_chunk::<8>()
                    .unwrap(),
            )
        };
        let (split, temp) = get_temp_branchless(end);
        let city = unsafe { from_utf8_unchecked(&line[..length - split - 1]) };
        if let Some(value) = results.get_mut(city) {
            value.update_single(temp);
        } else {
            results.set(city, &Temperature::new(temp));
        }
        start = mid + 1;
    });

    results
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_single() {
        let line = "Rostov-on-Don;8.7";
        let end = u64::from_be_bytes(*line.as_bytes().last_chunk::<8>().unwrap());
        let (split, temp) = get_temp_branchless(end);
        assert_eq!(line.split_at(line.len() - split - 1).0, "Rostov-on-Don");
        assert_eq!(87, temp);
    }
}
