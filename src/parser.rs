use std::str::from_utf8_unchecked;

use tokio_util::bytes::Bytes;

use crate::map::Map;
use crate::Temperature;

const fn create_mask(byte: u8) -> u64 {
    (!0u64 / 0xFF) * byte as u64
}

const fn has_zero(value: u64) -> u64 {
    value.wrapping_sub(create_mask(0x01)) & !(value) & create_mask(0x80)
}

const fn bytes_from_end(value: u64, mask: u64) -> u32 {
    (has_zero(value ^ mask).trailing_zeros() - 4) >> 3
}

const fn get_temp_branchless(end: u64) -> (usize, i32) {
    let split = bytes_from_end(end, create_mask(b';'));
    let negative = (has_zero(end ^ create_mask(b'-')) >> (((split - 1) << 3) + 7)) as i32;
    let mask = !(create_mask(0xFF) << (split << 3) >> (negative << 3));
    let end = end & mask;
    let ones = (end & 0xFF) as i32;
    let tens = (((end >> 16) & 0xFF) as i32) * 10;
    let has_hundreds = (((end >> 24) & 0xFF) as i32) >> 5;
    let hundreds = (((end >> 24) & 0xFF) as i32) * 100;
    let temp = ones - 48 + tens - 480 + hundreds - (has_hundreds * 4800);
    (split as usize, (temp ^ -negative) + negative)
}

fn get_temp(line: &[u8]) -> (usize, i32) {
    let length = line.len();
    let end = unsafe { line.last_chunk::<5>().unwrap_unchecked() };
    let mut temp = end[4] as i32 - 48 + (end[2] as i32 - 48) * 10;
    let split = if end[1] == b';' {
        length - 4
    } else if end[1] == b'-' {
        temp = -temp;
        length - 5
    } else {
        temp += (end[1] as i32 - 48) * 100;
        if end[0] == b';' {
            length - 5
        } else {
            temp = -temp;
            length - 6
        }
    };
    (split, temp)
}

pub type ResultMap = Map<Temperature, 10000>;

pub fn parser_branchless(chunk: &Bytes) -> ResultMap {
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

pub fn parser(chunk: &[u8]) -> ResultMap {
    let mut results = ResultMap::new();

    let mut start = 0;
    memchr::memchr_iter(b'\n', chunk).for_each(|mid| {
        let line = &chunk[start..mid];
        let (split, temp) = get_temp(line);
        let city = unsafe { from_utf8_unchecked(&line[..split]) };
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
    fn parser_branchless() {
        let line = "Rostov-on-Don;8.7";
        let end = u64::from_be_bytes(*line.as_bytes().last_chunk::<8>().unwrap());
        let (split, temp) = get_temp_branchless(end);
        assert_eq!(line.split_at(line.len() - split - 1).0, "Rostov-on-Don");
        assert_eq!(87, temp);
    }

    #[test]
    fn parser() {
        let line = "Rostov-on-Don;8.7";
        let (split, temp) = get_temp(line.as_bytes());
        assert_eq!(&line[..split], "Rostov-on-Don");
        assert_eq!(87, temp);
    }
}
