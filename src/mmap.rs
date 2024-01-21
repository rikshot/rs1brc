use std::{
    fs::File,
    str::from_utf8_unchecked,
    thread::{self, JoinHandle},
};

use ahash::AHashMap;
use memmap2::MmapOptions;
use nom::{
    bytes::complete::take_until,
    character::complete::{char, newline, u8},
    combinator::{iterator, opt},
    sequence::{pair, separated_pair, terminated},
    AsBytes, IResult,
};

use crate::Temperature;

fn fast_float(input: &[u8]) -> IResult<&[u8], f32> {
    match pair(opt(char('-')), separated_pair(u8, char('.'), u8))(input) {
        Ok((i, (sign, (integer, fraction)))) => {
            let float = integer as f32 + fraction as f32 / 10.0;
            Ok((i, if sign.is_some() { -float } else { float }))
        }
        Err(error) => Err(error),
    }
}

fn parser(input: &[u8]) -> IResult<&[u8], (&[u8], f32)> {
    terminated(
        separated_pair(take_until(";"), char(';'), fast_float),
        newline,
    )(input)
}

pub fn with_mmap() -> Vec<(String, Temperature)> {
    let file = File::open("measurements.txt").unwrap();
    let length = file.metadata().unwrap().len();

    let cpus = num_cpus::get() as u64;
    let chunk_length = length / cpus;
    let last_chunk_length = chunk_length + length % cpus;

    let mut chunks: Vec<(u64, u64)> = (0..cpus)
        .map(|index| {
            let start = index * chunk_length;
            let end = start
                + if index < cpus - 1 {
                    chunk_length
                } else {
                    last_chunk_length
                };
            (start, end)
        })
        .collect();

    (1..cpus as usize).for_each(|index| {
        let (_, end) = chunks[index - 1];
        let file = File::open("measurements.txt").unwrap();
        let map = unsafe {
            MmapOptions::new()
                .offset(end - 1)
                .len(100)
                .map(&file)
                .unwrap()
        };
        let mut extra = 0;
        while map[extra] != b'\n' {
            extra += 1;
        }
        let (_, end) = &mut chunks[index - 1];
        *end += extra as u64;
        let (start, _) = &mut chunks[index];
        *start += extra as u64;
    });

    let threads: Vec<JoinHandle<AHashMap<String, Temperature>>> = (0..cpus)
        .map(|index| {
            let (start, end) = chunks[index as usize];
            let chunk_size = end - start;
            thread::spawn(move || {
                let mut results: AHashMap<String, Temperature> = AHashMap::with_capacity(10000);
                let file = File::open("measurements.txt").unwrap();
                let map = unsafe {
                    MmapOptions::new()
                        .offset(start)
                        .len(chunk_size as usize)
                        .populate()
                        .map(&file)
                        .unwrap()
                };
                map.advise(memmap2::Advice::Sequential).unwrap();
                map.advise(memmap2::Advice::WillNeed).unwrap();
                iterator(map.as_bytes(), parser).for_each(|(city, temperature)| {
                    let city = unsafe { from_utf8_unchecked(city) };
                    let temperature = Temperature::new(temperature);
                    if let Some(value) = results.get_mut(city) {
                        value.update(&temperature);
                    } else {
                        results.insert(city.to_owned(), temperature);
                    }
                });
                results
            })
        })
        .collect();

    let mut results: AHashMap<String, Temperature> = AHashMap::with_capacity(10000);
    for thread in threads {
        let result = thread.join().unwrap();
        result.into_iter().for_each(|(city, temperature)| {
            if let Some(value) = results.get_mut(&city) {
                value.update(&temperature);
            } else {
                results.insert(city, temperature);
            }
        })
    }

    results.into_iter().collect::<Vec<(String, Temperature)>>()
}
