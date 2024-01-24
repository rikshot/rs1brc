use std::{
    fs::File,
    str::from_utf8_unchecked,
    sync::Arc,
    thread::{self, JoinHandle},
};

use ahash::AHashMap;
use memmap2::MmapOptions;
use nom::{combinator::iterator, AsBytes};

use crate::{parser, update, Temperature};

pub fn with_mmap() -> Vec<(String, Temperature)> {
    let file = File::open("measurements.txt").unwrap();

    let map = Arc::new(unsafe { MmapOptions::new().map(&file).unwrap() });
    map.advise(memmap2::Advice::Random).unwrap();

    let threads = std::env::var("THREADS").map_or(num_cpus::get(), |value| {
        value.parse().expect("Unable to parse thread count")
    });
    println!("Using {} threads", threads);

    let length = file.metadata().unwrap().len() as usize;
    let chunk_length = length / threads;
    let last_chunk_length = chunk_length + length % threads;
    let chunk_count = length / chunk_length;

    let mut chunks: Vec<(usize, usize)> = (0..chunk_count)
        .map(|index| {
            let start = index * chunk_length;
            let end = start
                + if index < threads - 1 {
                    chunk_length
                } else {
                    last_chunk_length
                };
            (start, end)
        })
        .collect();

    (1..chunk_count).for_each(|index| {
        let (_, end) = chunks[index - 1];
        let mut extra = 0;
        while map[end + extra] != b'\n' {
            extra += 1;
        }
        extra += 1;
        let (_, end) = &mut chunks[index - 1];
        *end += extra;
        let (start, _) = &mut chunks[index];
        *start += extra;
    });

    let threads: Vec<JoinHandle<AHashMap<String, Temperature>>> = chunks
        .into_iter()
        .map(|(start, end)| {
            let map = map.clone();
            thread::spawn(move || {
                let mut results: AHashMap<String, Temperature> = AHashMap::with_capacity(10000);
                iterator(map[start..end].as_bytes(), parser).for_each(|(city, temperature)| {
                    let city = unsafe { from_utf8_unchecked(city) };
                    let temperature = Temperature::new(temperature);
                    update(&mut results, city, &temperature);
                });
                results
            })
        })
        .collect();

    let mut results: AHashMap<String, Temperature> = AHashMap::with_capacity(10000);
    for thread in threads {
        let result = thread.join().unwrap();
        for (city, temperature) in result.iter() {
            update(&mut results, city, temperature);
        }
    }

    results.into_iter().collect::<Vec<(String, Temperature)>>()
}
