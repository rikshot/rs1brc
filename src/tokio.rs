use std::{io::SeekFrom, str::from_utf8_unchecked};

use ahash::AHashMap;
use nom::{combinator::iterator, AsBytes};
use tokio::{
    fs::File,
    io::{AsyncReadExt, AsyncSeekExt},
    task::JoinSet,
};

use crate::{parser, update, Temperature};

#[tokio::main]
pub async fn with_tokio() -> Vec<(String, Temperature)> {
    let file = File::open("measurements.txt").await.unwrap();
    let length = file.metadata().await.unwrap().len() as usize;

    let chunk_count = 8;
    let chunk_length = length / chunk_count;
    let last_chunk_length = chunk_length + length % chunk_count;

    let mut chunks: Vec<(usize, usize)> = (0..chunk_count)
        .map(|index| {
            let start = index * chunk_length;
            let end = start
                + if index < chunk_count - 1 {
                    chunk_length
                } else {
                    last_chunk_length
                };
            (start, end)
        })
        .collect();

    if chunk_count > 1 {
        for index in 1..chunk_count {
            let (_, end) = chunks[index - 1];
            let mut file = File::open("measurements.txt").await.unwrap();
            file.seek(SeekFrom::Start(end as u64)).await.unwrap();
            let mut buffer = [0u8; 100];
            let _read = file.read(&mut buffer).await.unwrap();
            let mut extra = 0;
            while buffer[extra] != b'\n' {
                extra += 1;
            }
            extra += 1;
            let (_, end) = &mut chunks[index - 1];
            *end += extra;
            let (start, _) = &mut chunks[index];
            *start += extra;
        }
    }

    let mut tasks = JoinSet::new();
    for (start, end) in chunks {
        let chunk_size = end - start;
        tasks.spawn(async move {
            let mut results: AHashMap<String, Temperature> = AHashMap::with_capacity(10000);
            let mut file = File::open("measurements.txt").await.unwrap();
            file.seek(SeekFrom::Start(start as u64)).await.unwrap();
            let mut buffer = vec![0u8; 4 * 1024 * 1024];
            let mut buffer_start = 0;
            let mut read = 0;
            loop {
                let received = file.read(&mut buffer[buffer_start..]).await.unwrap();
                let mut iterator = iterator(buffer[..buffer_start + received].as_bytes(), parser);
                iterator.for_each(|(city, temperature)| {
                    let city = unsafe { from_utf8_unchecked(city) };
                    let temperature = Temperature::new(temperature);
                    update(&mut results, city, &temperature);
                });
                read += received;
                if read >= chunk_size {
                    break;
                } else {
                    let (rest, _) = iterator.finish().unwrap();
                    let rest = rest.to_vec();
                    buffer_start = rest.len();
                    buffer[0..rest.len()].copy_from_slice(&rest);
                }
            }
            results
        });
    }

    let mut results: AHashMap<String, Temperature> = AHashMap::with_capacity(10000);
    while let Some(Ok(result)) = tasks.join_next().await {
        for (city, temperature) in result.iter() {
            update(&mut results, city, temperature);
        }
    }

    results.into_iter().collect::<Vec<(String, Temperature)>>()
}
