use std::str::from_utf8_unchecked;

use tokio::{fs::File, sync::mpsc};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::BytesMut,
    codec::{Decoder, Framed},
};

use crate::map::Map;
use crate::Temperature;

type ResultMap = Map<Temperature, 8000>;

fn parser(chunk: &[u8]) -> ResultMap {
    let mut results = ResultMap::new();

    let mut start = 0;
    memchr::memchr_iter(b'\n', chunk).for_each(|mid| {
        let line = &chunk[start..mid];
        let length = mid - start;
        let mut temp = line[length - 1] as i32 - 48 + (line[length - 3] as i32 - 48) * 10;
        let split = if line[length - 4] == b';' {
            length - 4
        } else if line[length - 4] == b'-' {
            temp = -temp;
            length - 5
        } else {
            temp += (line[length - 4] as i32 - 48) * 100;
            if line[length - 5] == b';' {
                length - 5
            } else {
                temp = -temp;
                length - 6
            }
        };
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

struct ChunkDecoder;

impl Decoder for ChunkDecoder {
    type Item = Vec<u8>;
    type Error = std::io::Error;

    #[inline]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match memchr::memrchr(b'\n', src) {
            Some(index) => {
                let data = src.split_to(index + 1);
                Ok(Some(data.to_vec()))
            }
            None => Ok(None),
        }
    }
}

#[tokio::main]
pub async fn with_decoder() -> Vec<(String, Temperature)> {
    let (tx, mut rx) = mpsc::unbounded_channel::<ResultMap>();
    tokio::spawn(async move {
        let file = File::open("measurements.txt").await.unwrap();
        // Tokio MAX_BUF for blocking IO: https://github.com/tokio-rs/tokio/blob/10c9eeb6c2af85961044b7cbb16a5a2d2e97287d/tokio/src/io/blocking.rs#L26
        let mut framed = Framed::with_capacity(file, ChunkDecoder, 2 * 1024 * 1024);
        while let Some(Ok(chunk)) = framed.next().await {
            let tx = tx.clone();
            tokio::task::spawn_blocking(move || {
                let results = parser(&chunk);
                tx.send(results).unwrap();
            });
        }
    });
    let results = tokio::task::spawn_blocking(move || {
        let mut results = ResultMap::new();
        while let Some(result) = rx.blocking_recv() {
            for entry in result.table.iter().flatten() {
                if let Some(value) = results.get_mut(&entry.key) {
                    value.update(&entry.value);
                } else {
                    results.set(&entry.key, &entry.value);
                }
            }
        }
        results
    });
    results
        .await
        .unwrap()
        .table
        .into_iter()
        .flatten()
        .map(|entry| (entry.key, entry.value))
        .collect::<Vec<(String, Temperature)>>()
}
