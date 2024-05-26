use bytes::{Bytes, BytesMut};
use tokio::{fs::File, sync::mpsc};
use tokio_stream::StreamExt;
use tokio_util::codec::{Decoder, Framed};

use crate::{
    parser::{parser, ResultMap},
    Temperature,
};

// Tokio MAX_BUF for blocking IO: https://github.com/tokio-rs/tokio/blob/master/tokio/src/io/blocking.rs#L26
static BUFFER_SIZE: usize = 2 * 1024 * 1024;

struct ChunkDecoder;

impl Decoder for ChunkDecoder {
    type Item = Bytes;
    type Error = std::io::Error;

    #[inline]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match memchr::memrchr(b'\n', src) {
            Some(index) => {
                let mut chunk = BytesMut::with_capacity(BUFFER_SIZE);
                let data = src.split_to(index + 1);
                chunk.extend_from_slice(&[0u8].repeat(BUFFER_SIZE - data.len()));
                chunk.extend_from_slice(&data);
                Ok(Some(chunk.freeze()))
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
        let mut framed = Framed::with_capacity(file, ChunkDecoder, BUFFER_SIZE);
        while let Some(Ok(chunk)) = framed.next().await {
            let tx = tx.clone();
            tokio::task::spawn_blocking(move || {
                tx.send(parser(&chunk)).unwrap();
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
