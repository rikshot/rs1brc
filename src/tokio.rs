use tokio::{fs::File, sync::mpsc};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Bytes, BytesMut},
    codec::{Decoder, FramedRead},
};

use crate::{
    parser::{parser_branchless, ResultMap},
    Temperature,
};

static BUFFER_SIZE: usize = 8 * 1024 * 1024;

struct ChunkDecoder;

impl Decoder for ChunkDecoder {
    type Item = Bytes;
    type Error = std::io::Error;

    #[inline]
    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        match memchr::memrchr(b'\n', src) {
            Some(index) => {
                let data = src.split_to(index + 1);
                src.reserve(BUFFER_SIZE);
                Ok(Some(data.freeze()))
            }
            None => Ok(None),
        }
    }
}

#[tokio::main(flavor = "current_thread")]
pub async fn with_decoder() -> Vec<(String, Temperature)> {
    let (tx, mut rx) = mpsc::unbounded_channel::<ResultMap>();
    tokio::spawn(async move {
        let mut file = File::open("measurements.txt").await.unwrap();
        file.set_max_buf_size(BUFFER_SIZE);
        let mut framed = FramedRead::with_capacity(file, ChunkDecoder, BUFFER_SIZE);
        while let Some(Ok(chunk)) = framed.next().await {
            let tx = tx.clone();
            tokio::task::spawn_blocking(move || {
                tx.send(parser_branchless(&chunk)).unwrap();
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
