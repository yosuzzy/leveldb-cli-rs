use rocksdb;

use std::path::Path;

use bincode::Options;

use std::io::{self, Write};
use arrayref::array_ref;
use serde::{Deserialize, Serialize};

#[derive(Debug)]
pub struct DB {
    db: rocksdb::DB,
}

pub type Bytes = Vec<u8>;

pub fn serialize_big<T>(value: &T) -> Result<Vec<u8>, bincode::Error>
    where
        T: ?Sized + serde::Serialize,
{
    big_endian().serialize(value)
}

pub fn deserialize_big<'a, T>(bytes: &'a [u8]) -> Result<T, bincode::Error>
    where
        T: serde::Deserialize<'a>,
{
    big_endian().deserialize(bytes)
}

pub fn serialize_little<T>(value: &T) -> Result<Vec<u8>, bincode::Error>
    where
        T: ?Sized + serde::Serialize,
{
    little_endian().serialize(value)
}

pub fn deserialize_little<'a, T>(bytes: &'a [u8]) -> Result<T, bincode::Error>
    where
        T: serde::Deserialize<'a>,
{
    little_endian().deserialize(bytes)
}


impl DB {
    pub fn open(path: &Path) -> DB {
        let mut db_opts = rocksdb::Options::default();
        db_opts.create_if_missing(true);
        db_opts.set_max_open_files(100_000); // TODO: make sure to `ulimit -n` this process correctly
        db_opts.set_compaction_style(rocksdb::DBCompactionStyle::Level);
        db_opts.set_compression_type(rocksdb::DBCompressionType::Snappy);
        db_opts.set_target_file_size_base(1_073_741_824);
        db_opts.set_write_buffer_size(256 << 20);
        db_opts.set_disable_auto_compactions(true); // for initial bulk load

        // db_opts.set_advise_random_on_open(???);
        db_opts.set_compaction_readahead_size(1 << 20);
        db_opts.increase_parallelism(2);

        // let mut block_opts = rocksdb::BlockBasedOptions::default();
        // block_opts.set_block_size(???);

        let db = DB {
            db: rocksdb::DB::open(&db_opts, path).expect("failed to open RocksDB"),
        };
        db
    }
    pub fn get(&self, key: &[u8]) -> Option<Bytes> {
        self.db.get(key).unwrap().map(|v| v.to_vec())
    }
}
pub type FullHash = [u8; 32]; // serialized SHA256 result

#[derive(Serialize, Deserialize)]
struct BlockKey {
    code: u8,
    hash: FullHash,
}

struct BlockRow {
    key: BlockKey,
    value: Bytes, // serialized output
}

impl BlockRow {
    fn meta_key(hash: FullHash) -> Bytes {
        [b"M", &hash[..]].concat()
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct BlockMeta {
    #[serde(alias = "nTx")]
    pub tx_count: u32,
    pub size: u32,
    pub weight: u32,
}
const HASH_LEN: usize = 32;

pub fn full_hash(hash: &[u8]) -> FullHash {
    *array_ref![hash, 0, HASH_LEN]
}

fn hex_string_to_bytes(hex: &str) -> Vec<u8> {
    let mut bytes = Vec::new();
    for i in (0..hex.len()).step_by(2) {
        let byte = u8::from_str_radix(&hex[i..i+2], 16).expect("Invalid hex string");
        bytes.push(byte);
    }
    bytes
}

fn main() {
    // CLI 인자 가져오기
    let db_path = "/home/ubuntu/data2/electrs/db/mainnet/newindex/txstore";
    let path = Path::new(db_path);
    // RocksDB 열기
    let db = DB::open(path);

    loop {
        // 사용자로부터 키 입력 받기
        print!("Enter key (type 'q' to quit): ");
        io::stdout().flush().unwrap();
        let mut key = String::new();
        io::stdin().read_line(&mut key).unwrap();
        let key = key.trim();

        if key == "q" {
            break;
        }

        let hash: Vec<u8> = hex_string_to_bytes(&key);
        println!("hash: {:?}",hash);
        let hash_key = &BlockRow::meta_key(full_hash(&hash[..]));
        println!("hash_key: {:?}",hash_key);
        // 값 저장
        let value: Option<BlockMeta>  = db.get(hash_key)
            .map(|val| deserialize_little(&val).expect("failed to parse BlockMeta"));
        println!("Value '{:?}'", value);
    }

    // RocksDB 닫기
    drop(db);
}

#[inline]
fn options() -> impl Options {
    bincode::options()
        .with_fixint_encoding()
        .with_no_limit()
        .allow_trailing_bytes()
}

#[inline]
fn big_endian() -> impl Options {
    options().with_big_endian()
}

/// Adding the endian flag for little endian
#[inline]
fn little_endian() -> impl Options {
    options().with_little_endian()
}
