use rocksdb;

use std::path::Path;

use std::{env};
use std::io::{self, Write};

#[derive(Debug)]
pub struct DB {
    db: rocksdb::DB,
}

pub type Bytes = Vec<u8>;

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

fn main() {
    // CLI 인자 가져오기
    let args: Vec<String> = env::args().collect();
    // if args.len() != 2 {
    //     println!("Usage: {} <path_to_db>", args[0]);
    //     return;
    // }
    let db_path = "/home/ubuntu/data2/electrs/db/mainnet/newindex/";
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

        // 값 저장
        let value = db.get(key.as_bytes()).unwrap();
        println!("Value '{:?}'", value);
    }

    // RocksDB 닫기
    drop(db);
}
