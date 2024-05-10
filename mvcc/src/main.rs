use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

// 存储引擎定义，这里使用一个简单的内存 BTreeMap
pub type KVEngine = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

// 全局递增的版本号
static VERSION: AtomicU64 = AtomicU64::new(1);

// 获取下一个版本号
fn acquire_next_version() -> u64 {
    let version = VERSION.fetch_add(1, Ordering::SeqCst);
    version
}

lazy_static! {
    // 当前活跃的事务 id，及其已经写入的 key 信息
    static ref ACTIVE_TXN: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>> = Arc::new(Mutex::new(HashMap::new()));
}

// MVCC 事务定义
pub struct MVCC {
    // KV 存储引擎
    kv: Arc<Mutex<KVEngine>>,
}

impl MVCC {
    pub fn new(kv: KVEngine) -> Self {
        Self {
            kv: Arc::new(Mutex::new(kv)),
        }
    }

    pub fn begin_transaction(&self) -> Transaction {
        Transaction::begin(self.kv.clone())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Key {
    raw_key: Vec<u8>,
    version: u64,
}

impl Key {
    fn encode(&self) -> Vec<u8> {
        bincode::serialize(self).unwrap()
    }
}

fn decode_key(b: &Vec<u8>) -> Key {
    bincode::deserialize(&b).unwrap()
}

// MVCC 事务
pub struct Transaction {
    // 底层 KV 存储引擎
    kv: Arc<Mutex<KVEngine>>,
    // 事务版本号
    version: u64,
    // 事务启动时的活跃事务列表
    active_xid: HashSet<u64>,
}

impl Transaction {
    // 开启事务
    pub fn begin(kv: Arc<Mutex<KVEngine>>) -> Self {
        // 获取全局事务版本号
        let version = acquire_next_version();

        let mut active_txn = ACTIVE_TXN.lock().unwrap();
        // 这个 map 的 key 就是当前所有活跃的事务
        let active_xid = active_txn.keys().cloned().collect();

        // 添加到当前活跃事务 id 列表中
        active_txn.insert(version, vec![]);

        // 返回结果
        Self {
            kv,
            version,
            active_xid,
        }
    }

    // 写入数据
    pub fn set(&self, key: &[u8], value: Vec<u8>) {
        self.write(key, Some(value))
    }

    // 删除数据
    pub fn delete(&self, key: &[u8]) {
        self.write(key, None)
    }

    fn write(&self, key: &[u8], value: Option<Vec<u8>>) {
        // 判断当前写入的 key 是否和其他的事务冲突
        // key 是按照 key-version 排序的，所以只需要判断最近的一个 key 即可
        let mut kvengine = self.kv.lock().unwrap();
        for (enc_key, _) in kvengine.iter().rev() {
            let key_version = decode_key(enc_key);
            if key_version.raw_key.eq(key) {
                if !self.is_visible(key_version.version) {
                    panic!("serialization error, try again.");
                }
                break;
            }
        }

        // 写入 TxnWrite
        let mut active_txn = ACTIVE_TXN.lock().unwrap();
        active_txn
            .entry(self.version)
            .and_modify(|keys| keys.push(key.to_vec()))
            .or_insert_with(|| vec![key.to_vec()]);

        // 写入数据
        let enc_key = Key {
            raw_key: key.to_vec(),
            version: self.version,
        };
        kvengine.insert(enc_key.encode(), value);
    }

    // 读取数据，从最后一条数据进行遍历，找到第一条可见的数据
    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let kvengine = self.kv.lock().unwrap();
        for (k, v) in kvengine.iter().rev() {
            let key_version = decode_key(k);
            if key_version.raw_key.eq(key) && self.is_visible(key_version.version) {
                return v.clone();
            }
        }
        None
    }

    // 打印出所有可见的数据
    fn print_all(&self) {
        let mut records = BTreeMap::new();
        let kvengine = self.kv.lock().unwrap();
        for (k, v) in kvengine.iter() {
            let key_version = decode_key(k);
            if self.is_visible(key_version.version) {
                records.insert(key_version.raw_key.to_vec(), v.clone());
            }
        }

        for (k, v) in records.iter() {
            if let Some(value) = v {
                print!(
                    "{}={} ",
                    String::from_utf8_lossy(k),
                    String::from_utf8_lossy(value)
                );
            }
        }
        println!("");
    }

    // 提交事务
    pub fn commit(&self) {
        // 清除活跃事务列表中的数据
        let mut active_txn = ACTIVE_TXN.lock().unwrap();
        active_txn.remove(&self.version);
    }

    // 回滚事务
    pub fn rollback(&self) {
        // 清除写入的数据
        let mut active_txn = ACTIVE_TXN.lock().unwrap();
        if let Some(keys) = active_txn.get(&self.version) {
            let mut kvengine = self.kv.lock().unwrap();
            for k in keys {
                let enc_key = Key {
                    raw_key: k.to_vec(),
                    version: self.version,
                };
                let res = kvengine.remove(&enc_key.encode());
                assert!(res.is_some());
            }
        }

        // 清除活跃事务列表中的数据
        active_txn.remove(&self.version);
    }

    // 判断一个版本的数据对当前事务是否可见
    // 1. 如果是另一个活跃事务的修改，则不可见
    // 2. 如果版本号比当前大，则不可见
    fn is_visible(&self, version: u64) -> bool {
        if self.active_xid.contains(&version) {
            return false;
        }
        version <= self.version
    }
}

fn main() {
    let eng = KVEngine::new();
    let mvcc = MVCC::new(eng);
    // 先新增几条数据
    let tx0 = mvcc.begin_transaction();
    tx0.set(b"a", b"a1".to_vec());
    tx0.set(b"b", b"b1".to_vec());
    tx0.set(b"c", b"c1".to_vec());
    tx0.set(b"d", b"d1".to_vec());
    tx0.set(b"e", b"e1".to_vec());
    tx0.commit();

    // 开启一个事务
    let tx1 = mvcc.begin_transaction();
    // 将 a 改为 a2，e 改为 e2
    tx1.set(b"a", b"a2".to_vec());
    tx1.set(b"e", b"e2".to_vec());
    // Time
    //  1  a2              e2
    //  0  a1  b1  c1  d1  e1
    //     a   b   c   d   e   Keys

    // t1 虽然未提交，但是能看到自己的修改了
    tx1.print_all(); // a=a2 b=b1 c=c1 d=d1 e=e2

    // 开启一个新的事务
    let tx2 = mvcc.begin_transaction();
    // 删除 b
    tx2.delete(b"b");
    // Time
    //  2      X
    //  1  a2              e2
    //  0  a1  b1  c1  d1  e1
    //     a   b   c   d   e   Keys

    // 此时 T1 没提交，所以 T2 看到的是
    tx2.print_all(); // a=a1 c=c1 d=d1 e=e1
                     // 提交 T1
    tx1.commit();
    // 此时 T2 仍然看不到 T1 的提交，因为 T2 开启的时候，T2 还没有提交（可重复读）
    tx2.print_all(); // a=a1 c=c1 d=d1 e=e1

    // 再开启一个新的事务
    let tx3 = mvcc.begin_transaction();
    // Time
    //  3
    //  2      X               uncommitted
    //  1  a2              e2  committed
    //  0  a1  b1  c1  d1  e1
    //     a   b   c   d   e   Keys
    // T3 能看到 T1 的提交，但是看不到 T2 的提交
    tx3.print_all(); // a=a2 b=b1 c=c1 d=d1 e=e2

    // T3 写新的数据
    tx3.set(b"f", b"f1".to_vec());
    // T2 写同样的数据，会冲突
    tx2.set(b"f", b"f1".to_vec());
}
