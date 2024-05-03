use std::{collections::{BTreeMap, HashSet, HashMap}, sync::{Arc, Mutex, atomic::{AtomicU64, Ordering}}};
use lazy_static::lazy_static;
use serde::{Serialize, Deserialize};

// 存储引擎定义，这里使用一个简单的内存 BTreeMap
pub type KVEngine = BTreeMap<Vec<u8>, Vec<u8>>;

// 全局递增的版本号
static VERSION: AtomicU64 = AtomicU64::new(1);

fn acquire_txn_version() -> u64 {
    let version = VERSION.fetch_add(1, Ordering::SeqCst);
    version
}

lazy_static! {
    // 当前活跃的事务 id，及其已经写入的 key 信息
    static ref ACTIVE_TXN: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>> = Arc::new(Mutex::new(HashMap::new()));
}

pub struct MVCC {
    kv: Arc<Mutex<KVEngine>>,
}

impl MVCC {
    pub fn new(kv: KVEngine) -> Self {
        Self {
            kv: Arc::new(Mutex::new(kv)),
        }
    }

    pub fn new_tx(&self) -> Transaction {
        Transaction::begin(self.kv.clone())
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct Key {
    row_key: Vec<u8>,
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
    pub fn begin(kv: Arc<Mutex<KVEngine>>) ->Self {
        // 获取全局事务号
        let version = acquire_txn_version();
        
        let mut active_txn = ACTIVE_TXN.lock().unwrap();
        let active_xid = active_txn.keys().cloned().collect();
        
        // 添加到当前活跃事务 id 列表中
        active_txn.insert(version, vec![]);

        // 返回结果
        Self { kv, version, active_xid}
    }

    // 写入数据
    pub fn set(&self, key: &[u8], value: Vec<u8>) {
        // 判断当前写入的 key 是否和其他的事务冲突
        let mut kvengine = self.kv.lock().unwrap();
        for (enc_key, _) in kvengine.iter().rev() {
            let key_version = decode_key(enc_key);
            if key_version.row_key.eq(key) {
                if !self.is_visible(key_version.version) {
                    panic!("serialization error");
                }
                break;
            }
        }

        // 写入 TxnWrite
        let mut active_txn = ACTIVE_TXN.lock().unwrap();
        active_txn.entry(self.version)
            .and_modify(|keys|keys.push(key.to_vec()))
            .or_insert_with(|| vec![key.to_vec()]);

        // 写入数据
        let enc_key = Key { row_key: key.to_vec(), version: self.version };
        kvengine.insert(enc_key.encode(), value);
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let kvengine = self.kv.lock().unwrap();
        for (k, v) in kvengine.iter().rev() {
            let key_version = decode_key(k);
            if key_version.row_key.eq(key) && self.is_visible(key_version.version) {
                return Some(v.to_vec());
            }
        }
        None
    }

    pub fn print_all(&self) {
        let kvengine = self.kv.lock().unwrap();
        for (k, v) in kvengine.iter().rev() {
            let key_version = decode_key(k);
            if self.is_visible(key_version.version) {
                println!("key = {:?}, value = {:?}", 
                    String::from_utf8(key_version.row_key.to_vec()), String::from_utf8(v.to_vec()));
            }
        }
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
                let enc_key = Key {row_key: k.to_vec(), version: self.version};
                let res = kvengine.remove(&enc_key.encode());
                assert!(res.is_some());
            }
        }

        // 清除 TxnWrite 的数据
        active_txn.remove(&self.version);
    }

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
    let tx1 = mvcc.new_tx();

    tx1.set(b"a", b"val1".to_vec());
    tx1.set(b"a", b"val11".to_vec());

    tx1.set(b"b", b"val2".to_vec());
    tx1.set(b"c", b"val3".to_vec());

    // tx1.commit();
    // tx1.rollback();

    let tx2 = mvcc.new_tx();
    tx2.print_all();
    tx2.set(b"dd", b"val22".to_vec());

    tx1.commit();
    tx2.print_all();
}
