use lazy_static::lazy_static;
use serde::{Deserialize, Serialize};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

// Storage engine definition, here we just use a simple in-memory BTreeMap
pub type KVEngine = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

// Globally increasing version number
static VERSION: AtomicU64 = AtomicU64::new(1);

// Get the next version number
fn acquire_next_version() -> u64 {
    let version = VERSION.fetch_add(1, Ordering::SeqCst);
    version
}

lazy_static! {
    // Currently active transaction ids, along with the keys they've already written
    static ref ACTIVE_TXN: Arc<Mutex<HashMap<u64, Vec<Vec<u8>>>>> = Arc::new(Mutex::new(HashMap::new()));
}

// MVCC definition
pub struct MVCC {
    // Underlying KV storage engine
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

// MVCC transaction
pub struct Transaction {
    // Underlying KV storage engine
    kv: Arc<Mutex<KVEngine>>,
    // Transaction version number
    version: u64,
    // List of active transactions when this transaction started
    active_xid: HashSet<u64>,
}

impl Transaction {
    // Begin a transaction
    pub fn begin(kv: Arc<Mutex<KVEngine>>) -> Self {
        // Acquire the global transaction version number
        let version = acquire_next_version();

        let mut active_txn = ACTIVE_TXN.lock().unwrap();
        // The keys of this map are all currently active transactions
        let active_xid = active_txn.keys().cloned().collect();

        // Add to the list of currently active transaction ids
        active_txn.insert(version, vec![]);

        // Return the result
        Self {
            kv,
            version,
            active_xid,
        }
    }

    // Write data
    pub fn set(&self, key: &[u8], value: Vec<u8>) {
        self.write(key, Some(value))
    }

    // Delete data
    pub fn delete(&self, key: &[u8]) {
        self.write(key, None)
    }

    fn write(&self, key: &[u8], value: Option<Vec<u8>>) {
        // Check whether the key being written conflicts with another transaction.
        // Keys are sorted by key-version, so we only need to check the most recent one
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

        // Record the TxnWrite
        let mut active_txn = ACTIVE_TXN.lock().unwrap();
        active_txn
            .entry(self.version)
            .and_modify(|keys| keys.push(key.to_vec()))
            .or_insert_with(|| vec![key.to_vec()]);

        // Write the data
        let enc_key = Key {
            raw_key: key.to_vec(),
            version: self.version,
        };
        kvengine.insert(enc_key.encode(), value);
    }

    // Read data, iterating from the last record to find the first visible one
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

    // Print out all visible data
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

    // Commit the transaction
    pub fn commit(&self) {
        // Remove this transaction from the active list
        let mut active_txn = ACTIVE_TXN.lock().unwrap();
        active_txn.remove(&self.version);
    }

    // Roll back the transaction
    pub fn rollback(&self) {
        // Remove the data that was written
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

        // Remove this transaction from the active list
        active_txn.remove(&self.version);
    }

    // Determine whether a given version of data is visible to the current transaction:
    // 1. Not visible if it's a modification from another active transaction
    // 2. Not visible if the version number is greater than the current one
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
    // Insert a few records first
    let tx0 = mvcc.begin_transaction();
    tx0.set(b"a", b"a1".to_vec());
    tx0.set(b"b", b"b1".to_vec());
    tx0.set(b"c", b"c1".to_vec());
    tx0.set(b"d", b"d1".to_vec());
    tx0.set(b"e", b"e1".to_vec());
    tx0.commit();

    // Begin a transaction
    let tx1 = mvcc.begin_transaction();
    // Change a to a2, e to e2
    tx1.set(b"a", b"a2".to_vec());
    tx1.set(b"e", b"e2".to_vec());
    // Time
    //  1  a2              e2
    //  0  a1  b1  c1  d1  e1
    //     a   b   c   d   e   Keys

    // Although t1 hasn't committed yet, it can see its own changes
    tx1.print_all(); // a=a2 b=b1 c=c1 d=d1 e=e2

    // Begin a new transaction
    let tx2 = mvcc.begin_transaction();
    // Delete b
    tx2.delete(b"b");
    // Time
    //  2      X
    //  1  a2              e2
    //  0  a1  b1  c1  d1  e1
    //     a   b   c   d   e   Keys

    // At this point T1 hasn't committed, so T2 sees
    tx2.print_all(); // a=a1 c=c1 d=d1 e=e1
                     // Commit T1
    tx1.commit();
    // T2 still can't see T1's commit, because when T2 started, T2 hadn't committed yet (repeatable read)
    tx2.print_all(); // a=a1 c=c1 d=d1 e=e1

    // Begin another new transaction
    let tx3 = mvcc.begin_transaction();
    // Time
    //  3
    //  2      X               uncommitted
    //  1  a2              e2  committed
    //  0  a1  b1  c1  d1  e1
    //     a   b   c   d   e   Keys
    // T3 can see T1's commit, but not T2's commit
    tx3.print_all(); // a=a2 b=b1 c=c1 d=d1 e=e2

    // T3 writes new data
    tx3.set(b"f", b"f1".to_vec());
    // T2 writes the same data, which will conflict
    tx2.set(b"f", b"f1".to_vec());
}
