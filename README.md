## Rust-Practice

some tiny learning projects in Rust, awesome!

* [mini-bitcask-rs](./mini-bitcask-rs) — a minimal Bitcask-model key-value disk storage engine, implementing the core log-structured set/get/delete/scan/merge logic in a few hundred lines.
* [expr-eval](./expr-eval) — an arithmetic expression parser and evaluator built around operator precedence, in about 200 lines.
* [mvcc](./mvcc) — a minimal Multi-Version Concurrency Control implementation supporting transaction commit/rollback and set/get/delete/scan operations.

---

### mini-bitcask-rs

**What is Bitcask?** Bitcask is a log-structured key-value storage model (originally from Riak). Instead of updating data in place, every write — `set` or `delete` — is *appended* to the end of a log file. An in-memory hash index (called the "keydir") maps each key to the offset/length of its most recent entry in the log. Old versions of a key just become dead space in the log until a **merge** (compaction) rewrites only the live entries into a fresh file and reclaims disk space. This design trades some memory (one index entry per key) for very fast, mostly-sequential writes and O(1) reads.

**How it's implemented here, in the simplest way:**
- The log file stores entries as `key_len(4B) | value_len_or_tombstone(4B) | key | value`; a negative length marks a tombstone (delete).
- `keydir: BTreeMap<Vec<u8>, (offset, length)>` is rebuilt in memory by scanning the log once on startup.
- `set`/`delete` just append a new entry and update the keydir; `get` looks up the keydir then reads that one slice of the file.
- `merge` walks the current keydir (i.e. only the live keys), rewrites them into a new log file, and swaps it in — dropping tombstones and superseded versions.

### mvcc

**What is MVCC?** Multi-Version Concurrency Control lets readers and writers work concurrently without blocking each other. Instead of overwriting a value, every write creates a **new version** tagged with a monotonically increasing version number. A transaction takes a version number when it begins and only ever sees versions that were already committed at that point (its "snapshot") — giving repeatable reads without taking read locks.

**How it's implemented here, in the simplest way:**
- A single global `AtomicU64` counter hands out the next version number to each new transaction.
- Data is stored as `(raw_key, version) -> value` in one `BTreeMap`, so all versions of a key sit next to each other in key order.
- A transaction remembers the set of transaction ids that were still active (uncommitted) when it began; a version is visible to it only if that version's writer isn't in that active set and its version number isn't newer than the transaction's own.
- `get` scans a key's versions from newest to oldest and returns the first visible one; `commit` just drops the transaction from the active set; `rollback` deletes everything it wrote and drops it from the active set.
- A write checks the latest version of the key first — if that version isn't visible to the current transaction (i.e. another concurrent transaction touched it), it panics with a serialization error instead of silently overwriting.

### expr-eval

A small arithmetic expression evaluator based on **operator-precedence parsing**: a `Tokenizer` turns the input string into a stream of `Token`s (numbers, operators, parentheses), and `Expr::compute_expr` recursively evaluates left-to-right while only descending into a sub-expression when the next operator binds tighter than the current minimum precedence (`min_prec`). Right-associative operators (like `^`) are handled by *not* bumping `min_prec` on recursion, while left-associative ones bump it by one — this one trick is what makes the whole algorithm fit in ~150 lines without a separate AST.

