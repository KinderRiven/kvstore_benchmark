# LevelDB Micro-Benchmark

LevelDB is a fast key-value storage library written at Google that provides an ordered mapping from string keys to string values.

[Open Source Code](https://github.com/google/leveldb/)

# Evaluation parameter description

* key_length: Key size

* value_length: Value size

* num_server_thread: Number of threads operating on the DB.

* num_warm: How many KVs does the DB have at the beginning.

* num_put: Insert count.

* num_get: Search count.

* num_delete: Delete count.

* num_scan: Scan count.

* scan_range: How many keys are obtained in one scan.

* seed: Seed for random data.

* seq: 0 is random read/write, 1 is seq read/write.

* max_file_size: SSTable size (64MB default).

* write_buffer_size: MemTable size (64MB default).

* bloom_bits: THe bloom filter bits allocated per key.

* db: The path of data (SSTable).

 