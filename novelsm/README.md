# NoveLSM Micro-Benchmark

NoveLSM is a persistent-memory implementation of LevelDB. More details are in ATC '18 paper.

[Paper](https://www.usenix.org/system/files/conference/atc18/atc18-kannan.pdf)

[Open Source Code](https://github.com/SudarsunKannan/lsm_nvm)

# Evaluation parameter description

* nvm: The path of persistent memmory.

* nvm_buffer_size: Persistent MemTable size.

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

 