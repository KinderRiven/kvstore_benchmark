#include <stdio.h>

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"

#include "easylogging/easylogging++.h"
#include "micro_benchmark.h"

using namespace rocksdb;

INITIALIZE_EASYLOGGINGPP

int main(int argc, char* argv[])
{
    struct benchmark_param_t warm_param;
    struct benchmark_param_t test_param;
    char db_path[128] = "sst";
    char nvm_path[128] = "nvm";
    size_t key_length = 16;
    size_t value_length = 1024;
    int seq = 0;
    int num_server_thread = 1;
    int num_backend_thread = 1;
    uint64_t num_warm_opt = 500000;
    uint64_t num_put_opt = 0;
    uint64_t num_get_opt = 0;
    uint64_t num_delete_opt = 0;
    uint64_t num_scan_opt = 0;
    uint64_t scan_range = 1000;
    uint64_t seed = 1000;
    uint64_t max_file_size = 2 * 1024 * 1024;
    uint64_t nvm_buffer_size = (size_t)2 * 1024 * 1024 * 1024;
    uint64_t write_buffer_size = 64 * 1024 * 1024;
    uint64_t bloom_bits = 10;
    uint64_t block_size = 4096;
    uint64_t pmem_size = 512 * 1024 * 1024;

    for (int i = 0; i < argc; i++) {
        double d;
        uint64_t n;
        char junk;

        if (sscanf(argv[i], "--pmem_file_size=%llu%c", &n, &junk) == 1) {
        } else if (sscanf(argv[i], "--key_length=%llu%c", &n, &junk) == 1) {
            key_length = n;
            assert(key_length <= MAX_KEY_LENGTH);
        } else if (sscanf(argv[i], "--value_length=%llu%c", &n, &junk) == 1) {
            value_length = n;
            assert(value_length <= MAX_VALUE_LENGTH);
        } else if (sscanf(argv[i], "--num_server_thread=%llu%c", &n, &junk) == 1) {
            num_server_thread = n;
        } else if (sscanf(argv[i], "--num_backend_thread=%llu%c", &n, &junk) == 1) {
            num_backend_thread = n;
        } else if (sscanf(argv[i], "--num_warm=%llu%c", &n, &junk) == 1) {
            num_warm_opt = n;
        } else if (sscanf(argv[i], "--num_put=%llu%c", &n, &junk) == 1) {
            num_put_opt = n;
        } else if (sscanf(argv[i], "--num_get=%llu%c", &n, &junk) == 1) {
            num_get_opt = n;
        } else if (sscanf(argv[i], "--num_delete=%llu%c", &n, &junk) == 1) {
            num_delete_opt = n;
        } else if (sscanf(argv[i], "--num_scan=%llu%c", &n, &junk) == 1) {
            num_scan_opt = n;
        } else if (sscanf(argv[i], "--scan_range=%llu%c", &n, &junk) == 1) {
            scan_range = n;
        } else if (sscanf(argv[i], "--seed=%llu%c", &n, &junk) == 1) {
            seed = n;
        } else if (sscanf(argv[i], "--seq=%llu%c", &n, &junk) == 1) {
            seq = n;
        } else if (sscanf(argv[i], "--max_file_size=%llu%c", &n, &junk) == 1) {
            max_file_size = (uint64_t)n * 1024 * 1024;
        } else if (sscanf(argv[i], "--write_buffer_size=%llu%c", &n, &junk) == 1) {
            write_buffer_size = (uint64_t)n * 1024 * 1024;
        } else if (sscanf(argv[i], "--nvm_buffer_size=%llu%c", &n, &junk) == 1) {
            nvm_buffer_size = (uint64_t)n * 1024 * 1024;
        } else if (sscanf(argv[i], "--bloom_bits=%llu%c", &n, &junk) == 1) {
            bloom_bits = n;
        } else if (strncmp(argv[i], "--db=", 5) == 0) {
            strcpy(db_path, argv[i] + 5);
        } else if (strncmp(argv[i], "--nvm=", 6) == 0) {
            strcpy(nvm_path, argv[i] + 6);
        } else if (i > 0) {
            LOG(INFO) << "Error Parameter [" << argv[i] << "]!";
            return 0;
        }
    }

    Options options;
    options.compression = kNoCompression;
    options.write_buffer_size = write_buffer_size;
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(bloom_bits, false));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.create_if_missing = true;
    options.target_file_size_base = max_file_size;

    LOG(INFO) << "|-----------------[RocksDB]-----------------";
    LOG(INFO) << "|- [db path:" << db_path << "]";
    LOG(INFO) << "|- [key/value length:" << key_length << "B/" << value_length << "B]";
    LOG(INFO) << "|- [write_buffer_size:" << write_buffer_size / (1024 * 1024) << "MB]";
    LOG(INFO) << "|- [max_file_size:" << max_file_size / (1024 * 1024) << "MB]";
    LOG(INFO) << "|- [block_size:" << block_size << "]";
    LOG(INFO) << "|- [bloom_bits:" << bloom_bits << "]";
    LOG(INFO) << "|-------------------------------------------";

    DB* db = nullptr;
    Status status = DB::Open(options, db_path, &db);
    assert(db != nullptr);
    assert(status.ok());

    warm_param.num_thread = num_server_thread;
    warm_param.seq = seq;
    warm_param.num_put_opt = num_warm_opt;
    warm_param.key_length = key_length;
    warm_param.value_length = value_length;

    for (int i = 0; i < num_server_thread; i++) {
        warm_param.put_seed[i] = seed + (uint64_t)123456789 * (i + 1);
        warm_param.put_sequence_id[i] = (uint64_t)(i + 1) * 987654321;
    }

    MicroBenchmark* warm_benchmark = new MicroBenchmark(&warm_param, db);
    warm_benchmark->Run();

    test_param.num_thread = num_server_thread;
    test_param.num_put_opt = num_put_opt;
    test_param.num_get_opt = num_get_opt;
    test_param.num_delete_opt = num_delete_opt;
    test_param.num_scan_opt = num_scan_opt;
    test_param.scan_range = scan_range;
    test_param.seq = seq;
    test_param.key_length = key_length;
    test_param.value_length = value_length;

    for (int i = 0; i < num_server_thread; i++) {
        test_param.put_seed[i] = seed + (uint64_t)777777777 * (i + 1);
        test_param.put_sequence_id[i] = (uint64_t)(i + 1) * 666666666;
        test_param.get_seed[i] = warm_param.put_seed[i];
        test_param.get_sequence_id[i] = warm_param.put_sequence_id[i];
        test_param.delete_seed[i] = warm_param.put_seed[i];
        test_param.delete_sequence_id[i] = warm_param.put_sequence_id[i];
        test_param.scan_seed[i] = warm_param.put_seed[i];
        test_param.scan_sequence_id[i] = warm_param.put_sequence_id[i];
    }

    MicroBenchmark* test_benchmark = new MicroBenchmark(&test_param, db);
    test_benchmark->Run();
    return 0;
}
