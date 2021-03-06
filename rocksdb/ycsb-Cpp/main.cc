#include <stdio.h>

#include "benchmark.h"
#include "easylogging/easylogging++.h"
#include "run_workload.h"

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"

INITIALIZE_EASYLOGGINGPP

using namespace rocksdb;

#define SCAN_RANGE 100

static int num_workloads = 6;
// static int ycsb_workloads[] = { YCSB_ONLY_WRITE };
static int zipfan = 1;
static size_t dbsize = 128 * 1024;
static int ycsb_workloads[] = { YCSB_A, YCSB_B, YCSB_C, YCSB_D, YCSB_E, YCSB_F };
static size_t workload_size[] = { 32 * 1024, 32 * 1024, 32 * 1024, 32 * 1024, 32 * 1024 }; // MB
static int workload_kv_type = 2;
static size_t workload_kv_length[] = { 256, 4096 };
static double workload_kv_proportion[] = { 0.5, 0.5 };

int main(int argc, char* argv[])
{
    char ssd_path[128] = "./pika_store";
    char pmem_file_path[128] = "/home/pmem0/pm";
    size_t pmem_file_size = (size_t)512 * 1024 * 1024;
    // Benchmark
    int num_server_thread = 1; // server thread
    int num_backend_thread = 1; // backend thread
    // LSM-Tree
    uint64_t max_file_size = 64 * 1024 * 1024;
    uint64_t write_buffer_size = 64 * 1024 * 1024;
    uint64_t bloom_bits = 10;
    uint64_t block_size = 4096;

    printf("FFFF_1 (%llu)\n", block_size);
    for (int i = 0; i < argc; i++) {
        double d;
        uint64_t n;
        char junk;
        if (sscanf(argv[i], "--pmem_file_size=%llu%c", &n, &junk) == 1) {
            pmem_file_size = n * 1024 * 1024;
        } else if (sscanf(argv[i], "--num_server_thread=%llu%c", &n, &junk) == 1) {
            num_server_thread = n;
        } else if (sscanf(argv[i], "--num_backend_thread=%llu%c", &n, &junk) == 1) {
            num_backend_thread = n;
        } else if (strncmp(argv[i], "--db=", 5) == 0) {
            strcpy(ssd_path, argv[i] + 5);
        } else if (strncmp(argv[i], "--nvm=", 6) == 0) {
            strcpy(pmem_file_path, argv[i] + 6);
        } else if (i > 0) {
            LOG(INFO) << "Error Parameter [" << argv[i] << "]";
            return 0;
        }
    }

    printf("FFFF_2 (%llu)\n", block_size);
    Options options;
    options.compression = kNoCompression;
    options.write_buffer_size = write_buffer_size;
    BlockBasedTableOptions table_options;
    table_options.no_block_cache = true;
    table_options.filter_policy.reset(NewBloomFilterPolicy(10, false));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.create_if_missing = true;
    options.target_file_size_base = max_file_size;

    printf("FFFF_3 (%llu)\n", block_size);
    LOG(INFO) << "|-----------------[RocksDB]-----------------";
    LOG(INFO) << "|- [db path:" << ssd_path << "]";
    LOG(INFO) << "|- [write_buffer_size:" << write_buffer_size / (1024 * 1024) << "MB]";
    LOG(INFO) << "|- [max_file_size:" << max_file_size / (1024 * 1024) << "MB]";
    LOG(INFO) << "|- [block_size:" << block_size << "]";
    LOG(INFO) << "|- [bloom_bits:" << bloom_bits << "]";
    LOG(INFO) << "|-------------------------------------------";
    DB* db = nullptr;
    Status status = DB::Open(options, ssd_path, &db);
    assert(db != nullptr);
    assert(status.ok());

    printf("FFFF_4\n");
    // Benchmark* benchmark = new YCSB_Benchmark(YCSB_ONLY_WRITE, num_server_thread, dbsize * 1024 * 1024, dbsize * 1024 * 1024, workload_kv_type, workload_kv_length, workload_kv_proportion, SCAN_RANGE / num_backend_thread);
    Benchmark* benchmark = new YCSB_Benchmark(YCSB_SEQ_LOAD | zipfan, num_server_thread, dbsize * 1024 * 1024, dbsize * 1024 * 1024, workload_kv_type, workload_kv_length, workload_kv_proportion, SCAN_RANGE / num_backend_thread);
    Workload* workload = new Workload(benchmark, db, num_server_thread);
    workload->Run();
    benchmark->Print();
    for (int i = 0; i < num_workloads; i++) {
        if (ycsb_workloads[i] == YCSB_E) {
            benchmark = new YCSB_Benchmark(ycsb_workloads[i] | zipfan, num_server_thread, dbsize * 1024 * 1024, workload_size[i] * 1024 * 1024 / 10, workload_kv_type, workload_kv_length, workload_kv_proportion, SCAN_RANGE / num_backend_thread);
        } else {
            benchmark = new YCSB_Benchmark(ycsb_workloads[i] | zipfan, num_server_thread, dbsize * 1024 * 1024, workload_size[i] * 1024 * 1024, workload_kv_type, workload_kv_length, workload_kv_proportion, SCAN_RANGE / num_backend_thread);
        }
        workload = new Workload(benchmark, db, num_server_thread);
        workload->Run();
        benchmark->Print();
    }
    return 0;
}
