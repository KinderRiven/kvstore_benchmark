#include <stdio.h>

#include "benchmark.h"
#include "easylogging/easylogging++.h"
#include "run_workload.h"

#include "rocksdb/db.h"
#include "rocksdb/filter_policy.h"
#include "rocksdb/table.h"

INITIALIZE_EASYLOGGINGPP

using namespace rocksdb;

static int num_workloads = 1;
static int ycsb_workloads[] = { YCSB_A };

int main(int argc, char* argv[])
{
    Benchmark* warm_benchmark;
    Benchmark* run_benchmark;
    // DB_DB
    char ssd_path[128] = "./pika_store";
    char pmem_file_path[128] = "/home/pmem0/pm";
    char benchmark_type[128] = "ycsb_a, ycsb_b, ycsb_c, ycsb_d, ycsb_e, ycsb_f";
    size_t pmem_file_size = (size_t)512 * 1024 * 1024;
    // Benchmark
    int seq = 0; // seq or random
    int num_server_thread = 2; // server thread
    int num_backend_thread = 1; // backend thread
    uint64_t num_warm_opt[OPT_TYPE_COUNT] = { 500000, 0, 0, 0, 0 };
    uint64_t num_run_opt[OPT_TYPE_COUNT] = { 100000, 0, 0, 0, 0 };
    int warm_seed[OPT_TYPE_COUNT] = { 1000, 0, 0, 0, 0 };
    int run_seed[OPT_TYPE_COUNT] = { 2000, 1000, 1000, 1000, 1000 };
    uint64_t scan_range = 100;
    // LSM-Tree
    uint64_t max_file_size = 64 * 1024 * 1024;
    uint64_t write_buffer_size = 64 * 1024 * 1024;
    uint64_t bloom_bits = 10;
    uint64_t block_size = 4096;

    printf("FFFF_1\n");

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
        } else if (sscanf(argv[i], "--num_warm=%llu%c", &n, &junk) == 1) {
            num_warm_opt[OPT_PUT] = n;
        } else if (sscanf(argv[i], "--num_put=%llu%c", &n, &junk) == 1) {
            num_run_opt[OPT_PUT] = n;
        } else if (sscanf(argv[i], "--num_update=%llu%c", &n, &junk) == 1) {
            num_run_opt[OPT_UPDATE] = n;
        } else if (sscanf(argv[i], "--num_get=%llu%c", &n, &junk) == 1) {
            num_run_opt[OPT_GET] = n;
        } else if (sscanf(argv[i], "--num_delete=%llu%c", &n, &junk) == 1) {
            num_run_opt[OPT_DELETE] = n;
        } else if (sscanf(argv[i], "--num_scan=%llu%c", &n, &junk) == 1) {
            num_run_opt[OPT_SCAN] = n;
        } else if (sscanf(argv[i], "--scan_range=%llu%c", &n, &junk) == 1) {
            scan_range = n;
        } else if (sscanf(argv[i], "--seed=%llu%c", &n, &junk) == 1) {
            warm_seed[0] = n;
            run_seed[1] = run_seed[2] = run_seed[3] = run_seed[4] = n;
        } else if (sscanf(argv[i], "--seq=%llu%c", &n, &junk) == 1) {
            seq = n;
        } else if (strncmp(argv[i], "--db=", 5) == 0) {
            strcpy(ssd_path, argv[i] + 5);
        } else if (strncmp(argv[i], "--nvm=", 6) == 0) {
            strcpy(pmem_file_path, argv[i] + 6);
        } else if (strncmp(argv[i], "--benchmark=", 12) == 0) {
            strcpy(benchmark_type, argv[i] + 12);
        } else if (i > 0) {
            LOG(INFO) << "Error Parameter [" << argv[i] << "]";
            return 0;
        }
    }

    printf("FFFF_2\n");

    Options options;
    options.compression = kNoCompression;
    options.write_buffer_size = write_buffer_size;
    BlockBasedTableOptions table_options;
    table_options.filter_policy.reset(NewBloomFilterPolicy(bloom_bits, false));
    options.table_factory.reset(NewBlockBasedTableFactory(table_options));
    options.create_if_missing = true;
    options.target_file_size_base = max_file_size;

    printf("FFFF_3\n");

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

    warm_benchmark = new YCSB_Benchmark(YCSB_SEQ_LOAD, num_server_thread, num_warm_opt[0], num_warm_opt[0]);
    Workload* warm_workload = new Workload(warm_benchmark, nullptr, num_server_thread);
    warm_workload->Run();
    warm_benchmark->print();

    for (int i = 0; i < num_workloads; i++) {
        run_benchmark = new YCSB_Benchmark(ycsb_workloads[i], num_server_thread, num_warm_opt[0], num_run_opt[0]);
        Workload* run_workload = new Workload(run_benchmark, nullptr, num_server_thread);
        run_workload->Run();
        run_benchmark->print();
    }
    return 0;
}
