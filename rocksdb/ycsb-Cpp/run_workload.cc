#include "benchmark.h"
#include "timer.h"

#include "easylogging/easylogging++.h"
#include "run_workload.h"

#include <algorithm>
#include <assert.h>
#include <pthread.h>
#include <vector>

#include "rocksdb/db.h"
using namespace rocksdb;

static int numa_0[] = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9,
    20, 21, 22, 23, 24, 25, 26, 27, 28, 29,
    10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
    30, 31, 32, 33, 34, 35, 36, 37, 38, 39 };

struct thread_param_t {
public:
    void* db;
    int thread_id;
    Benchmark* benchmark;
    uint64_t total_time;
    uint64_t sum_opt_count;
    uint64_t sum_count[OPT_TYPE_COUNT];
    uint64_t sum_latency[OPT_TYPE_COUNT];
    uint64_t sum_success_count[OPT_TYPE_COUNT];
    uint64_t get_succeed;
    uint64_t put_succeed;
    uint64_t scan_succeed;
    uint64_t update_succeed;
    size_t bytes;
};

#define STORE_EACH_LATENCY
#if (defined STORE_EACH_LATENCY)
static std::vector<uint64_t> small_latency[32][OPT_TYPE_COUNT];
static std::vector<uint64_t> large_latency[32][OPT_TYPE_COUNT];
#endif

static void result_output(const char* name, std::vector<uint64_t>& data)
{
    std::ofstream fout(name);
    if (fout.is_open()) {
        for (int i = 0; i < data.size(); i++) {
            fout << data[i] << std::endl;
        }
        fout.close();
        LOG(INFO) << "|- Detailed results have been output to the file : " << name;
    }
}

static void* thread_task(void* thread_args)
{
    thread_param_t* param = (struct thread_param_t*)thread_args;
    int thread_id = param->thread_id;

    Benchmark* benchmark = param->benchmark;
    assert(benchmark != nullptr);
    benchmark->Init();

    Status status;
    DB* db = (DB*)param->db;

#if (defined THREAD_BIND_CPU)
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(numa_0[thread_id], &mask);
    if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0) {
        printf("threadpool, set thread affinity failed.\n");
    }
#endif

    int result;
    char* key;
    char* value;
    size_t key_length;
    size_t value_length;
    uint64_t scan_count;

    Timer single_request_timer, total_timer;
    uint64_t latency = 0;
    total_timer.Start();

    std::string read_value;
    Slice sk, sv;

    while (true) {
        int test_type = benchmark->GenerateNewKVPair(thread_id, &key, key_length, &value, value_length);
        param->bytes += value_length;
        if (test_type == -1) {
            break;
        }
#if (defined STORE_EACH_LATENCY)
        single_request_timer.Start();
#endif
        if (test_type == OPT_PUT) {
            sk = Slice((char*)key, key_length);
            sv = Slice((char*)value, value_length);
            status = db->Put(WriteOptions(), sk, sv);
            if (status.ok()) {
                param->put_succeed++;
            }
        } else if (test_type == OPT_UPDATE) {
            sk = Slice((char*)key, key_length);
            sv = Slice((char*)value, value_length);
            status = db->Put(WriteOptions(), sk, sv);
            if (status.ok()) {
                param->update_succeed++;
            }
        } else if (test_type == OPT_GET) {
            sk = Slice((char*)key, key_length);
            status = db->Get(ReadOptions(), sk, &read_value);
            if (status.ok()) {
                param->get_succeed++;
            }
        } else if (test_type == OPT_DELETE) {
        } else if (test_type == OPT_RMW) {
            sk = Slice((char*)key, key_length);
            sv = Slice((char*)value, value_length);
            status = db->Get(ReadOptions(), sk, &read_value);
            if (status.ok()) {
                param->get_succeed++;
            }
            status = db->Put(WriteOptions(), sk, sv);
            if (status.ok()) {
                param->update_succeed++;
            }
        } else if (test_type == OPT_SCAN) {
            sk = Slice((char*)key, key_length);
            std::vector<std::string> vec_values;
            Iterator* it = db->NewIterator(ReadOptions());
            scan_count = 0;
            std::string scan_values;
            for (it->Seek(sk); it->Valid(); it->Next()) {
                vec_values.push_back(it->value().ToString());
                scan_count++;
                if (scan_count == benchmark->GetScanRange()) {
                    break;
                }
            }
            param->scan_succeed += scan_count;
            delete it;
        }

#if (defined STORE_EACH_LATENCY)
        single_request_timer.Stop();
        latency = single_request_timer.Get();
        param->sum_latency[test_type] += latency;
        if (value_length < 4096) {
            small_latency[thread_id][test_type].push_back(latency);
        } else {
            large_latency[thread_id][test_type].push_back(latency);
        }
#endif
        param->sum_count[test_type]++;
        param->sum_opt_count++;
    }
    total_timer.Stop();
    param->total_time = total_timer.Get();
    return NULL;
}

Workload::Workload(Benchmark* benchmark, void* db, int num_thread)
    : benchmark(benchmark)
    , db(db)
    , num_thread(num_thread)
{
}

static char workload_name[YCSB_WORKLOAD_TYPE][128] = { "YCSB_A", "YCSB_B", "YCSB_C", "YCSB_D", "YCSB_E", "YCSB_F", "YCSB_LOAD", "YCSB_SEQ" };

void Workload::Run()
{
    pthread_t thread_id[32];
    thread_param_t thread_params[32];

    for (int i = 0; i < num_thread; i++) {
        memset(&thread_params[i], 0, sizeof(thread_param_t));
        thread_params[i].thread_id = i;
        thread_params[i].benchmark = benchmark;
        thread_params[i].db = db;
        thread_params[i].bytes = 0;
        pthread_create(thread_id + i, NULL, thread_task, (void*)&thread_params[i]);
    }

    for (int i = 0; i < num_thread; i++) {
        pthread_join(thread_id[i], NULL);
    }

    uint64_t total_iops = 0;
    uint64_t avg_latency = 0;
    uint64_t get_succeed = 0;
    uint64_t put_succeed = 0;
    uint64_t scan_succeed = 0;
    uint64_t update_succeed = 0;
    size_t sum_bw = 0;

    for (int i = 0; i < num_thread; i++) {
        uint64_t sum_opt = 0;
        uint64_t sum_latency_ns = 0;
        uint64_t avg_latency_ns;
        for (int j = 0; j < OPT_TYPE_COUNT; j++) {
            sum_opt += thread_params[i].sum_count[j];
            sum_latency_ns += thread_params[i].sum_latency[j];
        }
        // double sum_latency_s = 1.0 * sum_latency_ns / (1000 * 1000 * 1000);
        // avg_latency_ns = sum_latency_ns / sum_opt;
        // uint64_t iops_1 = 1000000000.0 / avg_latency_ns;
        // LOG(INFO) << "|- [Each][Count:" << sum_opt << "][Time:" << sum_latency_s << "seconds][IOPS:" << iops_1 << "][Latency:" << avg_latency_ns << "ns]";
        double total_time = 1.0 * thread_params[i].total_time / (1000 * 1000 * 1000);
        avg_latency_ns = 1.0 * thread_params[i].total_time / sum_opt;
        uint64_t iops_2 = 1000000000.0 / avg_latency_ns;
        double bw = thread_params[i].bytes / (1024 * 1024 * total_time);
        sum_bw += bw;
        LOG(INFO) << "|- [Total][Count:" << sum_opt << "][Time:" << total_time << "seconds][IOPS:" << iops_2 << "][Latency:" << avg_latency_ns << "ns]";
        total_iops += iops_2;
        avg_latency += avg_latency_ns;
        put_succeed += thread_params[i].put_succeed;
        update_succeed += thread_params[i].update_succeed;
        get_succeed += thread_params[i].get_succeed;
        scan_succeed += thread_params[i].scan_succeed;
    }

    LOG(INFO) << "|- [IOPS:" << total_iops << "][Latency:" << avg_latency / num_thread << "ns]";
    LOG(INFO) << "|- [BW:" << sum_bw << "MB/s]";

#if (defined STORE_EACH_LATENCY)
    mkdir("detail_latency", 0777);
    for (int i = 0; i < num_thread; i++) {
        for (int j = 0; j < OPT_TYPE_COUNT; j++) {
            if (small_latency[i][j].size() > 0) {
                char name[128];
                snprintf(name, sizeof(name), "detail_latency/%s_%d_%d.small", workload_name[benchmark->GetType() >> 1], i, j);
                result_output(name, small_latency[i][j]);
                small_latency[i][j].clear();
            }
            if (large_latency[i][j].size() > 0) {
                char name[128];
                snprintf(name, sizeof(name), "detail_latency/%s_%d_%d.large", workload_name[benchmark->GetType() >> 1], i, j);
                result_output(name, large_latency[i][j]);
                large_latency[i][j].clear();
            }
        }
    }
#endif
    LOG(INFO) << "|- [OK_PUT:" << put_succeed << "][OK_UPDATE:" << update_succeed << "][OK_GET:" << get_succeed << "]";
    LOG(INFO) << "|-------------------------------------------";
}
