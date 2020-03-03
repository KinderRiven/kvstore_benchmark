#include "benchmark.h"
#include "timer.h"

#include "easylogging/easylogging++.h"
#include "run_workload.h"

#include <algorithm>
#include <assert.h>
#include <pthread.h>
#include <vector>

#define SCAN_RANGE 100

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

// #define STORE_EACH_LATENCY
#if (defined STORE_EACH_LATENCY)
static std::vector<uint64_t> vec_opt_latency[32][OPT_TYPE_COUNT];
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
    benchmark->init_thread();

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
    uint8_t* key;
    uint8_t* value;
    size_t key_length;
    size_t value_length;
    uint64_t scan_count;

    Timer little_timer, total_timer;
    uint64_t latency = 0;
    total_timer.Start();

    while (true) {
        int test_type = benchmark->get_kv_item(thread_id, &key, key_length, &value, value_length);
        param->bytes += value_length;
        if (test_type == -1) {
            Slice sk = Slice((char*)key, key_length);
            Slice sv = Slice((char*)value, value_length);
            db->Put(WriteOptions(), sk, sv);
            break;
        }
#if (defined STORE_EACH_LATENCY)
        little_timer.Start();
#endif
        if (test_type == OPT_PUT) {
        } else if (test_type == OPT_UPDATE) {
        } else if (test_type == OPT_GET) {
        } else if (test_type == OPT_DELETE) {
        } else if (test_type == OPT_SCAN) {
        }

#if (defined STORE_EACH_LATENCY)
        little_timer.Stop();
        latency = little_timer.Get();
        param->sum_latency[test_type] += latency;
        vec_opt_latency[thread_id][test_type].push_back(latency);
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
            if (vec_opt_latency[i][j].size() > 0) {
                char name[128];
                snprintf(name, sizeof(name), "detail_latency/%d_%d.lightkv", i, j);
                result_output(name, vec_opt_latency[i][j]);
                vec_opt_latency[i][j].clear();
            }
        }
    }
#endif
    LOG(INFO) << "|- [OK_PUT:" << put_succeed << "][OK_UPDATE:" << update_succeed << "][OK_GET:" << get_succeed << "]";
    LOG(INFO) << "|-------------------------------------------";
}
