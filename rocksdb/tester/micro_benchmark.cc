#include "micro_benchmark.h"
#include "config.h"
#include "easylogging/easylogging++.h"
#include "random.h"
#include "timer.h"

#include <algorithm>
#include <pthread.h>
#include <string>
#include <vector>

struct thread_result_t {
    uint64_t iops;
    uint64_t latency;
    uint64_t throughput;
};

struct thread_test_t {
    bool seq;
    int thread_id;
    size_t key_length;
    size_t value_length;
    uint64_t num_get_opt;
    uint64_t get_seed;
    uint64_t get_sequence_id;
    uint64_t num_put_opt;
    uint64_t put_seed;
    uint64_t put_sequence_id;
    uint64_t num_delete_opt;
    uint64_t delete_seed;
    uint64_t delete_sequence_id;
    uint64_t num_scan_opt;
    uint64_t scan_range;
    uint64_t scan_seed;
    uint64_t scan_sequence_id;
};

struct thread_param_t {
    DB* db;
    thread_test_t test;
    thread_result_t result;
};

#define TEST_TYPE_COUNT (4)
#define TEST_PUT (0)
#define TEST_GET (1)
#define TEST_DELETE (2)
#define TEST_SCAN (3)

// #define GET_FILTER(seed) ((seed % 3 == 0 || seed % 5 == 0 || seed % 7 == 0))
#define GET_FILTER(seed) ((seed % 1 == 0))
#define STORE_EACH_LATENCY

#if (defined STORE_EACH_LATENCY)
static std::vector<uint64_t> vec_opt_latency[32][TEST_TYPE_COUNT];
#endif

static bool generate_kv_pair(bool seq, uint64_t& sd, Random* rd, uint8_t* key, uint8_t* value)
{
    uint64_t seed = (seq == 1) ? sd : sd + rd->Next();
    sd++;
    memset(key, 0, MAX_KEY_LENGTH + 10);
    snprintf((char*)key, MAX_KEY_LENGTH + 10, "%016llu", seed);
    memset(value, 0, MAX_KEY_LENGTH + 10);
    snprintf((char*)value, MAX_KEY_LENGTH + 10, "%016llu", seed);
    return GET_FILTER(seed) == true ? true : false;
}

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

    bool res;
    bool seq = param->test.seq;
    int thread_id = param->test.thread_id;
    size_t key_length = param->test.key_length;
    size_t value_length = param->test.value_length;

    DB* db = param->db;

#if (defined THREAD_BIND_CPU)
    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(thread_id, &mask);
    if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0) {
        printf("threadpool, set thread affinity failed.\n");
    }
#endif

    uint64_t num_put_opt = param->test.num_put_opt;
    uint64_t num_get_opt = param->test.num_get_opt;
    uint64_t num_delete_opt = param->test.num_delete_opt;
    uint64_t num_scan_opt = param->test.num_scan_opt;

    uint64_t put_seed = param->test.put_seed;
    uint64_t get_seed = param->test.get_seed;
    uint64_t delete_seed = param->test.delete_seed;
    uint64_t scan_seed = param->test.scan_seed;
    uint64_t scan_range = param->test.scan_range;

    Random* put_random = new Random(put_seed);
    Random* get_random = new Random(get_seed);
    Random* delete_random = new Random(delete_seed);
    Random* scan_random = new Random(scan_seed);

    uint64_t put_sequence_id = param->test.put_sequence_id;
    uint64_t get_sequence_id = param->test.get_sequence_id;
    uint64_t delete_sequence_id = param->test.delete_sequence_id;
    uint64_t scan_sequence_id = param->test.scan_sequence_id;

    uint64_t match_search = 0;
    uint64_t match_delete = 0;
    uint64_t match_insert = 0;
    uint64_t match_scan = 0;
    uint64_t correct_search = 0;

    uint8_t key[MAX_KEY_LENGTH + 10];
    uint8_t value[MAX_VALUE_LENGTH + 10];

    uint64_t num_sum_opt = num_put_opt + num_get_opt + num_delete_opt + num_scan_opt;
    uint64_t get_count = 0;
    uint64_t put_count = 0;
    uint64_t delete_count = 0;
    uint64_t scan_count = 0;
    uint64_t scan_cover_count = 0;
    uint64_t sum_opt_count = 0;

    Timer timer;
    uint64_t opt_latency = 0;
    uint64_t sum_count[TEST_TYPE_COUNT] = { 0 };
    uint64_t sum_latency[TEST_TYPE_COUNT] = { 0 };

    if (num_sum_opt <= 0) {
        LOG(INFO) << "|- Thread " << thread_id << " does nothing.";
        return NULL;
    }

    LOG(INFO) << "|- [TEST:" << thread_id << "][SEED:" << put_seed << "/" << get_seed << "/" << delete_seed << "][SEQ:" << put_sequence_id << "/" << get_sequence_id << "/" << delete_sequence_id << "]";
    LOG(INFO) << "|- [PUT:" << num_put_opt << "(" << 100.0 * num_put_opt / num_sum_opt << "%)][GET:num_get_opt"
              << "(" << 100.0 * num_get_opt / num_sum_opt << "%)][DELETE:" << num_delete_opt << "(" << 100.0 * num_delete_opt / num_sum_opt
              << "%)][SCAN:" << num_scan_opt << "(" << 100.0 * num_scan_opt / num_sum_opt << "%)]";

    while (true) {
        bool flag = false;
        int test_type = -1;

        if (put_count < num_put_opt) {
            flag = true;
            test_type = TEST_PUT;
            put_count++;

            res = generate_kv_pair(seq, put_sequence_id, put_random, key, value);
            Slice sk = Slice((char*)key, key_length);
            Slice sv = Slice((char*)value, value_length);

            timer.Start();
            Status status = db->Put(WriteOptions(), sk, sv);
            timer.Stop();

            opt_latency = timer.Get();
            sum_latency[TEST_PUT] += opt_latency;
            sum_count[TEST_PUT]++;

#if (defined STORE_EACH_LATENCY)
            vec_opt_latency[thread_id][TEST_PUT].push_back(opt_latency);
#endif

            if (status.ok()) {
                match_insert++;
            }
        }
        if (get_count < num_get_opt) {
            flag = true;
            test_type = TEST_GET;
            get_count++;
            res = generate_kv_pair(seq, get_sequence_id, get_random, key, value);

            if (res) {
                Slice sk = Slice((char*)key, key_length);
                std::string sv;
                timer.Start();
                Status status = db->Get(ReadOptions(), sk, &sv);
                timer.Stop();

                opt_latency = timer.Get();
                sum_latency[TEST_GET] += opt_latency;
                sum_count[TEST_GET]++;

#if (defined STORE_EACH_LATENCY)
                vec_opt_latency[thread_id][TEST_GET].push_back(opt_latency);
#endif

                if (status.ok()) {
                    match_search++;
                    if (memcmp(sv.data(), key, key_length) == 0) {
                        correct_search++;
                    }
                }
            }
        }

        if (delete_count < num_delete_opt) {
            flag = true;
            delete_count++;
        }

        if (scan_count < num_scan_opt) {
            flag = true;
            scan_count++;
            res = generate_kv_pair(seq, scan_sequence_id, scan_random, key, value);

            int scan_kv_count = 0;
            std::vector<std::string> vec_value;
            Slice sk = Slice((char*)key, key_length);

            timer.Start();
            Iterator* it = db->NewIterator(ReadOptions());

            for (it->Seek(sk); it->Valid(); it->Next()) {
                vec_value.push_back(it->value().ToString());
                scan_kv_count++;
                if (scan_kv_count == scan_range) {
                    break;
                }
            }

            timer.Stop();
            opt_latency = timer.Get();
            sum_latency[TEST_GET] += opt_latency;
            sum_count[TEST_SCAN]++;
            match_scan += vec_value.size();
            delete (it);
        }

        if (!flag) {
            break;
        }
    }

    uint64_t thread_opt_count = 0;
    uint64_t thread_sum_latency = 0;

    for (int i = 0; i < TEST_TYPE_COUNT; i++) {
        thread_opt_count += sum_count[i];
        thread_sum_latency += sum_latency[i];
    }

    double exe_time = 1.0 * thread_sum_latency / (1000 * 1000 * 1000);
    uint64_t thread_avg_latency = thread_sum_latency / thread_opt_count;
    uint64_t thread_iops = 1000000000.0 / thread_avg_latency;
    param->result.iops = thread_iops;
    param->result.latency = thread_avg_latency;

    LOG(INFO) << "|- [All:" << thread_opt_count << "][Time:" << exe_time << "seconds][IOPS:" << thread_iops << "][Latency:" << thread_avg_latency << "ns]";
    if (put_count > 0) {
        LOG(INFO) << "|- [PUT][Match:" << match_insert << "/" << put_count << "]";
    }
    if (get_count > 0) {
        LOG(INFO) << "|- [GET][Match:" << match_search << "/" << correct_search << "/" << get_count << "]";
    }
    if (delete_count > 0) {
        LOG(INFO) << "|- [DELETE][Match:" << match_delete << "/" << delete_count << "]";
    }
    if (scan_count > 0) {
        LOG(INFO) << "|- [SCAN][Match:" << match_scan << "/" << scan_count << "][AVG:" << match_scan / scan_count << "]";
    }
    return NULL;
}

MicroBenchmark::MicroBenchmark(struct benchmark_param_t* param, DB* db)
{
    this->test_param = param;
    this->db = db;
}

void MicroBenchmark::Run()
{
    pthread_t thread_id[32];
    struct thread_param_t thread_params[32];
    int num_thread = this->test_param->num_thread;
    uint64_t num_put_opt = this->test_param->num_put_opt;
    uint64_t num_get_opt = this->test_param->num_get_opt;
    uint64_t num_delete_opt = this->test_param->num_delete_opt;
    uint64_t num_scan_opt = this->test_param->num_scan_opt;

    LOG(INFO) << "|----------[MicroBenchmark::Run]------------";
    LOG(INFO) << "|- [PUT:" << num_put_opt << "][GET:" << num_get_opt << "][DELETE:" << num_delete_opt << "][SCAN" << num_scan_opt << "]";

    for (int i = 0; i < num_thread; i++) {
        thread_params[i].db = this->db;
        thread_params[i].test.seq = this->test_param->seq;
        thread_params[i].test.thread_id = i;
        thread_params[i].test.key_length = this->test_param->key_length;
        thread_params[i].test.value_length = this->test_param->value_length;
        thread_params[i].test.num_put_opt = num_put_opt / num_thread;
        thread_params[i].test.num_get_opt = num_get_opt / num_thread;
        thread_params[i].test.num_delete_opt = num_delete_opt / num_thread;
        thread_params[i].test.num_scan_opt = num_scan_opt / num_thread;
        thread_params[i].test.put_seed = this->test_param->put_seed[i];
        thread_params[i].test.get_seed = this->test_param->get_seed[i];
        thread_params[i].test.delete_seed = this->test_param->delete_seed[i];
        thread_params[i].test.scan_seed = this->test_param->scan_seed[i];
        thread_params[i].test.put_sequence_id = this->test_param->put_sequence_id[i];
        thread_params[i].test.get_sequence_id = this->test_param->get_sequence_id[i];
        thread_params[i].test.delete_sequence_id = this->test_param->delete_sequence_id[i];
        thread_params[i].test.scan_sequence_id = this->test_param->scan_sequence_id[i];
        thread_params[i].test.scan_range = this->test_param->scan_range;
        pthread_create(thread_id + i, NULL, thread_task, (void*)&thread_params[i]);
    }

    for (int i = 0; i < num_thread; i++) {
        pthread_join(thread_id[i], NULL);
    }

    uint64_t total_iops = 0;
    uint64_t avg_latency = 0;

    for (int i = 0; i < num_thread; i++) {
        total_iops += thread_params[i].result.iops;
        avg_latency += thread_params[i].result.latency;
    }

    LOG(INFO) << "|- [IOPS:" << total_iops << "][Latency:" << avg_latency / num_thread << "ns]";
#if (defined STORE_EACH_LATENCY)
    char dname[128];
    snprintf(dname, sizeof(dname), "%s_%zu", "rocksdb_detail", this->test_param->value_length);
    mkdir(dname, 0777);
    for (int i = 0; i < num_thread; i++) {
        for (int j = 0; j < TEST_TYPE_COUNT; j++) {
            if (vec_opt_latency[i][j].size() > 0) {
                char name[128];
                snprintf(name, sizeof(name), "%s/%d_%d.lightkv", dname, i, j);
                result_output(name, vec_opt_latency[i][j]);
                vec_opt_latency[i][j].clear();
            }
        }
    }
#endif
    LOG(INFO) << "|-------------------------------------------";
}
