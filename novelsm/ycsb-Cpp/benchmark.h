#ifndef INCLUDE_BENCHMARK_H_
#define INCLUDE_BENCHMARK_H_

#include "workload_leveldb.h"
#include "workload_ycsb.h"
#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define MAX_TEST_THREAD (16)
#define BENCH_YCSB (1)
#define BENCH_LEVELDB (2)

#define OPT_KEY_LENGTH (8)
#define OPT_MAX_VALUE_LENGTH (4096)

#define OPT_TYPE_COUNT (6)
#define OPT_PUT (0)
#define OPT_UPDATE (1)
#define OPT_GET (2)
#define OPT_DELETE (3)
#define OPT_SCAN (4)
#define OPT_RMW (5)

#define YCSB_ZIPFAN (1)
#define YCSB_WORKLOAD_TYPE (9)
// YCSB-A: 50% updates, 50% reads
#define YCSB_A (0 << 1)
// YCSB-B: 5% updates, 95% reads
#define YCSB_B (1 << 1)
// YCSB-C: 100% reads
#define YCSB_C (2 << 1)
// YCSB-E: 5% updates, 95% scans (50 items)
#define YCSB_D (3 << 1)
// YCSB-D: 5% updates, 95% lastest-reads
#define YCSB_E (4 << 1)
// YCSB-F: 50% read-modify-write, 50% reads
#define YCSB_F (5 << 1)
// YCSB-Load-A: 100% writes
#define YCSB_LOAD (6 << 1)
// SEQ load
#define YCSB_SEQ_LOAD (7 << 1)
// 100% write
#define YCSB_ONLY_WRITE (8 << 1)

class Benchmark {
public:
    virtual bool Init() = 0;
    virtual int GenerateNewKVPair(int thread_id, char** key, size_t& key_length, char** value, size_t& value_length) = 0;
    virtual void Print() = 0;
    virtual int GetScanRange() = 0;
    virtual int GetType() = 0;
    virtual uint64_t GetKeyUpper() = 0;
};

class YCSB_Benchmark : public Benchmark {
public:
    YCSB_Benchmark(int type, int num_thread, size_t dbsize, size_t workload_size, int kv_type, size_t kv_length[], double kv_proportion[], int scan_range)
        : type(type & (~1))
        , num_item(0)
        , max_key(0)
        , num_thread(num_thread)
        , kv_type(kv_type)
        , scan_range(scan_range)
    {
        printf(">>Creating a new ycsb benchmark! (%d, %d)\n", this->type, type);
        printf("  [Distribute");
        for (int i = 0; i < kv_type; i++) {
            this->kv_length[i] = kv_length[i];
            uint64_t num_kv = (uint64_t)(1.0 * dbsize * kv_proportion[i] / kv_length[i]);
            max_key += num_kv;
            kv_distribute[i] = max_key;
            if (i > 0) {
                printf("|%zu-(%llu,%llu]", kv_length[i], kv_distribute[i - 1], kv_distribute[i]);
            } else {
                printf("|%zi-[0,%llu)", kv_length[i], kv_distribute[i]);
            }
            num_kv = (uint64_t)(1.0 * workload_size * kv_proportion[i] / kv_length[i]);
            num_item += num_kv;
        }
        printf("]\n");
        printf("  [DB:%llu][MAX:%llu][LOAD:%llu][NUM:%llu]\n", dbsize / (1024 * 1024), max_key, workload_size / (1024 * 1024), num_item);
        for (int i = 0; i < num_thread; i++) {
            printf("  [Thread_%d]", i);
            thread_size[i] = workload_size / num_thread;
            printf("[Size:%zuMB]", thread_size[i] / (1024 * 1024));
            thread_items[i] = num_item / num_thread;
            printf("[NUM:%zu]", thread_items[i]);
            thread_sequential_id[i] = thread_items[i] * i;
            printf("[SEQ:%llu]\n", thread_sequential_id[i]);
        }
        init_zipf_generator(0, max_key);
        zipfian = (type & YCSB_ZIPFAN) ? 1 : 0;
        memset(opt_sum, 0, sizeof(opt_sum));
        memset(opt_count, 0, sizeof(opt_count));
        memset(opt_kv_length, 0, sizeof(opt_kv_length));
        printf(">>Create ycsb benchmark finished! (max_key:%llu) (zipfan:%d)\n", max_key, zipfian);
    }

    ~YCSB_Benchmark()
    {
    }

    bool Init()
    {
        init_seed();
        return true;
    }

    int GetType()
    {
        return type;
    }

    uint64_t GetKeyUpper()
    {
        return max_key;
    }

    void Print()
    {
        printf(">>[Benchmark(YCSB)][num_item:%llu][max_key:%llu]\n", num_item, max_key);
        for (int i = 0; i < num_thread; i++) {
            printf("  [Thread_%d][seq_id:%llu|num_item:%llu]\n", i, thread_sequential_id[i], thread_items[i]);
            printf("  [Thread_%d][PUT:%llu|UPDATE:%llu|GET:%llu|DELETE:%llu|SCAN:%llu|RANGE:%d|RMW:%llu]\n",
                i, opt_count[i][OPT_PUT], opt_count[i][OPT_UPDATE], opt_count[i][OPT_GET],
                opt_count[i][OPT_DELETE], opt_count[i][OPT_SCAN], scan_range, opt_count[i][OPT_RMW]);
            printf("  [Thread_%d][KV", i);
            for (int j = 0; j < kv_type; j++) {
                printf("|%zu-%llu(%.2f%%)", kv_length[j], opt_kv_length[i][j], 100.0 * opt_kv_length[i][j] / thread_items[i]);
            }
            printf("]\n");
        }
    }

    int GetScanRange()
    {
        return scan_range;
    }

public:
    int GenerateNewKVPair(int thread_id, char** key, size_t& key_length, char** value, size_t& value_length)
    {
        int opt_type;
        opt_sum[thread_id]++;
        key_length = OPT_KEY_LENGTH;
        if (opt_sum[thread_id] > thread_items[thread_id]) {
            return -1;
        }
        if (type == YCSB_SEQ_LOAD) {
            value_length = generate_kv_pair(thread_id, ++thread_sequential_id[thread_id], max_key, key, value);
        } else {
            if (zipfian) {
                value_length = generate_kv_pair(thread_id, zipf_next(), max_key, key, value);
            } else {
                value_length = generate_kv_pair(thread_id, uniform_next(), max_key, key, value);
            }
        }
        opt_type = random_get_put(type);
        if (opt_type < 0) {
            return -1;
        } else {
            opt_count[thread_id][opt_type]++;
        }
        return opt_type;
    }

private:
    size_t generate_kv_pair(int thread_id, uint64_t uid, uint64_t max_uid, char** key, char** value)
    {
        size_t value_length = 0;
        for (int i = 0; i < kv_type; i++) {
            if (uid <= kv_distribute[i]) {
                value_length = kv_length[i];
                opt_kv_length[thread_id][i]++;
                break;
            }
        }
        assert(value_length != 0);
        *key = new char[OPT_KEY_LENGTH];
        *value = new char[OPT_MAX_VALUE_LENGTH];
        *((uint64_t*)(*key)) = uid;
        *((uint64_t*)(*value)) = uid;
        return value_length;
    }

    int random_get_put(int test)
    {
        long random = uniform_next() % 100;
        switch (test) {
        case YCSB_SEQ_LOAD:
            return OPT_PUT;
        case YCSB_LOAD:
            return OPT_PUT;
        case YCSB_ONLY_WRITE:
            return OPT_UPDATE;
        case YCSB_A:
            return (random >= 50) ? OPT_UPDATE : OPT_GET;
        case YCSB_B:
            return (random >= 95) ? OPT_UPDATE : OPT_GET;
        case YCSB_C:
            return OPT_GET;
        case YCSB_D:
            return (random >= 95) ? OPT_UPDATE : OPT_GET;
        case YCSB_E:
            return (random >= 95) ? OPT_SCAN : OPT_GET;
        case YCSB_F:
            return (random >= 50) ? OPT_RMW : OPT_GET;
        default:
            return -1;
        }
    }

public:
    int type;
    int zipfian;
    int scan_range;
    int num_thread;
    uint64_t max_key;
    uint64_t num_items;

public:
    int kv_type;
    size_t kv_length[32];
    uint64_t kv_distribute[32];

private:
    uint64_t num_item;
    uint64_t thread_size[MAX_TEST_THREAD];
    uint64_t thread_items[MAX_TEST_THREAD];
    uint64_t thread_length[MAX_TEST_THREAD];
    uint64_t thread_sequential_id[MAX_TEST_THREAD];

private:
    uint64_t opt_sum[MAX_TEST_THREAD];
    uint64_t opt_count[MAX_TEST_THREAD][OPT_TYPE_COUNT];
    uint64_t opt_kv_length[MAX_TEST_THREAD][32];
};

/*
class Mirco_Benchmark : public Benchmark {
public:
    Mirco_Benchmark(int num_thread)
        : num_thread(num_thread)
        , seq(1)
    {
        for (int i = 0; i < MAX_TEST_THREAD; i++) {
            key_[i] = new char[OPT_KEY_LENGTH];
            val_[i] = new char[OPT_MAX_VALUE_LENGTH];
        }
    }

    Mirco_Benchmark(int num_thread, int seed_[OPT_TYPE_COUNT], uint64_t num_opt[OPT_TYPE_COUNT], uint64_t scan_range)
        : pos(0)
        , num_thread(num_thread)
        , scan_range(scan_range)
    {
        for (int i = 0; i < MAX_TEST_THREAD; i++) {
            key_[i] = new char[OPT_KEY_LENGTH];
            val_[i] = new char[OPT_MAX_VALUE_LENGTH];
        }
        for (int i = 0; i < OPT_TYPE_COUNT; i++) {
            each_thread_opt[i] = num_opt[i] / num_thread; // average for each thread.
            seed[i] = seed_[i];
        }
        for (int i = 0; i < MAX_TEST_THREAD; i++) {
            for (int j = 0; j < OPT_TYPE_COUNT; j++) {
                random[i][j] = generate_random(0, seed[j] + i);
                done_opt_count[i][j] = 0;
            }
        }
    }

    ~Mirco_Benchmark()
    {
    }

    void init_thread()
    {
    }

    void print()
    {
    }

    int get_type()
    {
        return 0;
    }

public:
    int get_kv_item(int thread_id, uint8_t** key, size_t& key_length, uint8_t** value, size_t& value_length)
    {
        int mark = pos;
        while (true) {
            if (done_opt_count[thread_id][pos] < each_thread_opt[pos]) {
                done_opt_count[thread_id][pos]++;
                generate_kv_pair(thread_id, random[thread_id][pos]);
                *key = (uint8_t*)key_[thread_id];
                *value = (uint8_t*)val_[thread_id];
                key_length = OPT_KEY_LENGTH;
                value_length = OPT_VALUE_LENGTH;
                break;
            }
            pos = (pos + 1) % OPT_TYPE_COUNT;
            if (pos == mark) {
                return -1;
            }
        }
        return pos;
    }

private:
    Random* generate_random(int base, uint32_t seed)
    {
        Random* rd = new Random(seed);
        for (int i = 0; i < base; i++) {
            rd->Next();
        }
        return rd;
    }

    void generate_kv_pair(int thread_id, Random* rd)
    {
        uint64_t seed = rd->Next();
        *((uint64_t*)key_[thread_id]) = seed;
        *((uint64_t*)val_[thread_id]) = seed;
    }

private:
    int pos;
    int seq;
    int num_thread;
    uint64_t scan_range;
    int seed[OPT_TYPE_COUNT]; // seed for each operation
    uint64_t each_thread_opt[OPT_TYPE_COUNT]; // sum operation count
    uint64_t done_opt_count[OPT_TYPE_COUNT][OPT_TYPE_COUNT]; // has done for each operation
    Random* random[MAX_TEST_THREAD][OPT_TYPE_COUNT];

private:
    char* key_[MAX_TEST_THREAD];
    char* val_[MAX_TEST_THREAD];
};
*/

#endif