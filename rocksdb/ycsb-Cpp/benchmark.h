#ifndef INCLUDE_BENCHMARK_H_
#define INCLUDE_BENCHMARK_H_

#include "workload_leveldb.h"
#include "workload_ycsb.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define NUM_BENCH_THREAD (16)
#define BENCH_YCSB (1)
#define BENCH_LEVELDB (2)

#define OPT_KEY_LENGTH (8)
#define OPT_VALUE_LENGTH (1024)
#define OPT_MAX_VALUE_LENGTH (1024)

#define OPT_TYPE_COUNT (5)
#define OPT_PUT (0)
#define OPT_UPDATE (1)
#define OPT_GET (2)
#define OPT_DELETE (3)
#define OPT_SCAN (4)

#define YCSB_ZIPFAN (1)
// YCSB-A: 50% updates, 50% reads
#define YCSB_A (0 << 1)
// YCSB-B: 5% updates, 95% reads
#define YCSB_B (1 << 1)
// YCSB-C: 100% reads
#define YCSB_C (2 << 1)
// YCSB-E: 5% updates, 95% scans (50 items)
#define YCSB_E (3 << 1)
// YCSB-D: 5% updates, 95% lastest-reads
#define YCSB_D (4 << 1)
// YCSB-F: 50% read-modify-write, 50% reads
#define YCSB_F (5 << 1)
// YCSB-Load-A: 100% writes
#define YCSB_LOAD (6 << 1)
// SEQ load
#define YCSB_SEQ_LOAD (7 << 1)

class Benchmark {
public:
    virtual int get_kv_item(int thread_id, uint8_t** key, size_t& key_length, uint8_t** value, size_t& value_length) = 0;
    virtual void init_thread() = 0;
    virtual void print() = 0;
};

class YCSB_Benchmark : public Benchmark {
public:
    YCSB_Benchmark(int type, int num_thread, uint64_t num_item, uint64_t num_opt)
        : type(type & (~1))
        , num_thread(num_thread)
        , num_item(num_item)
        , seq_id(1)
    {
        init_zipf_generator(0, num_item - 1);
        each_thread_opt = num_opt / num_thread;
        zipfian = (type & YCSB_ZIPFAN) ? 1 : 0;
        memset(opt_sum, 0, sizeof(opt_sum));
        memset(opt_count, 0, sizeof(opt_count));

        for (int i = 0; i < NUM_BENCH_THREAD; i++) {
            key_[i] = new char[OPT_KEY_LENGTH];
            val_[i] = new char[OPT_MAX_VALUE_LENGTH];
        }
    }

    ~YCSB_Benchmark()
    {
    }

    void init_thread()
    {
        init_seed();
    }

    void print()
    {
        printf(">>[YCSB-Benchmark]\n");
        for (int i = 0; i < num_thread; i++) {
            printf("  [%d][PUT:%llu][UPDATE:%llu][GET:%llu][DELETE:%llu][SCAN:%llu]\n",
                i, opt_count[i][OPT_PUT], opt_count[i][OPT_UPDATE], opt_count[i][OPT_GET],
                opt_count[i][OPT_DELETE], opt_count[i][OPT_SCAN]);
        }
    }

public:
    int get_kv_item(int thread_id, uint8_t** key, size_t& key_length, uint8_t** value, size_t& value_length)
    {
        int opt_type;
        opt_sum[thread_id]++;
        key_length = OPT_KEY_LENGTH;
        if (opt_sum[thread_id] > each_thread_opt) {
            return -1;
        }
        if (type == YCSB_SEQ_LOAD) {
            value_length = generate_kv_pair(thread_id, seq_id++, num_item);
            opt_count[thread_id][OPT_PUT]++;
            opt_type = OPT_PUT;
        } else if (type == YCSB_LOAD) {
            if (zipfian) { // zipf skew
                value_length = generate_kv_pair(thread_id, zipf_next(), num_item);
            } else { // uniform skew
                value_length = generate_kv_pair(thread_id, uniform_next(), num_item);
            }
            opt_count[thread_id][OPT_PUT]++;
            opt_type = OPT_PUT;
        } else if (type == YCSB_A || type == YCSB_B || type == YCSB_C) { // ycsb-a, ycsb-b, ycsb-c
            if (zipfian) { // zipf skew
                value_length = generate_kv_pair(thread_id, zipf_next(), num_item);
            } else { // uniform skew
                value_length = generate_kv_pair(thread_id, uniform_next(), num_item);
            }
            if (random_get_put(type)) {
                opt_count[thread_id][OPT_UPDATE]++; // update
                opt_type = OPT_UPDATE;
            } else {
                opt_count[thread_id][OPT_GET]++; // scan
                opt_type = OPT_GET;
            }
        } else if (type == YCSB_E) { // ycsb-e
            if (zipfian) { // zipf skew
                value_length = generate_kv_pair(thread_id, zipf_next(), num_item);
            } else { // uniform skew
                value_length = generate_kv_pair(thread_id, uniform_next(), num_item);
            }
            if (random_get_put(type)) {
                opt_count[thread_id][OPT_UPDATE]++; // update
                opt_type = OPT_UPDATE;
            } else {
                opt_count[thread_id][OPT_SCAN]++; // get
                opt_type = OPT_SCAN;
            }
        } else { // ycsb-d, ycsb-f
            opt_type = -1;
        }
        *key = (uint8_t*)key_[thread_id];
        *value = (uint8_t*)val_[thread_id];
        return opt_type;
    }

private:
    size_t generate_kv_pair(int thread_id, uint64_t uid, uint64_t max_uid)
    {
        size_t item_size;
        if (uid * 100LU / max_uid < 50) {
            // item_size = 256;
            item_size = OPT_VALUE_LENGTH;
        } else if (uid * 100LU / max_uid < 82) {
            item_size = OPT_MAX_VALUE_LENGTH;
        } else if (uid * 100LU / max_uid < 98) {
            item_size = OPT_MAX_VALUE_LENGTH;
        } else {
            item_size = OPT_MAX_VALUE_LENGTH;
        }

        *((uint64_t*)key_[thread_id]) = uid;
        *((uint64_t*)val_[thread_id]) = uid;
        return item_size;
    }

    int random_get_put(int test)
    {
        long random = uniform_next() % 100;
        switch (test) {
        case YCSB_LOAD:
            return 1;
        case YCSB_A: // A
            return random >= 50;
        case YCSB_B: // B
            return random >= 95;
        case YCSB_C: // C
            return 0;
        case YCSB_E: // E
            return random >= 95;
        default:
            return 0;
        }
    }

private:
    uint64_t seq_id;

private:
    int type;
    int each_thread_opt;
    int num_thread;
    int zipfian;
    uint64_t num_item;

private:
    char* key_[NUM_BENCH_THREAD];
    char* val_[NUM_BENCH_THREAD];

private:
    uint64_t opt_sum[NUM_BENCH_THREAD];
    uint64_t opt_count[NUM_BENCH_THREAD][OPT_TYPE_COUNT];
};

class Mirco_Benchmark : public Benchmark {
public:
    Mirco_Benchmark(int num_thread)
        : num_thread(num_thread)
        , seq(1)
    {
        for (int i = 0; i < NUM_BENCH_THREAD; i++) {
            key_[i] = new char[OPT_KEY_LENGTH];
            val_[i] = new char[OPT_MAX_VALUE_LENGTH];
        }
    }

    Mirco_Benchmark(int num_thread, int seed_[OPT_TYPE_COUNT], uint64_t num_opt[OPT_TYPE_COUNT], uint64_t scan_range)
        : pos(0)
        , num_thread(num_thread)
        , scan_range(scan_range)
    {
        for (int i = 0; i < NUM_BENCH_THREAD; i++) {
            key_[i] = new char[OPT_KEY_LENGTH];
            val_[i] = new char[OPT_MAX_VALUE_LENGTH];
        }
        for (int i = 0; i < OPT_TYPE_COUNT; i++) {
            each_thread_opt[i] = num_opt[i] / num_thread; // average for each thread.
            seed[i] = seed_[i];
        }
        for (int i = 0; i < NUM_BENCH_THREAD; i++) {
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
    Random* random[NUM_BENCH_THREAD][OPT_TYPE_COUNT];

private:
    char* key_[NUM_BENCH_THREAD];
    char* val_[NUM_BENCH_THREAD];
};

#endif