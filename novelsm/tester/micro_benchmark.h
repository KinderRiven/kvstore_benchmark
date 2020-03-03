#ifndef INCLUDE_MICRO_BENCHMARK_H_
#define INCLUDE_MICRO_BENCHMARK_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "config.h"
#include "leveldb/db.h"

using namespace leveldb;

#define MAX_TEST_THREAD (32)

struct benchmark_param_t
{
public:
  bool seq;
  int num_thread;
  size_t key_length;
  size_t value_length;
  uint64_t num_get_opt;
  uint64_t num_put_opt;
  uint64_t num_delete_opt;
  uint64_t num_scan_opt;
  uint64_t put_seed[MAX_TEST_THREAD];
  uint64_t get_seed[MAX_TEST_THREAD];
  uint64_t delete_seed[MAX_TEST_THREAD];
  uint64_t scan_seed[MAX_TEST_THREAD];
  uint64_t put_sequence_id[MAX_TEST_THREAD];
  uint64_t get_sequence_id[MAX_TEST_THREAD];
  uint64_t delete_sequence_id[MAX_TEST_THREAD];
  uint64_t scan_sequence_id[MAX_TEST_THREAD];
  uint64_t scan_range;

public:
  benchmark_param_t()
  {
    seq = false;
    num_thread = 1;
    key_length = 16;
    value_length = 1024;
    num_get_opt = num_put_opt = num_delete_opt = num_scan_opt = 0;

    memset(put_seed, 0, sizeof(put_seed));
    memset(get_seed, 0, sizeof(get_seed));
    memset(delete_seed, 0, sizeof(delete_seed));
    memset(scan_seed, 0, sizeof(scan_seed));

    memset(put_sequence_id, 0, sizeof(put_sequence_id));
    memset(get_sequence_id, 0, sizeof(get_sequence_id));
    memset(delete_sequence_id, 0, sizeof(delete_sequence_id));
    memset(scan_sequence_id, 0, sizeof(scan_sequence_id));

    scan_range = 1000;
  }
};

class MicroBenchmark
{
public:
  MicroBenchmark(struct benchmark_param_t *param, DB *db);
  void Run();
  void Print();

private:
  struct benchmark_param_t *test_param;
  DB *db;
};

#endif