#ifndef INCLUDE_MICRO_BENCHMARK_H_
#define INCLUDE_MICRO_BENCHMARK_H_

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "benchmark.h"

class Workload {
public:
    Workload(Benchmark* benchmark, void* db, int num_thread);
    void Run();
    void Print();

private:
    void* db;
    int num_thread;
    Benchmark* benchmark;
};

#endif