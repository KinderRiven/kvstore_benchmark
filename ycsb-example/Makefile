all: detail
	g++ -std=c++11 run_workload.cc main.cc easylogging/easylogging++.cc ycsb-local/workload_ycsb.c -o tester -I. -Iycsb-local -Ileveldb_bench -lpthread

pmdk: detail
	g++ -std=c++11 run_workload.cc main.cc easylogging/easylogging++.cc ycsb-local/workload_ycsb.c -o tester -I. -Iycsb-local -Ileveldb_bench -laio -lpthread -lpmem

detail: detail.cc
	g++ -std=c++11 detail.cc -o detail

export_lib:
	export LD_LIBRARY_PATH=../build:../lib/tbb
