all:
	g++ -std=c++11 run_workload.cc main.cc easylogging/easylogging++.cc ycsb-local/workload_ycsb.c -o tester -I. -I../include -Iycsb-local -Ileveldb_bench -L../../lib/novelsm -lleveldb -lpthread -lpthread 

detail: detail.cc
	g++ -std=c++11 detail.cc -o detail

export_lib:
	export LD_LIBRARY_PATH=../build:../lib/tbb
