EXEC_DIR=exec

all: detail
	g++ -std=c++11 tester/micro_benchmark.cc tester/main.cc ../lib/easylogging/easylogging++.cc -o $(EXEC_DIR)/test -Itester -Iinclude -I../lib -L../lib/novelsm -lleveldb -lpthread -lsnappy -lnuma

dir:
	mkdir $(EXEC_DIR)

detail: tester/detail.cc
	g++ -std=c++11 tester/detail.cc -o $(EXEC_DIR)/detail

export_lib:
	export LD_LIBRARY_PATH=../lib/leveldb
