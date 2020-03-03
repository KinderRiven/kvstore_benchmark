EXEC_DIR=exec

all: detail
	g++ -std=c++11 tester/micro_benchmark.cc tester/main.cc ../lib/easylogging/easylogging++.cc -o $(EXEC_DIR)/test -Itester -Iinclude -I../lib -L../lib/rocksdb -lrocksdb -ljemalloc -ldl -lpthread -lsnappy -lgflags -lz -lbz2 -llz4 -lzstd

dir:
	mkdir $(EXEC_DIR)

detail: tester/detail.cc
	g++ -std=c++11 tester/detail.cc -o $(EXEC_DIR)/detail

export_lib:
	export LD_LIBRARY_PATH=../lib/leveldb
	# yum install jemalloc-devel.x86_64
	# yum install gflags-devel.x86_64
	# yum install lz4-devel.x86_64
	# yum install bzip2-devel.x86_64
