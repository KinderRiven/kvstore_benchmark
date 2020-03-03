include ../../../Makefile.config
EXECUTABLE = mapkeeper_novelsm

all : thrift
	g++ -Wall -o $(EXECUTABLE) NoveLSMServer.cc -I $(THRIFT_DIR)/include/thrift -I $(THRIFT_DIR)/include \
        -I../include -L../../lib/novelsm -lleveldb -lpthread -lsnappy -lnuma \
        -lboost_thread -lboost_system -lboost_filesystem -lthrift -I ../../../thrift/gen-cpp -L $(THRIFT_DIR)/lib \
        -L ../../../thrift/gen-cpp -lmapkeeper \
        -Wl,-rpath,\$$ORIGIN/../../../thrift/gen-cpp \
        -Wl,-rpath,$(THRIFT_DIR)/lib \
        -std=c++11

thrift:                                                                                 
	make -C ../../../thrift
run:
	./$(EXECUTABLE) --sync

clean :
	-rm -rf $(THRIFT_SRC) $(EXECUTABLE) *.o

wipe:
	rm -rf data/*
