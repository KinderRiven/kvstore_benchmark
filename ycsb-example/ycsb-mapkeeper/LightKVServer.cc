#include <boost/filesystem.hpp>
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <cstdio>
#include <dirent.h>
#include <errno.h>
#include <iostream>
#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <string>
#include <sys/time.h>
#include <sys/types.h>
#include <transport/TBufferTransports.h>
#include <transport/TServerSocket.h>
#include <vector>

#include "MapKeeper.h"
#include "lightkv.h"
#include "option.h"
#include "timer.h"

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;
using boost::shared_ptr;
using namespace boost::filesystem;
using namespace mapkeeper;
using std::string;

int output;
char db_path[128] = "dbtmp";
char nvm_path[128] = "/home/pmem0/pm";

// #define LATENCY_OUTPUT
#define YCSB_KEY_LENGTH (20)
#define YCSB_VALUE_LENGTH (1024)

class LightKVServer : virtual public MapKeeperIf {
private:
    void file_write(const char* name, std::vector<uint64_t>& data)
    {
        std::ofstream fout(name);
        if (fout.is_open()) {
            for (int i = 0; i < data.size(); i++) {
                fout << data[i] << std::endl;
            }
            fout.close();
        } else {
            printf("Can not't open write file (%s)\n", name);
        }
    }

    void result_output(const char* name, std::vector<uint64_t>& data)
    {
        if (data.size() > 0) {
            double p[] = { 0.99, 0.999, 0.9999 };
            uint64_t sum = 0;
            printf("  [%s]", name);
            for (size_t i = 0; i < data.size(); i++) {
                sum += data[i];
            }
            printf("[Average:%llu]", sum / (uint64_t)data.size());
            for (int i = 0; i < 3; i++) {
                size_t idx;
                size_t size;
                size = data.size();
                idx = (size_t)(1.0 * size * p[i]);
                printf("[%.2fth:%lluns|%zu/%zu]", 100.0 * p[i], data[idx], idx, size);
            }
            printf("\n");
        }
        return;
    }

public:
    LightKVServer()
    {
        printf("[LightKVServer::LightKVServer] LightKVServer Start!\n");
        search_match = 0;
        output = 0;
        option.num_server_thread = 1;
        option.num_backend_thread = 4;
        option.num_partition = 2048;
        option.num_segment = 3 * option.num_partition / 2;
        option.segment_size = 4 * 1024 * 1024;
        option.compact_count = 6;
        option.main_segment_size = option.num_partition * option.segment_size;
        option.persist_index = false;
    }

    ~LightKVServer()
    {
    }

    ResponseCode::type ping()
    {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName)
    {
        boost::ptr_map<std::string, LightKV>::iterator itr;
        itr = maps_.find(mapName);

        if (itr == maps_.end()) {
            printf("[LightKVServer::addMap] Create a new DB (LightKV)!\n");
            boost::unique_lock<boost::shared_mutex> writeLock(mutex_);
            strcpy(option.pmem_file_path, nvm_path);
            strcpy(option.db_path, db_path);
            LightKV* db = new LightKV(option);
            db->Init();
            std::string name = mapName;
            maps_.insert(name, db);
        } else {
            uint64_t sum_lat = 0;
            uint64_t lat_insert = 0;
            uint64_t lat_update = 0;
            uint64_t lat_search = 0;
            uint64_t lat_delete = 0;
            uint64_t lat_scan = 0;

            for (size_t i = 0; i < vec_lat_insert.size(); i++) {
                lat_insert += vec_lat_insert[i];
                sum_lat += vec_lat_insert[i];
            }
            for (size_t i = 0; i < vec_lat_update.size(); i++) {
                lat_update += vec_lat_update[i];
                sum_lat += vec_lat_update[i];
            }
            for (size_t i = 0; i < vec_lat_search.size(); i++) {
                lat_search += vec_lat_search[i];
                sum_lat += vec_lat_search[i];
            }
            for (size_t i = 0; i < vec_lat_delete.size(); i++) {
                lat_delete += vec_lat_delete[i];
                sum_lat += vec_lat_delete[i];
            }
            for (size_t i = 0; i < vec_lat_scan.size(); i++) {
                lat_scan += vec_lat_scan[i];
                sum_lat += vec_lat_scan[i];
            }

            size_t sum_opt = vec_lat_insert.size() + vec_lat_update.size() + vec_lat_search.size() + vec_lat_delete.size() + vec_lat_scan.size();
            printf("[LightKVServer::addMap] [SUM:%zu][PUT:%zu][UPDATE:%zu][GET:%llu/%zu][DEL:%zu][SCAN:%zu]\n", sum_opt, vec_lat_insert.size(), vec_lat_update.size(), search_match, vec_lat_search.size(), vec_lat_delete.size(), vec_lat_scan.size());

#if (defined LATENCY_OUTPUT)
            if (vec_lat_insert.size() > 0) {
                sprintf(name, "ycsb_%s_%d.latency", "INSERT", output);
                file_write(name, vec_lat_insert);
            }
            if (vec_lat_update.size() > 0) {
                sprintf(name, "ycsb_%s_%d.latency", "UPDATE", output);
                file_write(name, vec_lat_update);
            }
            if (vec_lat_search.size() > 0) {
                sprintf(name, "ycsb_%s_%d.latency", "SEARCH", output);
                file_write(name, vec_lat_search);
            }
            if (vec_lat_delete.size() > 0) {
                sprintf(name, "ycsb_%s_%d.latency", "DELETE", output);
                file_write(name, vec_lat_delete);
            }
            if (vec_lat_scan.size() > 0) {
                sprintf(name, "ycsb_%s_%d.latency", "SCAN", output);
                file_write(name, vec_lat_scan);
            }
#endif
            sort(vec_lat_insert.begin(), vec_lat_insert.end());
            sort(vec_lat_update.begin(), vec_lat_update.end());
            sort(vec_lat_search.begin(), vec_lat_search.end());
            sort(vec_lat_delete.begin(), vec_lat_delete.end());
            sort(vec_lat_scan.begin(), vec_lat_scan.end());
            result_output("INSERT", vec_lat_insert);
            result_output("UPDATE", vec_lat_update);
            result_output("SEARCH", vec_lat_search);
            result_output("DELETE", vec_lat_delete);
            result_output("SCAN", vec_lat_scan);
            search_match = 0;
            output++;
            vec_lat_insert.clear();
            vec_lat_search.clear();
            vec_lat_delete.clear();
            vec_lat_scan.clear();
            vec_lat_update.clear();
            printf("[LightKVServer::addMap] [existed DB]\n");
        }
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName)
    {
        printf("[LightKVServer::dropMap] Remove drapMap!\n");
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return)
    {
        printf("[LightKVServer::listMaps] ListMaps!\n");
        _return.responseCode = ResponseCode::Success;
    }

    void scan(RecordListResponse& _return, const std::string& mapName, const ScanOrder::type order,
        const std::string& startKey, const bool startKeyIncluded,
        const std::string& endKey, const bool endKeyIncluded,
        const int32_t maxRecords, const int32_t maxBytes)
    {
    }

    void scanAscending(RecordListResponse& _return, const string& mapName,
        const std::string& startKey, const bool startKeyIncluded,
        const std::string& endKey, const bool endKeyIncluded,
        const int32_t maxRecords, const int32_t maxBytes)
    {
    }

    void scanDescending(RecordListResponse& _return, const std::string& mapName,
        const std::string& startKey, const bool startKeyIncluded,
        const std::string& endKey, const bool endKeyIncluded,
        const int32_t maxRecords, const int32_t maxBytes)
    {
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& string_key)
    {
        if ((string_key.size() - 4) > YCSB_KEY_LENGTH) {
            std::cout << string_key << std::endl;
            printf("[LightKVServer::Get] Invaild key/value length: %zu\n", string_key.size());
        }

        assert((string_key.size() - 4) <= YCSB_KEY_LENGTH);
        boost::shared_lock<boost::shared_mutex> readLock(mutex_);
        boost::ptr_map<std::string, LightKV>::iterator itr = maps_.find(mapName);

        if (itr == maps_.end()) {
            printf("[LightKVServer::Get] Invaild Mapper!\n");
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }

        Timer timer;
        uint64_t latency_ns;
        uint8_t key[YCSB_KEY_LENGTH] = { 0 };
        uint8_t value[YCSB_VALUE_LENGTH] = { 0 };
        size_t key_length = YCSB_KEY_LENGTH;
        size_t value_length;

        for (int i = 0; i < key_length; i++) {
            key[i] = '0';
        }
        memcpy(key, string_key.data() + 4, string_key.size() - 4); // remove 'user' head

        timer.Start();
        bool res = itr->second->Get(key, key_length, value, value_length);
        timer.Stop();
        latency_ns = timer.Get();
        vec_lat_search.push_back(latency_ns);

        if (!res) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        } else {
            search_match++;
            _return.responseCode = ResponseCode::Success;
        }
    }

    ResponseCode::type put(const std::string& mapName, const std::string& string_key, const std::string& string_value)
    {
        if (((string_key.size() - 4) > YCSB_KEY_LENGTH)) // skip 'user'
        {
            std::cout << string_key << std::endl;
            printf("[LightKVServer::PUT] Invaild key/value length: %zu/%zu\n", string_key.size(), string_value.size());
        }
        assert(((string_key.size() - 4) <= YCSB_KEY_LENGTH));

        boost::ptr_map<std::string, LightKV>::iterator itr;
        boost::shared_lock<boost::shared_mutex> readLock(mutex_);
        itr = maps_.find(mapName);

        if (itr == maps_.end()) {
            printf("[LightKVServer::PUT] Put Not Found!\n");
            return ResponseCode::MapNotFound;
        }

        Timer timer;
        uint64_t latency_ns;
        uint8_t key[YCSB_KEY_LENGTH] = { 0 };
        uint8_t value[YCSB_VALUE_LENGTH] = { 0 };
        size_t key_length = YCSB_KEY_LENGTH;
        size_t value_length = YCSB_VALUE_LENGTH < string_value.size() ? YCSB_VALUE_LENGTH : string_value.size();

        for (int i = 0; i < key_length; i++) {
            key[i] = '0';
        }
        memcpy(key, string_key.data() + 4, string_key.size() - 4);
        memcpy(value, string_value.data(), value_length);

        timer.Start();
        itr->second->Put(key, key_length, value, value_length);
        timer.Stop();
        latency_ns = timer.Get();
        vec_lat_insert.push_back(latency_ns);
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& string_key, const std::string& string_value)
    {
        if (((string_key.size() - 4) > YCSB_KEY_LENGTH)) // skip 'user'
        {
            std::cout << string_key << std::endl;
            printf("[LightKVServer::PUT] Invaild key/value length: %zu/%zu\n", string_key.size(), string_value.size());
        }

        assert(((string_key.size() - 4) <= YCSB_KEY_LENGTH));
        boost::ptr_map<std::string, LightKV>::iterator itr;
        boost::shared_lock<boost::shared_mutex> readLock(mutex_);
        itr = maps_.find(mapName);

        if (itr == maps_.end()) {
            printf("[LightKVServer::PUT] Put Not Found!\n");
            return ResponseCode::MapNotFound;
        }

        Timer timer;
        uint64_t latency_ns;
        uint8_t key[YCSB_KEY_LENGTH] = { 0 };
        uint8_t value[YCSB_VALUE_LENGTH] = { 0 };
        size_t key_length = YCSB_KEY_LENGTH;
        size_t value_length = YCSB_VALUE_LENGTH < string_value.size() ? YCSB_VALUE_LENGTH : string_value.size();

        for (int i = 0; i < key_length; i++) {
            key[i] = '0';
        }
        memcpy(key, string_key.data() + 4, string_key.size() - 4);
        memcpy(value, string_value.data(), value_length);

        timer.Start();
        itr->second->Put(key, key_length, value, value_length);
        timer.Stop();
        latency_ns = timer.Get();
        vec_lat_insert.push_back(latency_ns);
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& string_key, const std::string& string_value)
    {
        if (((string_key.size() - 4) > YCSB_KEY_LENGTH)) // skip 'user'
        {
            std::cout << string_key << std::endl;
            printf("[LightKVServer::PUT] Invaild key/value length: %zu/%zu\n", string_key.size(), string_value.size());
        }
        assert(((string_key.size() - 4) <= YCSB_KEY_LENGTH));

        boost::ptr_map<std::string, LightKV>::iterator itr;
        boost::shared_lock<boost::shared_mutex> readLock(mutex_);
        itr = maps_.find(mapName);

        if (itr == maps_.end()) {
            printf("[LightKVServer::PUT] Put Not Found!\n");
            return ResponseCode::MapNotFound;
        }

        Timer timer;
        uint64_t latency_ns;
        uint8_t key[YCSB_KEY_LENGTH] = { 0 };
        uint8_t value[YCSB_VALUE_LENGTH] = { 0 };
        size_t key_length = YCSB_KEY_LENGTH;
        size_t value_length = YCSB_VALUE_LENGTH < string_value.size() ? YCSB_VALUE_LENGTH : string_value.size();

        for (int i = 0; i < key_length; i++) {
            key[i] = '0';
        }
        memcpy(key, string_key.data() + 4, string_key.size() - 4);
        memcpy(value, string_value.data(), value_length);

        timer.Start();
        itr->second->Update(key, key_length, value, value_length);
        timer.Stop();
        latency_ns = timer.Get();
        vec_lat_update.push_back(latency_ns);
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& string_key)
    {
        return ResponseCode::Success;
    }

private:
    boost::ptr_map<std::string, LightKV> maps_;
    boost::shared_mutex mutex_;
    Option option;

private:
    uint64_t search_match;
    std::vector<uint64_t> vec_lat_insert;
    std::vector<uint64_t> vec_lat_update;
    std::vector<uint64_t> vec_lat_search;
    std::vector<uint64_t> vec_lat_scan;
    std::vector<uint64_t> vec_lat_delete;
};

int main(int argc, char** argv)
{
    if (argc != 2) {
        printf("Please input [nvm path:%s] [db path:%s]\n", argv[1], argv[2]);
    }
    memcpy(nvm_path, argv[1], strlen(argv[1]));
    memcpy(db_path, argv[2], strlen(argv[2]));

    int port = 9090;
    std::shared_ptr<LightKVServer> handler(new LightKVServer());
    std::shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    std::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    std::shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    std::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server(processor, serverTransport, transportFactory, protocolFactory);

    cpu_set_t mask;
    CPU_ZERO(&mask);
    CPU_SET(0, &mask);
    if (pthread_setaffinity_np(pthread_self(), sizeof(mask), &mask) < 0) {
        printf("threadpool, set thread affinity failed.\n");
    }
    server.serve();
    return 0;
}
