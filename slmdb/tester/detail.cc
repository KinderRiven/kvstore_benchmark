#include <algorithm>
#include <dirent.h>
#include <fcntl.h>
#include <fstream>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

static void result_output(const char* name, std::vector<uint64_t>& data)
{
    std::ofstream fout(name);
    if (fout.is_open()) {
        for (int i = 0; i < data.size(); i++) {
            fout << data[i] << std::endl;
        }

        fout.close();
    }
}

static bool read_from_file(const char* name, std::vector<uint64_t>& vec_opt_latency)
{
    uint64_t latency;
    std::ifstream fin(name);

    if (fin.is_open()) {
        while (fin >> latency) {
            vec_opt_latency.push_back(latency);
        }
        fin.close();
        return true;
    } else {
        return false;
    }
}

static void tail_latency_handler(const char* name, std::vector<uint64_t>& vec_opt_latency, double p, uint64_t interval)
{
    size_t idx;
    size_t size;
    char new_name[128];
    std::vector<uint64_t> vec_tail_latency;
    std::vector<uint64_t> vec_tmp;

    snprintf(new_name, sizeof(new_name), "%s_%.2fth", name, 100.0 * p);
    size = vec_opt_latency.size();

    for (size_t i = 0; i < size; i++) {
        vec_tmp.push_back(vec_opt_latency[i]);
        if (i % interval == 0) {
            sort(vec_tmp.begin(), vec_tmp.end());
            idx = idx = (size_t)(1.0 * vec_tmp.size() * p);
            vec_tail_latency.push_back(vec_tmp[idx]);
            vec_tmp.clear();
        }
    }

    printf("  [%s] (%zu)\n", new_name, vec_tail_latency.size());
    result_output(new_name, vec_tail_latency);
}

static void get_avg_latency(std::vector<uint64_t>& vec_opt_latency)
{
    size_t size = vec_opt_latency.size();
    uint64_t sum = 0;
    for (size_t i = 0; i < size; i++) {
        sum += vec_opt_latency[i];
    }
    sum /= size;
    printf("  [Average]:%9lluns\n", sum);
    return;
}

static void get_tail_latency(std::vector<uint64_t>& vec_opt_latency, double p)
{
    size_t idx;
    size_t size;
    size = vec_opt_latency.size();
    idx = (size_t)(1.0 * size * p);
    printf("  [%.2fth]:%9lluns|(%9zu/%-9zu)\n", 100.0 * p, vec_opt_latency[idx], idx, size);
    return;
}

static void detail_handler(const char* name, int interval)
{
    std::vector<uint64_t> vec_opt_latency;

    read_from_file(name, vec_opt_latency);
    printf(">>[Handler] Reading opterator latency from [%s] (%zu).\n", name, vec_opt_latency.size());

    tail_latency_handler(name, vec_opt_latency, 0.99, interval);
    tail_latency_handler(name, vec_opt_latency, 0.999, interval);
    tail_latency_handler(name, vec_opt_latency, 0.9999, interval);

    sort(vec_opt_latency.begin(), vec_opt_latency.end());
    get_avg_latency(vec_opt_latency);
    get_tail_latency(vec_opt_latency, 0.5);
    get_tail_latency(vec_opt_latency, 0.75);
    get_tail_latency(vec_opt_latency, 0.90);
    get_tail_latency(vec_opt_latency, 0.99);
    get_tail_latency(vec_opt_latency, 0.999);
    get_tail_latency(vec_opt_latency, 0.9999);
}

int main(int argc, char* argv[])
{
    char dir[128];
    char dir_name[128];
    char cur_dir[] = ".";
    char up_dir[] = "..";

    DIR* dirp;
    struct dirent* dp;
    struct stat dir_stat;

    uint64_t interval;

    if (argc < 2) {
        return -1;
    }

    strcpy(dir, argv[1]);
    interval = atol(argv[2]);

    if (0 != access(dir, F_OK)) {
        printf("error1 (%s)\n", dir);
        return 0;
    }

    if (0 > stat(dir, &dir_stat)) {
        printf("error2 (%s)\n", dir);
        return -1;
    }

    if (S_ISDIR(dir_stat.st_mode)) {
        dirp = opendir(dir);
        while ((dp = readdir(dirp)) != NULL) {
            if ((0 == strcmp(cur_dir, dp->d_name)) || (0 == strcmp(up_dir, dp->d_name))) {
                continue;
            }
            sprintf(dir_name, "%s/%s", dir, dp->d_name);
            detail_handler(dir_name, interval);
        }
        closedir(dirp);
    }

    return 0;
}
