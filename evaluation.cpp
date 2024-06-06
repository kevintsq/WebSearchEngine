#define _CRT_SECURE_NO_WARNINGS
#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <queue>
#include <algorithm>
#include <cmath>
#include <chrono>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>

using std::string;
using std::vector;
using std::pair;
using std::ifstream;
using std::cin;
using std::cerr;
using std::endl;
using std::getline;
using std::unordered_map;
using std::unordered_set;
using std::list;
using std::shared_ptr;
using std::make_shared;
using std::sort;
using std::thread;
using std::unique_lock;
using std::mutex;
using std::condition_variable;
using std::queue;
using std::tuple;
using std::get;
using std::lock_guard;

#ifdef _MSC_VER
#define ftell64 _ftelli64
#define fseek64 _fseeki64
#else
#define ftell64 ftello64
#define fseek64 fseeko64
#endif

struct Entry {
    string term;
    vector<unsigned> doc_ids, freqs;

    bool operator<(const Entry &rhs) const {
        return term < rhs.term || (term == rhs.term && doc_ids < rhs.doc_ids);
    }

    bool operator>(const Entry &rhs) const {
        return term > rhs.term || (term == rhs.term && doc_ids > rhs.doc_ids);
    }
};

using EntryP = shared_ptr<Entry>;

struct StorageInfo {
    long long ids_begin = 0, freqs_begin = 0;
    unsigned doc_cnt = 0;
};

unordered_map<string, StorageInfo> storage_info;
//mutex storage_info_mutex;

struct DocInfo {
    string url;
    unsigned term_cnt = 0;
    long long begin = 0, end = 0;
};

vector<DocInfo> docs_info;
//mutex docs_info_mutex;

using ResultDocInfos = vector<pair<unsigned, double>>;

template<typename KeyType, typename ValueType>
class LRUCache {
protected:
    int capacity;
    unordered_map<KeyType, pair<ValueType, typename list<KeyType>::iterator>> cache;
    list<KeyType> lru_list;
    mutex cache_mutex;

public:
    explicit LRUCache(int capacity) : capacity(capacity) {}

    virtual ~LRUCache() = default;

    ValueType get(const KeyType &key) {
        lock_guard<mutex> lock(cache_mutex);
        if (cache.find(key) != cache.end()) {
            lru_list.erase(cache[key].second);
            lru_list.push_front(key);
            cache[key].second = lru_list.begin();
            return cache[key].first;
        } else {
            return ValueType();
        }
    }

    virtual void put(const KeyType &key, const ValueType &value) {
        lock_guard<mutex> lock(cache_mutex);
        if (cache.find(key) != cache.end()) {
            lru_list.erase(cache[key].second);
        } else if (cache.size() >= capacity) {
            KeyType leastRecent = lru_list.back();
            lru_list.pop_back();
            cache.erase(leastRecent);
        }
        lru_list.push_front(key);
        cache[key] = {value, lru_list.begin()};
    }
};

//template <typename KeyType>
//class LRUCacheForPyObjects : public LRUCache<KeyType, PyObject *> {
//public:
//    explicit LRUCacheForPyObjects(int capacity) : LRUCache<KeyType, PyObject *>(capacity) {}
//
//    ~LRUCacheForPyObjects() override {
//        for (auto &item: this->cache) {
//            Py_DECREF(item.second.first);
//        }
//    }
//
//    virtual void put(const KeyType &key, PyObject *value) {
//        if (this->cache.find(key) != this->cache.end()) {
//            this->lru_list.erase(this->cache[key].second);
//        } else if (this->cache.size() >= this->capacity) {
//            KeyType leastRecent = this->lru_list.back();
//            this->lru_list.pop_back();
//            Py_DECREF(this->cache[leastRecent].first);
//            this->cache.erase(leastRecent);
//        }
//        this->lru_list.push_front(key);
//        this->cache[key] = {value, this->lru_list.begin()};
//    }
//};

void read_index_bin(FILE *ids_fp, FILE *freqs_fp, long long ids_begin, long long freqs_begin,
                    unsigned count, EntryP &entry) {
    fseek64(ids_fp, ids_begin, SEEK_SET);
    unsigned prev_doc_id = 0;
    unsigned doc_id = 0;
    for (unsigned i = 0; i < count; i++) {
        fread(&doc_id, sizeof(unsigned), 1, ids_fp);
        doc_id += prev_doc_id;
        entry->doc_ids.push_back(doc_id);
        prev_doc_id = doc_id;
    }
    fseek64(freqs_fp, freqs_begin, SEEK_SET);
    entry->freqs.resize(count);
    fread(entry->freqs.data(), sizeof(unsigned), count, freqs_fp);
}

void read_index_vbyte(FILE *ids_fp, FILE *freqs_fp, long long ids_begin, long long freqs_begin,
                      unsigned count, EntryP &entry) {
    fseek64(ids_fp, ids_begin, SEEK_SET);
    unsigned prev_doc_id = 0;
    unsigned doc_id = 0;
    unsigned shift = 0;
    for (unsigned i = 0; i < count;) {
        unsigned char byte;
        fread(&byte, sizeof(unsigned char), 1, ids_fp);
        doc_id |= (byte & 0x7f) << shift;
        if (byte & 0x80) {
            doc_id += prev_doc_id;
            entry->doc_ids.push_back(doc_id);
            prev_doc_id = doc_id;
            doc_id = 0;
            shift = 0;
            i++;
        } else {
            shift += 7;
        }
    }
    fseek64(freqs_fp, freqs_begin, SEEK_SET);
    unsigned freq = 0;
    shift = 0;
    for (unsigned i = 0; i < count;) {
        unsigned char byte;
        fread(&byte, sizeof(unsigned char), 1, freqs_fp);
        freq |= (byte & 0x7f) << shift;
        if (byte & 0x80) {
            entry->freqs.push_back(freq);
            freq = 0;
            shift = 0;
            i++;
        } else {
            shift += 7;
        }
    }
}

int get_utf8_char_len(const char *s, const char *end) {
    if ((*s & 0x80) == 0) { // 0xxxxxxx is 1 byte character
        return 1;
    } else if ((*s & 0xE0) == 0xC0) { // 110xxxxx is 2 byte character
        if (s + 1 <= end && (*(s + 1) & 0xC0) == 0x80) {
            return 2;
        }
    } else if ((*s & 0xF0) == 0xE0) { // 1110xxxx is 3 byte character
        if (s + 2 <= end && (*(s + 1) & 0xC0) == 0x80 && (*(s + 2) & 0xC0) == 0x80) {
            return 3;
        }
    } else if ((*s & 0xF8) == 0xF0) { // 11110xxx is 4 byte character
        if (s + 3 <= end && (*(s + 1) & 0xC0) == 0x80 && (*(s + 2) & 0xC0) == 0x80 &&
            (*(s + 3) & 0xC0) == 0x80) {
            return 4;
        }
    }
    fprintf(stderr, "invalid utf-8 character at %p\n", s);
    exit(EXIT_FAILURE);
}

bool is_al_num(const char *c, int char_len) {
    // reference: https://www.compart.com/en/unicode/block
    if (char_len == 1) {
        return isalnum(*c);
    } else if (char_len == 3) {
        bool is_general_punctuation = *c == '\xe2' &&
                                      (*(c + 1) == '\x80' && *(c + 2) >= '\x80' ||
                                       *(c + 1) == '\x81' && *(c + 2) <= '\xaf');
        bool is_cjk_symbols_and_punctuation = *c == '\xe3' &&
                                              (*(c + 1) == '\x80' && *(c + 2) >= '\x80' ||
                                               *(c + 1) == '\x81' && *(c + 2) <= '\xbf');
        if (is_general_punctuation || is_cjk_symbols_and_punctuation) {
            return false;
        }
    }
    return true;
}

void print_usage(char *program_name) {
    printf("Usage: %s [-h] [-p doc_info_file] [-s storage_info_file]\n"
           "\t[-i index_ids_file] [-f index_freqs_file] [-t index_file_type]\n"
           "\t[-q queries_path] [-r relevance_path] [-n n_results] [-m n_threads] [-c cache_size]\n"
           "Options:\n"
           "\t-p\tdoc info (page table) file, default: docs.txt\n"
           "\t-s\tstorage info (lexicon) file, default: storage_vbyte.txt\n"
           "\t-i\tindex ids file, default: merged_index.vbyte\n"
           "\t-f\tindex freqs file, default: freqs.vbyte\n"
           "\t-t\tindex file type (bin|vbyte), default: vbyte\n"
           "\t-q\tqueries path, default: queries.doctrain.tsv\n"
           "\t-r\trelevance path, default: msmarco-doctrain-qrels-idconverted.tsv\n"
           "\t-n\tnumber of results, default: 10\n"
           "\t-m\tnumber of threads, default: number of logical cores (%u on this machine)\n"
           "\t-c\tcache size, default: 131072\n"
           "\t-h\thelp\n", program_name, std::thread::hardware_concurrency());
}

struct Options {
    const char *doc_info_path = "docs.txt";
    const char *storage_path = "storage_vbyte.txt";
    const char *index_ids_path = "merged_index.vbyte";
    const char *index_freqs_path = "freqs.vbyte";
    const char *queries_path = "queries.doctrain.tsv";
    const char *relevance_path = "msmarco-doctrain-qrels-idconverted.tsv";
    // func pointer for read_index
    decltype(read_index_vbyte) *read_index = read_index_vbyte;
    int total_doc_cnt = 3213835;
    double avg_doc_len = 1172.448644;
    double k = 0.9, b = 0.4;
    int n_results = 10;
    int n_threads = (int) std::thread::hardware_concurrency();
    int cache_size = 131072;
};

Options parse_args(int argc, char *argv[]) {
    Options options;
    for (int i = 1; i < argc; i += 2) {
        char *option = argv[i];
        if (strcmp(option, "-h") == 0) {
            print_usage(argv[0]);
            exit(EXIT_SUCCESS);
        }
        if (i + 1 < argc) {
            char *value = argv[i + 1];
            if (strcmp(option, "-p") == 0) options.doc_info_path = value;
            else if (strcmp(option, "-s") == 0) options.storage_path = value;
            else if (strcmp(option, "-i") == 0) options.index_ids_path = value;
            else if (strcmp(option, "-f") == 0) options.index_freqs_path = value;
            else if (strcmp(option, "-q") == 0) options.queries_path = value;
            else if (strcmp(option, "-r") == 0) options.relevance_path = value;
            else if (strcmp(option, "-t") == 0) {
                if (strcmp(value, "bin") == 0) {
                    options.read_index = read_index_bin;
                } else if (strcmp(value, "vbyte") == 0) {
                    options.read_index = read_index_vbyte;
                } else {
                    cerr << "Invalid value for option -t: " << value << endl;
                    print_usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
            } else if (strcmp(option, "-n") == 0) {
                options.n_results = atoi(value);
                if (options.n_results <= 0) {
                    cerr << "Invalid value for option -n: " << value << endl;
                    print_usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
            } else if (strcmp(option, "-m") == 0) {
                options.n_threads = atoi(value);
                if (options.n_threads <= 0) {
                    cerr << "Invalid value for option -m: " << value << endl;
                    print_usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
            } else if (strcmp(option, "-c") == 0) {
                options.cache_size = atoi(value);
                if (options.cache_size <= 0) {
                    cerr << "Invalid value for option -m: " << value << endl;
                    print_usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
            } else {
                cerr << "Unknown option: " << option << endl;
                print_usage(argv[0]);
                exit(EXIT_FAILURE);
            }
        } else {
            cerr << "Missing value for option: " << argv[i] << endl;
            print_usage(argv[0]);
            exit(EXIT_FAILURE);
        }
    }
    return options;
}

void read_storage_info(const char *filename) {
    printf("Reading storage info from %s...", filename);
    fflush(stdout);
    ifstream fin(filename);
    if (!fin.is_open()) {
        cerr << "Failed to open file " << filename << endl;
        exit(EXIT_FAILURE);
    }
    string term;
    StorageInfo info;
    while (fin >> term >> info.ids_begin >> info.freqs_begin >> info.doc_cnt) {
        storage_info[term] = info;
    }
    fin.close();
    printf("done\n");
}

void read_docs_info(Options &options) {
    printf("Reading docs info from %s...", options.doc_info_path);
    fflush(stdout);
    ifstream fin(options.doc_info_path);
    if (!fin.is_open()) {
        cerr << "Failed to open file " << options.doc_info_path << endl;
        exit(EXIT_FAILURE);
    }
    DocInfo info;
    unsigned long long total_term_cnt = 0;
    while (fin >> info.url >> info.term_cnt >> info.begin >> info.end) {
        docs_info.push_back(info);
        total_term_cnt += info.term_cnt;
    }
    options.total_doc_cnt = (int) docs_info.size();
    options.avg_doc_len = (double) total_term_cnt / options.total_doc_cnt;
    fin.close();
    printf("done\n");
}

FILE *fopen_guarded(const string &filename, const char *mode) {
    FILE *fp = fopen(filename.c_str(), mode);
    if (fp == nullptr) {
        perror(("Failed to open file " + filename).c_str());
        exit(EXIT_FAILURE);
    }
    return fp;
}

string clean_query(const string &query, vector<string> &query_list) {
    // Split query by space
    // - Omit leading and trailing spaces
    // - Omit duplicate words
    // - Treat consecutive spaces as one
    // - Convert to lowercase
    unordered_set<string> query_set;
    string cleaned_query = query;
    char *begin_p = cleaned_query.data();
    char *end_p = cleaned_query.data() + cleaned_query.size();
    while (begin_p < end_p) {
        char* word_begin = begin_p;
        int len = get_utf8_char_len(begin_p, end_p);
        while (is_al_num(begin_p, len)) {
            if (len == 1) {
                *begin_p = (char)tolower(*begin_p);
            }
            begin_p += len;
            len = get_utf8_char_len(begin_p, end_p);
        }
        if (begin_p > word_begin) {
            *begin_p = '\0';
            if (query_set.find(word_begin) == query_set.end()) {
                query_set.emplace(word_begin);
                query_list.emplace_back(word_begin);
            }
        }
        begin_p += len;
    }
    sort(query_list.begin(), query_list.end());
    cleaned_query.clear();
    for (const auto &word : query_list) {
        cleaned_query += word + " ";
    }
    if (!cleaned_query.empty()) {
        cleaned_query.pop_back();
    }
    return cleaned_query;
}

double BM25(unsigned freq, unsigned doc_cnt, unsigned doc_len, const Options &options) {
    double K = options.k * ((1 - options.b) + options.b * doc_len / options.avg_doc_len);
    return log((options.total_doc_cnt - doc_cnt + 0.5) / (doc_cnt + 0.5)) *
           (freq * (options.k + 1)) / (freq + K);
}

class Evaluator {
protected:
    const Options &options;

    virtual shared_ptr<ResultDocInfos>
    collect_and_rank_docs(const string &query) = 0;

public:
    const char *name = "Evaluator";

    explicit Evaluator(const Options &options) : options(options) {}

    virtual ~Evaluator() = default;

    virtual double evaluate(const string &query, unsigned relevant_doc_id) = 0;
};

class BM25Evaluator : public Evaluator {
    FILE *ids_fp = nullptr, *freqs_fp = nullptr;
    LRUCache<string, shared_ptr<Entry>> &entry_cache;
    vector<string> query_list;
    unordered_map<unsigned, double> infos;
    vector<EntryP> entries;

public:
    explicit BM25Evaluator(LRUCache<string, shared_ptr<Entry>> &entry_cache, const Options &options) :
            Evaluator(options),
            ids_fp(fopen_guarded(options.index_ids_path, "rb")),
            freqs_fp(fopen_guarded(options.index_freqs_path, "rb")),
            entry_cache(entry_cache) {
        name = "BM25Evaluator";
    }

    ~BM25Evaluator() override {
        if (ids_fp != nullptr) {
            fclose(ids_fp);
        }
        if (freqs_fp != nullptr) {
            fclose(freqs_fp);
        }
    }

    double evaluate(const string &query, unsigned relevant_doc_id) override {
        // Clean query
        query_list.clear();
        const string &cleaned_query = clean_query(query, query_list);

        // Collect and rank docs
        auto sorted_infos = collect_and_rank_docs(cleaned_query);

        // Evaluate results
        if (!sorted_infos) {
            return 0;
        }
        for (int i = 0; i < options.n_results && i < sorted_infos->size(); i++) {
            auto [doc_id, _] = sorted_infos->at(i);
            if (doc_id == relevant_doc_id) {
                return 1. / (i + 1);
            }
        }
        return 0;
    }

    shared_ptr<ResultDocInfos>
    collect_and_rank_docs(const string &query) override {
        // Search query in storage_info
        entries.clear();
        for (const auto &term : query_list) {
            //storage_info_mutex.lock();
            if (storage_info.find(term) != storage_info.end()) {
                // Check if entry is in cache
                auto entry = entry_cache.get(term);
                if (!entry) {
                    entry = make_shared<Entry>();
                    entry->term = term;
                    auto info = storage_info[term];
                    //storage_info_mutex.unlock();
                    entry->doc_ids.reserve(info.doc_cnt);
                    entry->freqs.reserve(info.doc_cnt);
                    // Read entry from file
                    options.read_index(ids_fp, freqs_fp, info.ids_begin, info.freqs_begin, info.doc_cnt, entry);
                    // Cache entry
                    entry_cache.put(term, entry);
                }
                /*else {
                    storage_info_mutex.unlock();
                }*/
                entries.push_back(entry);
            }
            /*else {
                storage_info_mutex.unlock();
            }*/
        }
        if (entries.empty()) {
            return nullptr;
        }

        // Calculate infos and sort by score
        infos.clear();
        auto last_intersection = entries[0]->doc_ids;  // if not pointer, use & to avoid copy
        vector<unsigned> current_intersection;
        for (int i = 1; i < entries.size(); i++) {
            set_intersection(last_intersection.begin(), last_intersection.end(),
                             entries[i]->doc_ids.begin(), entries[i]->doc_ids.end(),
                             back_inserter(current_intersection));
            swap(last_intersection, current_intersection);
            current_intersection.clear();
        }
        for (const auto &entry : entries) {
            for (auto doc_id : last_intersection) {
                auto it = lower_bound(entry->doc_ids.begin(), entry->doc_ids.end(), doc_id);
                if (it != entry->doc_ids.end() && *it == doc_id) {
                    auto freq = entry->freqs[it - entry->doc_ids.begin()];
                    unsigned term_cnt;
                    {
                        //lock_guard<mutex> lock(docs_info_mutex);
                        term_cnt = docs_info[doc_id].term_cnt;
                    }
                    infos[doc_id] += BM25(freq, (unsigned)entry->doc_ids.size(), term_cnt, options);
                }
            }
        }

        // Sort by score
        auto sorted_infos = make_shared<ResultDocInfos>();
        sorted_infos->reserve(infos.size());
        for (const auto &info : infos) {
            sorted_infos->emplace_back(info);
        }
        sort(sorted_infos->begin(), sorted_infos->end(),
             [](const pair<unsigned, double> &lhs, const pair<unsigned, double> &rhs) {
                 return lhs.second > rhs.second ||
                        (lhs.second == rhs.second && lhs.first < rhs.first);
             });


        return sorted_infos;
    }
};

class EvaluatorPool {
public:
    explicit EvaluatorPool(vector<Evaluator *> &evaluators) :
            evaluators(evaluators), n_threads((int)evaluators.size()), stop(false) {
        for (int i = 0; i < n_threads; ++i) {
            threads.emplace_back(&EvaluatorPool::worker_thread, this, i);
        }
    }

    void add_task(const string &query, unsigned relevant_doc_id) {
        {
            unique_lock<mutex> lock(queueMutex);
            tasks.emplace(query, relevant_doc_id);
        }
        condition.notify_one();
    }

    vector<double> get_results() {
        {
            unique_lock<mutex> lock(queueMutex);
            stop = true;
        }
        condition.notify_all();

        for (auto &thread : threads) {
            thread.join();
        }
        return results;
    }

    ~EvaluatorPool() {
        {
            unique_lock<mutex> lock(queueMutex);
            stop = true;
        }
        condition.notify_all();

        for (auto &thread : threads) {
            if (thread.joinable()) {
                thread.join();
            }
        }
    }

private:
    vector<Evaluator *> &evaluators;
    vector<thread> threads;
    queue<tuple<string, unsigned>> tasks;
    mutex queueMutex;
    condition_variable condition;
    vector<double> results;
    mutex result_mutex;
    int n_threads;
    bool stop;

    void worker_thread(int id) {
        int cnt = 0;
        while (true) {
            tuple<string, unsigned> task;
            {
                unique_lock<mutex> lock(queueMutex);
                condition.wait(lock, [this] { return stop || !tasks.empty(); });

                if (stop && tasks.empty()) {
                    return;
                }

                task = std::move(tasks.front());
                tasks.pop();
            }

            // Extract task arguments
            const string &query = get<0>(task);
            unsigned relevant_doc_id = get<1>(task);

            // Process the task using the corresponding evaluator
            auto e = evaluators[id];
            double result = e->evaluate(query, relevant_doc_id);
            cnt++;
            if (cnt % 100 == 0) {
                printf("Thread %2d: %5d tasks processed\n", id, cnt);
            }

            // Store the result
            {
                unique_lock<mutex> lock(result_mutex);
                results.push_back(result);
            }
        }
    }
};

vector<pair<unsigned, string>> read_queries(const string &filename) {
    printf("Reading queries from %s...", filename.c_str());
    fflush(stdout);
    vector<pair<unsigned, string>> queries;
    ifstream fin(filename);
    if (!fin.is_open()) {
        cerr << "Failed to open file " << filename << endl;
        exit(EXIT_FAILURE);
    }
    unsigned id;
    string query;
    while (fin >> id) {
        getline(fin, query);
        queries.emplace_back(id, query);
    }
    fin.close();
    printf("done\n");
    return queries;
}

unordered_map<unsigned, unsigned> read_relevance(const char *filename) {
    printf("Reading relevance from %s...", filename);
    fflush(stdout);
    unordered_map<unsigned, unsigned> relevance;
    FILE *fp = fopen_guarded(filename, "r");
    unsigned id, doc_id;
    while (fscanf(fp, "%u %u", &id, &doc_id) == 2) {
        relevance[id] = doc_id;
    }
    fclose(fp);
    printf("done\n");
    return relevance;
}

int main(int argc, char *argv[]) {
    Options options = parse_args(argc, argv);
    read_storage_info(options.storage_path);
    read_docs_info(options);
    auto queries = read_queries(options.queries_path);
    auto relevance = read_relevance(options.relevance_path);
    LRUCache<string, shared_ptr<Entry>> entry_cache(options.cache_size);

    auto start = std::chrono::steady_clock::now();

    vector<Evaluator *> evaluators(options.n_threads);
    for (int i = 0; i < options.n_threads; i++) {
        evaluators[i] = new BM25Evaluator(entry_cache, options);
    }
    EvaluatorPool pool(evaluators);
    for (auto &[id, query] : queries) {
        pool.add_task(query, relevance[id]);
    }

    auto results = pool.get_results();
    double sum = 0;
    for (auto result : results) {
        sum += result;
    }
    printf("MRR@%d: %f\n", options.n_results, sum / (double) results.size());

    auto end = std::chrono::steady_clock::now();
    auto diff = std::chrono::duration<double>(end - start).count();
    printf("Time elapsed: %.2f seconds\n", diff);

    for (auto evaluator : evaluators) {
        delete evaluator;
    }
    return 0;
}
