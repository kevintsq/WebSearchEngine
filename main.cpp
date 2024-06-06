#include <iostream>
#include <vector>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <list>
#include <algorithm>
#include <cmath>
#include <csignal>
#include <Python.h>
#include "httplib.h"
#include "json.hpp"

using json = nlohmann::json;
using std::string;
using std::vector;
using std::pair;
using std::ifstream;
using std::stringstream;
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
using std::min;
using std::max;

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

struct DocInfo {
    string url;
    unsigned term_cnt = 0;
    long long begin = 0, end = 0;
};

vector<DocInfo> docs_info;

struct ResultDocInfo {
    double score = 0;
    shared_ptr<vector<pair<string, unsigned>>> freqs = make_shared<vector<pair<string, unsigned>>>();
};

using ResultDocInfos = vector<pair<unsigned, ResultDocInfo>>;

template<typename T>
class LRUCache {
private:
    int capacity;
    unordered_map<string, pair<shared_ptr<T>, list<string>::iterator>> cache;
    list<string> lruList;

public:
    explicit LRUCache(int capacity) : capacity(capacity) {}

    shared_ptr<T> get(const string &key) {
        if (cache.find(key) != cache.end()) {
            lruList.erase(cache[key].second);
            lruList.push_front(key);
            cache[key].second = lruList.begin();
            return cache[key].first;
        } else {
            return nullptr;
        }
    }

    void put(const string &key, const shared_ptr<T> &value) {
        if (cache.find(key) != cache.end()) {
            lruList.erase(cache[key].second);
        } else if (cache.size() >= capacity) {
            string leastRecent = lruList.back();
            lruList.pop_back();
            cache.erase(leastRecent);
        }
        lruList.push_front(key);
        cache[key] = {value, lruList.begin()};
    }
};

FILE *ids_fp, *freqs_fp, *dataset_fp;
unordered_map<unsigned, ResultDocInfo> infos;
vector<EntryP> entries;
char *doc_content, *tokenize_buffer, *home_page_buffer;
size_t buf_size = 2064927;
class Searcher *searcher;
httplib::Server svr;

void read_index_bin(long long ids_begin, long long freqs_begin,
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

void read_index_vbyte(long long ids_begin, long long freqs_begin,
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
    printf("Usage: %s [-h] [-d dataset_file] [-p doc_info_file] [-s storage_info_file]\n"
           "\t[-i index_ids_file] [-f index_freqs_file] [-t index_file_type] [-c corpus_id_to_doc_id_file]\n"
           "\t[-w server_port] [-q query_type] [-n n_results] [-l snippet_len] [-m cache_size]\n"
           "Options:\n"
           "\t-d\tdataset file, default: fulldocs-new.trec\n"
           "\t-p\tdoc info (page table) file, default: docs.txt\n"
           "\t-s\tstorage info (lexicon) file, default: storage_vbyte.txt\n"
           "\t-i\tindex ids file, default: merged_index.vbyte\n"
           "\t-f\tindex freqs file, default: freqs.vbyte\n"
           "\t-t\tindex file type (bin|vbyte), default: vbyte\n"
           "\t-c\tcorpus id to doc id file, default: corpus_id_to_doc_id.txt\n"
           "\t-w\tserver port ([0, 65535] for web, others for cli), default: 8080\n"
           "\t-q\tquery type for cli (conjunctive|disjunctive|semantic|reranking), default: semantic\n"
           "\t-n\tnumber of results, default: 10\n"
           "\t-l\tsnippet length, default: 200\n"
           "\t-m\tcache size, default: 1000\n"
           "\t-h\thelp\n", program_name);
}

enum class QueryType {
    CONJUNCTIVE, DISJUNCTIVE, SEMANTIC, RERANKING
};

struct Options {
    int server_port = 8080;
    const char *dataset_path = "fulldocs-new.trec";
    const char *doc_info_path = "docs.txt";
    const char *storage_path = "storage_vbyte.txt";
    const char *index_ids_path = "merged_index.vbyte";
    const char *index_freqs_path = "freqs.vbyte";
    const char *corpus_id_to_doc_id_path = "corpus_id_to_doc_id.txt";
    // func pointer for read_index
    decltype(read_index_vbyte) *read_index = read_index_vbyte;
    int total_doc_cnt = 3213835;
    double avg_doc_len = 1172.448644;
    double k = 0.9, b = 0.4;
    QueryType query_type = QueryType::SEMANTIC;
    int n_results = 10;
    int snippet_len = 200;
    int cache_size = 1000;
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
            if (strcmp(option, "-d") == 0) options.dataset_path = value;
            else if (strcmp(option, "-p") == 0) options.doc_info_path = value;
            else if (strcmp(option, "-s") == 0) options.storage_path = value;
            else if (strcmp(option, "-i") == 0) options.index_ids_path = value;
            else if (strcmp(option, "-f") == 0) options.index_freqs_path = value;
            else if (strcmp(option, "-c") == 0) options.corpus_id_to_doc_id_path = value;
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
            } else if (strcmp(option, "-w") == 0) {
                options.server_port = atoi(value);
                if (options.server_port <= 0 || options.server_port > 65535) {
                    printf("Entering cli mode...\n");
                }
            } else if (strcmp(option, "-q") == 0) {
                // if "conjunctive" startswith value
                if (strncmp(value, "conjunctive", strlen(value)) == 0) {
                    options.query_type = QueryType::CONJUNCTIVE;
                } else if (strncmp(value, "disjunctive", strlen(value)) == 0) {
                    options.query_type = QueryType::DISJUNCTIVE;
                } else if (strncmp(value, "semantic", strlen(value)) == 0) {
                    options.query_type = QueryType::SEMANTIC;
                } else if (strncmp(value, "reranking", strlen(value)) == 0) {
                    options.query_type = QueryType::RERANKING;
                } else {
                    cerr << "Invalid value for option -q: " << value << endl;
                    print_usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
            } else if (strcmp(option, "-l") == 0) {
                options.snippet_len = atoi(value);
                if (options.snippet_len <= 0) {
                    cerr << "Invalid value for option -l: " << value << endl;
                    print_usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
            } else if (strcmp(option, "-m") == 0) {
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

void *realloc_guarded(void *ptr, size_t size) {
    void *tmp = realloc(ptr, size);
    if (tmp == nullptr) {
        perror("Failed to allocate memory");
        exit(EXIT_FAILURE);
    }
    return tmp;
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
        char *word_begin = begin_p;
        int len = get_utf8_char_len(begin_p, end_p);
        while (is_al_num(begin_p, len)) {
            if (len == 1) {
                *begin_p = (char) tolower(*begin_p);
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
    for (const auto &word: query_list) {
        cleaned_query += word + " ";
    }
    if (!cleaned_query.empty()) {
        cleaned_query.pop_back();
    }
    return cleaned_query;
}

size_t read_doc(unsigned doc_id) {
    fseek64(dataset_fp, docs_info[doc_id].begin, SEEK_SET);
    size_t new_size = docs_info[doc_id].end - docs_info[doc_id].begin;
    if (new_size + 1 > buf_size) {
        buf_size = new_size;
        doc_content = (char *) realloc_guarded(doc_content, buf_size + 1);
        tokenize_buffer = (char *) realloc_guarded(tokenize_buffer, buf_size + 1);
    }
    fread(doc_content, sizeof(char), new_size, dataset_fp);
    doc_content[new_size] = '\0';
    return new_size;
}

double BM25(unsigned freq, unsigned doc_cnt, unsigned doc_len, Options &options) {
    double K = options.k * ((1 - options.b) + options.b * doc_len / options.avg_doc_len);
    return log((options.total_doc_cnt - doc_cnt + 0.5) / (doc_cnt + 0.5)) *
           (freq * (options.k + 1)) / (freq + K);
}

class Searcher {
protected:
    LRUCache<ResultDocInfos> result_cache;

    virtual shared_ptr<ResultDocInfos>
    collect_and_rank_docs(const string &query, const string &cleaned_query, vector<string> &query_list,
                          LRUCache<Entry> &entry_cache, Options &options, json &result) = 0;

public:
    explicit Searcher(int cache_size) : result_cache(cache_size) {}

    virtual ~Searcher() = default;

    json search(const string &query, Options &options) {
        static LRUCache<Entry> entry_cache(options.cache_size);
        static vector<char *> words;
        static vector<string> query_list;

        // Clean query
        query_list.clear();
        const string &cleaned_query = clean_query(query, query_list);

        // Collect and rank docs
        json result;
        auto start = std::chrono::steady_clock::now();
        auto sorted_infos = collect_and_rank_docs(query, cleaned_query, query_list, entry_cache, options, result);

        // Generate results
        auto stop = std::chrono::steady_clock::now();
        auto diff = std::chrono::duration<double, std::micro>(stop - start).count();
        result["time"] = diff;
        if (!sorted_infos) {
            result["count"] = 0;
            return result;
        }
        result["count"] = sorted_infos->size();
        result["data"] = json::array();
        for (int i = 0; i < options.n_results && i < sorted_infos->size(); i++) {
            auto &[doc_id, info] = sorted_infos->at(i);
            json item;
            item["rank"] = i + 1;
            item["score"] = info.score;
            if (info.freqs) {  // transformer does not have freqs
                item["freqs"] = *info.freqs;
            }
            item["url"] = docs_info[doc_id].url;
            size_t size = read_doc(doc_id);
            memcpy(tokenize_buffer, doc_content, size + 1);
            words.clear();
            words.reserve(docs_info[doc_id].term_cnt);
            char *begin_p = tokenize_buffer;
            char *end_p = tokenize_buffer + size;
            while (begin_p < end_p) {
                char *word_begin = begin_p;
                int len = get_utf8_char_len(begin_p, end_p);
                while (is_al_num(begin_p, len)) {
                    if (len == 1) {
                        *begin_p = (char) tolower(*begin_p);
                    }
                    begin_p += len;
                    len = get_utf8_char_len(begin_p, end_p);
                }
                if (begin_p > word_begin) {
                    *begin_p = '\0';
                    words.push_back(word_begin);
                }
                begin_p += len;
            }
            // Find first occurrence of each query word
            for (const auto &term: query_list) {
                char *q_pos = nullptr;
                for (const auto word: words) {
                    if (strcmp(word, term.c_str()) == 0) {
                        q_pos = word;
                        break;
                    }
                }
                if (q_pos != nullptr) {
                    // Form snippet
                    long long begin_pos = max((long long) (q_pos - tokenize_buffer) - options.snippet_len / 2, 0LL);
                    while (begin_pos >= 0 && (doc_content[begin_pos] & 0x80) != 0) {  // keep complete utf-8 characters
                        begin_pos--;
                    }
                    size_t end_pos = min((long long) (q_pos - tokenize_buffer) + options.snippet_len / 2, (long long) size);
                    size_t original_end_pos = end_pos;
                    while (end_pos < size && (doc_content[end_pos] & 0x80) != 0) {  // keep complete utf-8 characters
                        end_pos++;
                    }
                    // include the last byte if the last character is an utf-8 character
                    end_pos += (end_pos + 1 < size && end_pos != original_end_pos);
                    doc_content[end_pos] = '\0';
                    item["snippet"] = doc_content + begin_pos;
                    result["data"].push_back(item);
                    break;
                }
            }
        }
        if (result["data"].empty()) {
            result["count"] = 0;
        }
        return result;
    }
};

class ConjunctiveSearcher : public Searcher {
public:
    explicit ConjunctiveSearcher(int cache_size) : Searcher(cache_size) {}

private:
    shared_ptr<ResultDocInfos>
    collect_and_rank_docs(const string &, const string &cleaned_query, vector<string> &query_list,
                          LRUCache<Entry> &entry_cache, Options &options, json &result) override {
        // Check if query is in cache
        auto sorted_infos = result_cache.get(cleaned_query);
        if (sorted_infos) {
            result["cached"] = true;
            return sorted_infos;
        }
        result["cached"] = false;

        // Search query in storage_info
        entries.clear();
        for (const auto &term: query_list) {
            if (storage_info.find(term) != storage_info.end()) {
                // Check if entry is in cache
                auto entry = entry_cache.get(term);
                if (!entry) {
                    entry = make_shared<Entry>();
                    entry->term = term;
                    entry->doc_ids.reserve(storage_info[term].doc_cnt);
                    entry->freqs.reserve(storage_info[term].doc_cnt);
                    // Read entry from file
                    options.read_index(storage_info[term].ids_begin,
                                       storage_info[term].freqs_begin,
                                       storage_info[term].doc_cnt, entry);
                    // Cache entry
                    entry_cache.put(term, entry);
                }
                entries.push_back(entry);
            }
        }
        if (entries.empty()) {
            return sorted_infos;
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
        for (const auto &entry: entries) {
            for (auto doc_id: last_intersection) {
                auto it = lower_bound(entry->doc_ids.begin(), entry->doc_ids.end(), doc_id);
                if (it != entry->doc_ids.end() && *it == doc_id) {
                    auto freq = entry->freqs[it - entry->doc_ids.begin()];
                    infos[doc_id].score += BM25(freq, (unsigned) entry->doc_ids.size(),
                                                docs_info[doc_id].term_cnt, options);
                    infos[doc_id].freqs->emplace_back(entry->term, freq);
                }
            }
        }

        // Sort by score
        sorted_infos = make_shared<ResultDocInfos>();
        sorted_infos->reserve(infos.size());
        for (const auto &info: infos) {
            sorted_infos->emplace_back(info);
        }
        sort(sorted_infos->begin(), sorted_infos->end(),
             [](const pair<unsigned, ResultDocInfo> &lhs, const pair<unsigned, ResultDocInfo> &rhs) {
                 return lhs.second.score > rhs.second.score ||
                        (lhs.second.score == rhs.second.score && lhs.first < rhs.first);
             });

        // Cache result
        result_cache.put(cleaned_query, sorted_infos);
        return sorted_infos;
    }
};

class DisjunctiveSearcher : public Searcher {
public:
    explicit DisjunctiveSearcher(int cache_size) : Searcher(cache_size) {}

private:
    shared_ptr<ResultDocInfos>
    collect_and_rank_docs(const string &, const string &cleaned_query, vector<string> &query_list,
                          LRUCache<Entry> &entry_cache, Options &options, json &result) override {
        // Check if query is in cache
        auto sorted_infos = result_cache.get(cleaned_query);
        if (sorted_infos) {
            result["cached"] = true;
            return sorted_infos;
        }
        result["cached"] = false;

        // Search query in storage_info
        entries.clear();
        for (const auto &term: query_list) {
            if (storage_info.find(term) != storage_info.end()) {
                // Check if entry is in cache
                auto entry = entry_cache.get(term);
                if (!entry) {
                    entry = make_shared<Entry>();
                    entry->term = term;
                    entry->doc_ids.reserve(storage_info[term].doc_cnt);
                    entry->freqs.reserve(storage_info[term].doc_cnt);
                    // Read entry from file
                    options.read_index(storage_info[term].ids_begin,
                                       storage_info[term].freqs_begin,
                                       storage_info[term].doc_cnt, entry);
                    // Cache entry
                    entry_cache.put(term, entry);
                }
                entries.push_back(entry);
            }
        }
        if (entries.empty()) {
            return sorted_infos;
        }

        // Calculate infos and sort by score
        infos.clear();
        for (const auto &entry: entries) {
            for (int i = 0; i < entry->doc_ids.size(); i++) {
                unsigned doc_id = entry->doc_ids[i];
                infos[doc_id].score += BM25(entry->freqs[i], (unsigned) entry->doc_ids.size(), docs_info[doc_id].term_cnt, options);
                infos[doc_id].freqs->emplace_back(entry->term, entry->freqs[i]);
            }
        }

        // Sort by score
        sorted_infos = make_shared<ResultDocInfos>();
        sorted_infos->reserve(infos.size());
        for (const auto &info: infos) {
            sorted_infos->emplace_back(info);
        }
        sort(sorted_infos->begin(), sorted_infos->end(),
             [](const pair<unsigned, ResultDocInfo> &lhs, const pair<unsigned, ResultDocInfo> &rhs) {
                 return lhs.second.score > rhs.second.score ||
                        (lhs.second.score == rhs.second.score && lhs.first < rhs.first);
             });

        // Cache result
        result_cache.put(cleaned_query, sorted_infos);
        return sorted_infos;
    }
};

class TransformerSearcher : public Searcher {
    PyObject *pModule, *pfnSemanticSearch, *pfnRerank;
    LRUCache<ResultDocInfos> reranking_result_cache;
    vector<unsigned> doc_ids;

public:
    explicit TransformerSearcher(Options &options) : Searcher(options.cache_size), reranking_result_cache(options.cache_size) {
        FILE *fp = fopen_guarded(options.corpus_id_to_doc_id_path, "r");
        unsigned corpus_id = 0, doc_id;
        while (fscanf(fp, "%u", &doc_id) != EOF) {
            doc_ids.push_back(doc_id);
            corpus_id++;
        }
        printf("Initializing Python for Transformer...");
        fflush(stdout);
        Py_Initialize();
        if (!Py_IsInitialized()) {
            PyErr_Print();
            exit(EXIT_FAILURE);
        }
        PyRun_SimpleString("import sys; sys.path.append('.')");
        pModule = PyImport_ImportModule("learning_to_rank");
        if (!pModule) {
            PyErr_Print();
            exit(EXIT_FAILURE);
        }
        pfnSemanticSearch = PyObject_GetAttrString(pModule, "semantic_search");
        if (!pfnSemanticSearch) {
            PyErr_Print();
            exit(EXIT_FAILURE);
        }
        pfnRerank = PyObject_GetAttrString(pModule, "rerank_inplace");
        if (!pfnRerank) {
            PyErr_Print();
            exit(EXIT_FAILURE);
        }
        printf("done\n");
    }

    ~TransformerSearcher() override {
        Py_DECREF(pModule);
        Py_DECREF(pfnSemanticSearch);
        Py_DECREF(pfnRerank);
        Py_Finalize();
    }

private:
    shared_ptr<ResultDocInfos>
    collect_and_rank_docs(const string &query, const string &, vector<string> &,
                          LRUCache<Entry> &, Options &options, json &result) override {
        // Check if query is in cache
        shared_ptr<ResultDocInfos> sorted_infos;
        if (options.query_type == QueryType::RERANKING) {
            sorted_infos = reranking_result_cache.get(query);
        } else {
            sorted_infos = result_cache.get(query);
        }
        if (sorted_infos) {
            result["cached"] = true;
            return sorted_infos;
        }
        result["cached"] = false;

        Py_BEGIN_ALLOW_THREADS
        auto gstate = PyGILState_Ensure();
        // Get semantic search results
        PyObject *pArgs = PyTuple_New(1);  // new reference
        PyObject *pQuery = PyUnicode_FromString(query.c_str());  // new reference, but will be borrowed by pArgs
        PyTuple_SetItem(pArgs, 0, pQuery);  // steals reference, pQuery will be DECREFed when pArgs is DECREFed
        Py_INCREF(pQuery);
        PyObject *pResults = PyObject_CallObject(pfnSemanticSearch, pArgs);  // new reference, but may be borrowed by pArgs
        if (!pResults) {
            PyErr_Print();
            exit(EXIT_FAILURE);
        }
        Py_DECREF(pArgs);  // also DECREFs pQuery

        auto size = PyList_Size(pResults);
        if (size > 0 && options.query_type == QueryType::RERANKING) {
            // Get docs according to doc ids
            PyObject *pQueryDocPairs = PyList_New(size);  // new reference, but will be borrowed by pArgs
            for (Py_ssize_t i = 0; i < size; i++) {
                PyObject *pResult = PyList_GetItem(pResults, i);  // borrowed reference
                PyObject *pCorpusId = PyDict_GetItemString(pResult, "corpus_id");  // borrowed reference
                read_doc(doc_ids[PyLong_AsUnsignedLong(pCorpusId)]);
                PyObject *pDoc = PyUnicode_FromString(doc_content);  // new reference, but will be borrowed by pQueryDocPair

                PyObject *pQueryDocPair = PyList_New(2);  // new reference, but will be borrowed by pQueryDocPairs
                PyList_SetItem(pQueryDocPair, 0, pQuery);  // steals reference, pQuery will be DECREFed when pQueryDocPair is DECREFed
                Py_INCREF(pQuery);
                PyList_SetItem(pQueryDocPair, 1, pDoc);  // steals reference, pDoc will be DECREFed when pQueryDocPair is DECREFed
                PyList_SetItem(pQueryDocPairs, i, pQueryDocPair);  // steals reference, pQueryDocPair will be DECREFed when pDocs is DECREFed
            }
            Py_DECREF(pQuery);  // pQuery will be DECREFed when all pQueryDocPairs are DECREFed

            // Get reranked results
            pArgs = PyTuple_New(2);  // new reference
            PyTuple_SetItem(pArgs, 0, pQueryDocPairs);  // steals reference, pQueryDocPairs will be DECREFed when pArgs is DECREFed
            PyTuple_SetItem(pArgs, 1, pResults);  // steals reference, pResults will be DECREFed when pArgs is DECREFed
            PyObject *pNone = PyObject_CallObject(pfnRerank, pArgs);  // new reference
            if (!pNone) {
                PyErr_Print();
                exit(EXIT_FAILURE);
            }
            Py_DECREF(pNone);
        }

        if (size > 0) {
            sorted_infos = make_shared<ResultDocInfos>();
            sorted_infos->reserve(size);
            for (Py_ssize_t i = 0; i < size; i++) {
                PyObject *pResult = PyList_GetItem(pResults, i);  // borrowed reference
                PyObject *pCorpusId = PyDict_GetItemString(pResult, "corpus_id");  // borrowed reference
                PyObject *pScore = PyDict_GetItemString(pResult, "score");  // borrowed reference
                sorted_infos->emplace_back(doc_ids[PyLong_AsUnsignedLong(pCorpusId)],
                                           ResultDocInfo{PyFloat_AsDouble(pScore), nullptr});
            }
            if (options.query_type == QueryType::RERANKING) {
                Py_DECREF(pArgs);  // also DECREFs pQuery, pDocs, and pResults
            }
        } else {
            Py_DECREF(pQuery);
            Py_DECREF(pResults);
        }

        // Cache result
        if (options.query_type == QueryType::RERANKING) {
            reranking_result_cache.put(query, sorted_infos);
        } else {
            result_cache.put(query, sorted_infos);
        }

        PyGILState_Release(gstate);
        Py_END_ALLOW_THREADS
        return sorted_infos;
    }
};

void report_error(const char *msg, httplib::Response &res) {
    json response;
    response["message"] = msg;
    string json_str = response.dump();
    res.status = 400;
    res.set_content(json_str, "application/json");
}

// exit when Ctrl+C is pressed
void clean_up(int signal) {
    printf("\nCleaning up...\n");
    fflush(stdout);
    if (svr.is_running()) {
        svr.stop();
    }
    if (ids_fp != nullptr) {
        fclose(ids_fp);
    }
    if (freqs_fp != nullptr) {
        fclose(freqs_fp);
    }
    if (dataset_fp != nullptr) {
        fclose(dataset_fp);
    }
    if (doc_content != nullptr) {
        free(doc_content);
    }
    if (tokenize_buffer != nullptr) {
        free(tokenize_buffer);
    }
    if (home_page_buffer != nullptr) {
        free(home_page_buffer);
    }
    delete searcher;
    exit(EXIT_SUCCESS);
}

int main(int argc, char *argv[]) {
    signal(SIGINT, clean_up);
    Options options = parse_args(argc, argv);
    read_storage_info(options.storage_path);
    read_docs_info(options);
    ids_fp = fopen_guarded(options.index_ids_path, "rb");
    freqs_fp = fopen_guarded(options.index_freqs_path, "rb");
    dataset_fp = fopen_guarded(options.dataset_path, "r");
    doc_content = (char *) malloc(buf_size + 1);
    tokenize_buffer = (char *) malloc(buf_size + 1);

    if (options.server_port >= 0 && options.server_port <= 65535) {
        ConjunctiveSearcher conjunctive_searcher(options.cache_size);
        DisjunctiveSearcher disjunctive_searcher(options.cache_size);
        TransformerSearcher transformer_searcher(options);

        FILE *fp = fopen_guarded("index.html", "rb");
        fseek(fp, 0, SEEK_END);
        long long size = ftell64(fp);
        fseek(fp, 0, SEEK_SET);
        home_page_buffer = (char *) malloc(size + 1);
        fread(home_page_buffer, sizeof(char), size, fp);
        home_page_buffer[size] = '\0';
        fclose(fp);
        svr.Get("/", [](const httplib::Request &req, httplib::Response &res) {
            res.set_content(home_page_buffer, "text/html");
        });
        svr.Post("/", [&](const httplib::Request &req, httplib::Response &res) {
            try {
                json post = json::parse(req.body);
                const string &query = post["query"].get<string>();
                int snippet_len = post["snippet_len"].get<int>();
                if (snippet_len <= 0) {
                    report_error("Invalid value for snippet_len", res);
                    return;
                }
                options.snippet_len = snippet_len;
                int n_results = post["n_results"].get<int>();
                if (n_results <= 0) {
                    report_error("Invalid value for n_results", res);
                    return;
                }
                options.n_results = n_results;
                options.query_type = (QueryType) post["query_type"].get<int>();
                json result;
                switch (options.query_type) {
                    case QueryType::CONJUNCTIVE:
                        result = conjunctive_searcher.search(query, options);
                        break;
                    case QueryType::DISJUNCTIVE:
                        result = disjunctive_searcher.search(query, options);
                        break;
                    default:
                        result = transformer_searcher.search(query, options);
                        break;
                }
                string json_str = result.dump(/*-1, ' ', false, json::error_handler_t::ignore*/);
                res.set_content(json_str, "application/json");
            } catch (std::exception &e) {
                report_error(e.what(), res);
            }
        });
        printf("Server is running on http://localhost:%d\n", options.server_port);
        svr.listen("localhost", options.server_port);
    } else {
        switch (options.query_type) {
            case QueryType::CONJUNCTIVE:
                searcher = new ConjunctiveSearcher(options.cache_size);
                break;
            case QueryType::DISJUNCTIVE:
                searcher = new DisjunctiveSearcher(options.cache_size);
                break;
            default:
                searcher = new TransformerSearcher(options);
                break;
        }
        printf("query> ");
        string query;
        while (getline(cin, query)) {
            json result = searcher->search(query, options);
            if (result["count"] == 0) {
                printf("\nNo results found. Checked in %.2f microseconds.\n\n\n",
                       result["time"].get<double>());
            } else {
                if (result["cached"]) {
                    if (options.query_type >= QueryType::SEMANTIC) {
                        printf("\nFound the following results from cache in %.2f microseconds.\n\n\n",
                               result["time"].get<double>());
                    } else {
                        printf("\nFound %lld results from cache in %.2f microseconds.\n\n\n",
                               result["count"].get<long long>(), result["time"].get<double>());
                    }
                } else {
                    if (options.query_type >= QueryType::SEMANTIC) {
                        printf("\nFound the following results in %.2f milliseconds.\n\n\n",
                               result["time"].get<double>() / 1000.);
                    } else {
                        printf("\nFound %lld results in %.2f milliseconds.\n\n\n",
                               result["count"].get<long long>(), result["time"].get<double>() / 1000.);
                    }
                }
                for (const auto &item: result["data"]) {
                    printf("%d. [%.2f] ", item["rank"].get<int>(), item["score"].get<double>());
                    if (options.query_type < QueryType::SEMANTIC) {  // transformer does not have freqs
                        for (const auto &freq: item["freqs"]) {
                            printf("%s(%d) ", freq[0].get<string>().c_str(), freq[1].get<int>());
                        }
                    }
                    printf("\n\n%s\n\n...%s...\n\n\n", item["url"].get<string>().c_str(),
                           item["snippet"].get<string>().c_str());
                }
            }
            printf("query> ");
        }
    }
    clean_up(SIGINT);
    return 0;
}
