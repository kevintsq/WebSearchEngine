#include <iostream>
#include <cstring>
#include <fstream>
#include <filesystem>
#include <utility>
#include <vector>
#include <unordered_map>
#include <queue>
#include <algorithm>

using std::cout;
using std::cerr;
using std::endl;
using std::ifstream;
using std::istringstream;
using std::string;
using std::vector;
using std::pair;
using std::sort;
using std::unordered_map;
using std::priority_queue;
namespace fs = std::filesystem;

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

using Index = vector<Entry>;

struct StorageInfo {
    const string *term = nullptr;
    size_t ids_begin = 0, scores_begin = 0;
    unsigned doc_cnt = 0;
};

vector<StorageInfo> storage_info;

Index read_index_txt(ifstream &id_file, ifstream &freq_file, int n_entries) {
    Index index;
    string id_line, freq_line;
    string term;
    for (int i = 0; i < n_entries && getline(id_file, id_line) && getline(freq_file, freq_line); i++) {
        istringstream id_iss(id_line), freq_iss(freq_line);
        id_iss >> term;
        unsigned doc_id, freq;
        auto &entry = index.emplace_back(term, vector<unsigned>(), vector<unsigned>());
        while (id_iss >> doc_id && freq_iss >> freq) {
            entry.doc_ids.push_back(doc_id);
            entry.freqs.push_back(freq);
        }
    }
    return index;
}

Index read_index_bin(ifstream &id_file, ifstream &freq_file, int n_entries) {
    Index index;
    string term;
    for (int i = 0; i < n_entries; i++) {
        id_file >> term;
        if (term.empty()) {
            break;
        }
        id_file.seekg(1, ifstream::cur);  // skip the space
        unsigned ids_size, freqs_size;
        id_file.read((char *) &ids_size, sizeof(unsigned));
        freq_file.read((char *) &freqs_size, sizeof(unsigned));
        if (ids_size != freqs_size) {
            fprintf(stderr, "ids_size != freqs_size");
            exit(EXIT_FAILURE);
        }
        Entry &entry = index.emplace_back(term, vector<unsigned>(ids_size), vector<unsigned>(freqs_size));
        id_file.read((char *) entry.doc_ids.data(), (long long) sizeof(unsigned) * ids_size);
        freq_file.read((char *) entry.freqs.data(), (long long) sizeof(unsigned) * freqs_size);
    }
    return index;
}

void read_uints_vbyte(ifstream &file, vector<unsigned> &uints, unsigned count) {
    uints.clear();
    uints.reserve(count);
    unsigned value = 0;
    unsigned shift = 0;
    unsigned i = 0;
    while (i < count) {
        unsigned char byte;
        file.read((char *) &byte, sizeof(unsigned char));
        value |= (byte & 0x7f) << shift;
        if (byte & 0x80) {
            uints.push_back(value);
            value = 0;
            shift = 0;
            i++;
        } else {
            shift += 7;
        }
    }
}

Index read_index_vbyte(ifstream &id_file, ifstream &freq_file, int n_entries) {
    Index index;
    string term;
    for (int i = 0; i < n_entries; i++) {
        id_file >> term;
        if (term.empty()) {
            break;
        }
        id_file.seekg(1, ifstream::cur);  // skip the space
        unsigned ids_size, freqs_size;
        id_file.read((char *) &ids_size, sizeof(unsigned));
        freq_file.read((char *) &freqs_size, sizeof(unsigned));
        if (ids_size != freqs_size) {
            fprintf(stderr, "ids_size != freqs_size");
            exit(EXIT_FAILURE);
        }
        Entry &entry = index.emplace_back(term, vector<unsigned>(), vector<unsigned>());
        read_uints_vbyte(id_file, entry.doc_ids, ids_size);
        read_uints_vbyte(freq_file, entry.freqs, ids_size);
    }
    return index;
}

struct Options;

void dump_index_vbyte(const Index &index, FILE *ids_fp, FILE *freqs_fp, Options &options);

void dump_index_bin(const Index &index, FILE *ids_fp, FILE *freqs_fp, Options &options);

struct Options {
    const char *index_path = "index";
    string storage_path = ".";
    string merged_index_path = ".";
    string input_index_type = "vbyte";
    // func pointer for read_index
    decltype(read_index_vbyte) *read_index = read_index_vbyte;
    const char *merged_index_type = "vbyte";
    // func pointer for dump_index
    decltype(dump_index_vbyte) *dump_index = dump_index_vbyte;
    bool store_diff = true;
    int input_index_chunk_size = 8192;
    int output_entry_size = 131072;
};

void log() {
    static int cnt = 1;
    static auto start = std::chrono::steady_clock::now();
    auto end = std::chrono::steady_clock::now();
    cout << "processing chunk " << cnt++ << ", time used " << std::chrono::duration_cast<std::chrono::seconds>(
            end - start).count() << "s" << endl;
    start = end;
}

void dump_index_txt(const Index &index, FILE *ids_fp, FILE *freqs_fp, Options &options) {
    log();
    for (auto &p: index) {
        fprintf(ids_fp, "%s", p.term.c_str());
        fprintf(freqs_fp, "%s", p.term.c_str());
        // use pointer to avoid copy
        auto &info = storage_info.emplace_back(&p.term, ftell64(ids_fp), ftell64(freqs_fp),
                                               (unsigned) p.doc_ids.size());
        unsigned last_doc_id = 0;
        for (unsigned i = 0; i < info.doc_cnt; i++) {
            unsigned doc_id = p.doc_ids[i];
            if (options.store_diff) {
                fprintf(ids_fp, " %u", doc_id - last_doc_id);
                last_doc_id = doc_id;
            } else {
                fprintf(ids_fp, " %u", doc_id);
            }
            fprintf(freqs_fp, " %u", p.freqs[i]);
        }
        fprintf(ids_fp, "\n");
        fprintf(freqs_fp, "\n");
    }
}

void dump_index_bin(const Index &index, FILE *ids_fp, FILE *freqs_fp, Options &options) {
    log();
    for (auto &p: index) {
        // use pointer to avoid copy
        auto &info = storage_info.emplace_back(&p.term, ftell64(ids_fp), ftell64(freqs_fp),
                                               (unsigned) p.doc_ids.size());
        unsigned last_doc_id = 0;
        for (auto &doc_id: p.doc_ids) {
            if (options.store_diff) {
                unsigned diff = doc_id - last_doc_id;
                fwrite(&diff, sizeof(unsigned), 1, ids_fp);
                last_doc_id = doc_id;
            } else {
                fwrite(&doc_id, sizeof(unsigned), 1, ids_fp);
            }
        }
        fwrite(p.freqs.data(), sizeof(unsigned), info.doc_cnt, freqs_fp);
    }
}

void dump_index_vbyte(const Index &index, FILE *ids_fp, FILE *freqs_fp, Options &options) {
    log();
    for (auto &p: index) {
        // use pointer to avoid copy
        storage_info.emplace_back(&p.term, ftell64(ids_fp), ftell64(freqs_fp),
                                  (unsigned) p.doc_ids.size());
        unsigned last_doc_id = 0;
        for (auto &doc_id: p.doc_ids) {
            unsigned value = doc_id;
            if (options.store_diff) {
                value -= last_doc_id;
                last_doc_id = doc_id;
            }
            unsigned char byte;
            while (value >= 128) {
                byte = value & 127;
                fwrite(&byte, sizeof(unsigned char), 1, ids_fp);
                value >>= 7;
            }
            byte = value | 128;
            fwrite(&byte, sizeof(unsigned char), 1, ids_fp);
        }
        for (auto &freq: p.freqs) {
            unsigned value = freq;
            unsigned char byte;
            while (value >= 128) {
                byte = value & 127;
                fwrite(&byte, sizeof(unsigned char), 1, freqs_fp);
                value >>= 7;
            }
            byte = value | 128;
            fwrite(&byte, sizeof(unsigned char), 1, freqs_fp);
        }
    }
}

void dump_storage_txt(FILE *fp) {
    for (auto &info: storage_info) {
        fprintf(fp, "%s %zu %zu %u\n", info.term->c_str(), info.ids_begin, info.scores_begin, info.doc_cnt);
    }
    storage_info.clear();
}

void print_usage(char *program_name) {
    printf("Usage: %s [-h] [-i index_path] [-s storage_path] [-o merged_index_path]\n"
           "\t[-t input_index_type] [-m merged_index_type] [-d store_diff]\n"
           "\t[-c input_index_chunk_size] [-e output_entry_size]\n"
           "Options:\n"
           "\t-i\tindex path, default: index\n"
           "\t-s\tstorage info (lexicon) path, default: .\n"
           "\t-o\tmerged index path, default: .\n"
           "\t-t\tinput index type (txt|bin|vbyte), default: vbyte\n"
           "\t-m\tmerged index type (txt|bin|vbyte), default: vbyte\n"
           "\t-d\tstore diff docIDs in the merged index (true|false), default: true\n"
           "\t-c\tinput index chunk size, default: 8192\n"
           "\t-e\toutput entry size, default: 131072\n"
           "\t-h\thelp", program_name);
}

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
            if (strcmp(option, "-i") == 0) options.index_path = value;
            else if (strcmp(option, "-s") == 0) options.storage_path = value;
            else if (strcmp(option, "-o") == 0) options.merged_index_path = value;
            else if (strcmp(option, "-t") == 0) {
                if (strcmp(value, "txt") == 0) {
                    options.input_index_type = value;
                    options.read_index = read_index_txt;
                } else if (strcmp(value, "bin") == 0) {
                    options.input_index_type = value;
                    options.read_index = read_index_bin;
                } else if (strcmp(value, "vbyte") == 0) {
                    options.input_index_type = value;
                    options.read_index = read_index_vbyte;
                } else {
                    cerr << "Invalid input index type: " << value << endl;
                    print_usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
            } else if (strcmp(option, "-m") == 0) {
                if (strcmp(value, "txt") == 0) {
                    options.merged_index_type = value;
                    options.dump_index = dump_index_txt;
                } else if (strcmp(value, "bin") == 0) {
                    options.merged_index_type = value;
                    options.dump_index = dump_index_bin;
                } else if (strcmp(value, "vbyte") == 0) {
                    options.merged_index_type = value;
                    options.dump_index = dump_index_vbyte;
                } else {
                    cerr << "Invalid merged index type: " << value << endl;
                    print_usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
            } else if (strcmp(option, "-d") == 0) {
                if (strcmp(value, "true") == 0) {
                    options.store_diff = true;
                } else if (strcmp(value, "false") == 0) {
                    options.store_diff = false;
                } else {
                    cerr << "Invalid store diff: " << value << endl;
                    print_usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
            } else if (strcmp(option, "-c") == 0) {
                options.input_index_chunk_size = atoi(value);
                if (options.input_index_chunk_size <= 0) {
                    cerr << "Invalid input index chunk size: " << value << endl;
                    print_usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
            } else if (strcmp(option, "-e") == 0) {
                options.output_entry_size = atoi(value);
                if (options.output_entry_size <= 0) {
                    cerr << "Invalid output entry size: " << value << endl;
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

int main(int argc, char *argv[]) {
    auto start = std::chrono::steady_clock::now();

    Options options = parse_args(argc, argv);

    vector<string> id_filenames, freq_filenames;
    if (!fs::exists(options.index_path) || !fs::is_directory(options.index_path)) {
        perror("Failed to open index directory");
        exit(EXIT_FAILURE);
    }
    for (const auto &entry: fs::directory_iterator(options.index_path)) {
        string path = entry.path().string();
        if (path.ends_with("freqs." + options.input_index_type)) {
            freq_filenames.push_back(path);
        } else if (path.ends_with(options.input_index_type)) {
            id_filenames.push_back(path);
        }
    }
    if (id_filenames.empty() || freq_filenames.empty()) {
        perror("Failed to find index files");
        exit(EXIT_FAILURE);
    }
    if (id_filenames.size() != freq_filenames.size()) {
        perror("The number of index files is not equal");
        exit(EXIT_FAILURE);
    }
    vector<ifstream> id_files, freq_files;
    for (const auto &id_file: id_filenames) {
        id_files.emplace_back(id_file, std::ios::binary);
        if (!id_files.back().is_open()) {
            perror(("Failed to open index id_file " + id_file).c_str());
            exit(EXIT_FAILURE);
        }
    }
    for (const auto &freq_file: freq_filenames) {
        freq_files.emplace_back(freq_file, std::ios::binary);
        if (!freq_files.back().is_open()) {
            perror(("Failed to open index freq_file " + freq_file).c_str());
            exit(EXIT_FAILURE);
        }
    }
    vector<Index> indices;
    for (int i = 0; i < id_files.size(); i++) {
        indices.emplace_back(options.read_index(id_files[i], freq_files[i], options.input_index_chunk_size));
    }
    auto cmp = [](const pair<Entry, int> &a, const pair<Entry, int> &b) {
        return a.first > b.first;
    };
    priority_queue<pair<Entry, int>, vector<pair<Entry, int>>, decltype(cmp)> pq(cmp);
    for (int i = 0; i < indices.size(); i++) {
        if (!indices[i].empty()) {
            pq.emplace(indices[i].front(), i);  // TODO: It is a copy. Is using a pointer better?
        }
    }
    vector<int> indices_pos(indices.size());
    if (!fs::exists(options.merged_index_path) || !fs::is_directory(options.merged_index_path)) {
        if (!fs::create_directories(options.merged_index_path)) {
            perror("Failed to create merged index directory");
            exit(EXIT_FAILURE);
        }
    }
    FILE *merged_index_fp = fopen((options.merged_index_path + "/merged_index." + options.merged_index_type).c_str(),
                                  "wb");
    if (merged_index_fp == nullptr) {
        perror("Failed to open merged index file");
        exit(EXIT_FAILURE);
    }
    FILE *scores_fp = fopen((options.merged_index_path + "/freqs." + options.merged_index_type).c_str(), "wb");
    if (scores_fp == nullptr) {
        perror("Failed to open freqs file");
        exit(EXIT_FAILURE);
    }
    Index merged_index;
    if (!fs::exists(options.storage_path) || !fs::is_directory(options.storage_path)) {
        if (!fs::create_directories(options.storage_path)) {
            perror("Failed to create storage directory");
            exit(EXIT_FAILURE);
        }
    }
    FILE *storage_fp = fopen((options.storage_path + "/storage_" + options.merged_index_type + ".txt").c_str(), "w");
    if (storage_fp == nullptr) {
        perror("Failed to open storage file");
        exit(EXIT_FAILURE);
    }
    while (!pq.empty()) {
        auto [min_entry, min_index] = pq.top();  // TODO: min_entry is a copy. Is using a pointer better?
        pq.pop();
        if (!merged_index.empty() && min_entry.term == merged_index.back().term) {
            merged_index.back().doc_ids.insert(merged_index.back().doc_ids.end(),
                                               min_entry.doc_ids.begin(),
                                               min_entry.doc_ids.end());
            merged_index.back().freqs.insert(merged_index.back().freqs.end(),
                                             min_entry.freqs.begin(),
                                             min_entry.freqs.end());
        } else {
            if (merged_index.size() > options.output_entry_size) {  // prevent potential duplicate
                auto last = merged_index.back();  // TODO: It is a copy. Is using a pointer better?
                merged_index.pop_back();
                options.dump_index(merged_index, merged_index_fp, scores_fp, options);
                dump_storage_txt(storage_fp);  // must be called before merged_index.clear()
                merged_index.clear();
                merged_index.push_back(last);  // TODO: It is a copy. Is using a pointer better?
            }
            merged_index.push_back(min_entry);  // TODO: min_entry is a copy. Is using a pointer better?
        }
        int i = min_index;
        indices_pos[i]++;
        // case 1: all finished
        if (indices[i].empty()) {
            continue;
        }
        // case 2: chunk finished, has new chunk
        if (indices_pos[i] >= indices[i].size()) {
            indices[i] = options.read_index(id_files[i], freq_files[i], options.input_index_chunk_size);
            indices_pos[i] = 0;
        }
        // case 3: chunk not finished
        // case 2 and 3 both needs push new entry
        if (!indices[i].empty()) {
            pq.emplace(indices[i][indices_pos[i]], i);
        }
        // case 4: chunk finished, no new chunk
        // nothing to do
    }

    dump_storage_txt(storage_fp);  // must be called before merged_index.clear()
    options.dump_index(merged_index, merged_index_fp, scores_fp, options);

    for (auto &file_stream: id_files) {
        file_stream.close();
    }
    for (auto &file_stream: freq_files) {
        file_stream.close();
    }
    fclose(merged_index_fp);
    fclose(storage_fp);
    auto end = std::chrono::steady_clock::now();
    cout << "total time used " << std::chrono::duration_cast<std::chrono::seconds>(
            end - start).count() << "s" << endl;
    return 0;
}
