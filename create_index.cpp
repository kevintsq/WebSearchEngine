#include <iostream>
#include <cstring>
#include <vector>
#include <unordered_map>
#include <algorithm>
#include <filesystem>
#include "zlib.h"

namespace fs = std::filesystem;
using std::cout;
using std::cerr;
using std::endl;
using std::string;
using std::stringstream;
using std::vector;
using std::pair;
using std::sort;
using std::unordered_map;

struct Entry {
    vector<unsigned> doc_ids, freqs;
};
using Index = unordered_map<string, Entry>;

FILE *dataset_fp;
gzFile zip_fp;
char *buffer;
int pos;
int end_pos;
long long offset;

struct DocInfo {
    string url;
    unsigned term_cnt = 0;
    long long begin = 0, end = 0;
};
vector<DocInfo> docs;
Index inv_index;

void log() {
    static int cnt = 1;
    static auto start = std::chrono::steady_clock::now();
    auto end = std::chrono::steady_clock::now();
    cout << "pos " << pos << ", processing chunk " << cnt++ << ", time used "
         << std::chrono::duration_cast<std::chrono::seconds>(end - start).count() << "s" << endl;
    start = end;
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

void dump_uints_txt(FILE *fp, const vector<unsigned> &uints) {
    for (auto &uint: uints) {
        fprintf(fp, "%u ", uint);
    }
    fprintf(fp, "\n");
}

void dump_uints_bin(FILE *fp, const vector<unsigned> &uints) {
    auto size = (unsigned) uints.size();
    fwrite(&size, sizeof(unsigned), 1, fp);
    fwrite(uints.data(), sizeof(unsigned), uints.size(), fp);
}

void dump_uints_vbyte(FILE *fp, const vector<unsigned> &uints) {
    static vector<unsigned char> bytes;
    bytes.clear();
    auto size = (unsigned) uints.size();
    fwrite(&size, sizeof(unsigned), 1, fp);
    for (auto uint: uints) {
        while (uint >= 128) {
            bytes.push_back(uint & 127);
            uint >>= 7;
        }
        bytes.push_back(uint | 128);
    }
    fwrite(bytes.data(), sizeof(unsigned char), bytes.size(), fp);
}

struct Options {
    bool read_zip = false;
    const char *dataset_file_path = "fulldocs-new.trec";
    const char *index_path = "index";
    string doc_info_path = ".";
    string index_type = "vbyte";
    // func pointer for dump_index
    decltype(dump_uints_vbyte) *dump_uints = dump_uints_vbyte;
    int input_buffer_size = 256 * 1024 * 1024;
    int output_entry_size = 1000000;
} options;

FILE *fopen_guarded(const string &filename, const char *mode) {
    FILE *fp = fopen(filename.c_str(), mode);
    if (fp == nullptr) {
        perror(("Failed to open file " + filename).c_str());
        exit(EXIT_FAILURE);
    }
    return fp;
}

void sort_and_dump_index(const char *path, int name_num) {
    vector<pair<const string, Entry> *> index_sorted;  // use pointer to avoid copying
    for (auto &entry: inv_index) {
        index_sorted.push_back(&entry);
    }
    sort(index_sorted.begin(), index_sorted.end(),
         [](const auto a, const auto b) {
             return a->first < b->first;
         });
    if (!fs::exists("index") || !fs::is_directory("index")) {
        if (!fs::create_directory("index")) {
            perror("Failed to create index directory");
            exit(EXIT_FAILURE);
        }
    }
    stringstream filename;
    // name_num should have 3 digits
    filename << path << "/" << std::setw(3) << std::setfill('0') << name_num;
    string filename_prefix = filename.str();
    FILE *ids_fp = fopen_guarded(filename_prefix + "." + options.index_type, "wb");
    FILE *freqs_fp = fopen_guarded(filename_prefix + "_freqs." + options.index_type, "wb");
    for (auto &entry: index_sorted) {
        fwrite(entry->first.c_str(), sizeof(char), entry->first.size(), ids_fp);
        char sep = ' ';
        fwrite(&sep, sizeof(char), 1, ids_fp);
        if (entry->second.doc_ids.size() != entry->second.freqs.size()) {
            fprintf(stderr, "doc_ids.size() != freqs.size()\n");
            exit(EXIT_FAILURE);
        }
        options.dump_uints(ids_fp, entry->second.doc_ids);
        options.dump_uints(freqs_fp, entry->second.freqs);
    }
    fclose(ids_fp);
    fclose(freqs_fp);
}

void dump_docs_info_txt(FILE *docs_info_fp) {
    for (auto &doc: docs) {
        fprintf(docs_info_fp, "%s %u %lld %lld\n", doc.url.c_str(), doc.term_cnt, doc.begin, doc.end);
    }
}

void fetch_more() {
    log();
    // copy the remaining part to the beginning of the buffer
    memcpy(buffer, buffer + pos, options.input_buffer_size - pos);
    pos = options.input_buffer_size - pos;
    int read;
    if (options.read_zip) {
        read = gzread(zip_fp, buffer + pos, options.input_buffer_size - pos);
    } else {
        read = (int) fread(buffer + pos, 1, options.input_buffer_size - pos, dataset_fp);
    }
    if (read <= 0) {
        fprintf(stderr, "unexpected EOF\n");
        exit(EXIT_FAILURE);
    }
    if (read < options.input_buffer_size - pos) {
        buffer[pos + read] = '\0';
    }
    end_pos = pos + read;
    pos = 0;
}

bool startswith(const char *keyword, int len) {
    if (pos + len >= options.input_buffer_size) {
        fetch_more();
    }
    if (strncmp(buffer + pos, keyword, len) == 0) {
        pos += len;
        offset += len;
        return true;
    } else {
        return false;
    }
}

char *next_char() {
    if (pos >= options.input_buffer_size) {
        fetch_more();
    }
    offset++;
    return &buffer[pos++];
}

int get_utf8_char_len() {
    if (pos + 3 >= options.input_buffer_size) {
        fetch_more();
    }
    if ((buffer[pos] & 0x80) == 0) { // 0xxxxxxx is 1 byte character
        return 1;
    } else if ((buffer[pos] & 0xE0) == 0xC0) { // 110xxxxx is 2 byte character
        if ((buffer[pos + 1] & 0xC0) == 0x80) {
            return 2;
        }
    } else if ((buffer[pos] & 0xF0) == 0xE0) { // 1110xxxx is 3 byte character
        if ((buffer[pos + 1] & 0xC0) == 0x80 && (buffer[pos + 2] & 0xC0) == 0x80) {
            return 3;
        }
    } else if ((buffer[pos] & 0xF8) == 0xF0) { // 11110xxx is 4 byte character
        if ((buffer[pos + 1] & 0xC0) == 0x80 && (buffer[pos + 2] & 0xC0) == 0x80 &&
            (buffer[pos + 3] & 0xC0) == 0x80) {
            return 4;
        }
    }
    fprintf(stderr, "invalid utf-8 character at pos %d\n", pos);
    exit(EXIT_FAILURE);
}

void print_usage(char *program_name) {
    printf("Usage: %s [-h] [-d dataset_file_path] [-i index_path] [-p doc_info_path]\n"
           "\t[-t index_type] [-b input_buffer_size] [-e output_entry_size]\n"
           "Options:\n"
           "\t-d\tdataset path, default: msmarco-docs.trec.gz\n"
           "\t-i\tindex path, default: index\n"
           "\t-p\tdoc info (page table) path, default: .\n"
           "\t-t\tindex type (txt|bin|vbyte), default: vbyte\n"
           "\t-b\tinput buffer size (unsigned int but must < 2GB, unit: bytes), default: 256MB\n"
           "\t-e\toutput entry size, default: 1000000\n"
           "\t-h\thelp\n", program_name);
}

void parse_args(int argc, char *argv[]) {
    for (int i = 1; i < argc; i += 2) {
        char *option = argv[i];
        if (strcmp(option, "-h") == 0) {
            print_usage(argv[0]);
            exit(EXIT_SUCCESS);
        }
        if (i + 1 < argc) {
            char *value = argv[i + 1];
            if (strcmp(option, "-d") == 0) options.dataset_file_path = value;
            else if (strcmp(option, "-i") == 0) options.index_path = value;
            else if (strcmp(option, "-p") == 0) options.doc_info_path = value;
            else if (strcmp(option, "-t") == 0) {
                if (strcmp(value, "txt") == 0) {
                    options.index_type = value;
                    options.dump_uints = dump_uints_txt;
                } else if (strcmp(value, "bin") == 0) {
                    options.index_type = value;
                    options.dump_uints = dump_uints_bin;
                } else if (strcmp(value, "vbyte") == 0) {
                    options.index_type = value;
                    options.dump_uints = dump_uints_vbyte;
                } else {
                    cerr << "Invalid index type: " << value << endl;
                    print_usage(argv[0]);
                    exit(EXIT_FAILURE);
                }
            } else if (strcmp(option, "-b") == 0) {
                options.input_buffer_size = atoi(value);
                if (options.input_buffer_size <= 0) {
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
}

int main(int argc, char *argv[]) {
    auto start = std::chrono::steady_clock::now();

    parse_args(argc, argv);
    buffer = new char[options.input_buffer_size];
    pos = options.input_buffer_size;

    // auto detect file type
    zip_fp = gzopen(options.dataset_file_path, "r");
    if (zip_fp == nullptr) {
        dataset_fp = fopen_guarded(options.dataset_file_path, "r");
    } else {
        options.read_zip = true;
    }

    FILE *docs_info_fp = fopen_guarded(options.doc_info_path + "/docs.txt", "w");

    char *next;
    string word;
    int index_file_cnt = 0;
    unsigned long long total_term_cnt = 0;
    unsigned doc_id;
    for (doc_id = 0;; doc_id++) {
        if (!startswith("<DOC>", 5) == 1) {
            fprintf(stderr, "unexpected format: <DOC> not found\n");
            exit(EXIT_FAILURE);
        }
        if (*next_char() != '\n') {
            fprintf(stderr, "warning: <DOC> not followed by newline\n");
            pos--;
            offset--;
        }
        if (!startswith("<DOCNO>", 7)) {
            fprintf(stderr, "unexpected format: <DOCNO> not found\n");
            exit(EXIT_FAILURE);
        }
        while (*next_char() != '<') {
        }
        pos--;
        offset--;
        if (!startswith("</DOCNO>", 8)) {
            fprintf(stderr, "unexpected format: </DOCNO> not found\n");
            exit(EXIT_FAILURE);
        }
        if (*next_char() != '\n') {
            fprintf(stderr, "warning: <DOC> not followed by newline\n");
            pos--;
            offset--;
        }
        if (!startswith("<TEXT>", 6)) {
            fprintf(stderr, "unexpected format: <TEXT> not found\n");
            exit(EXIT_FAILURE);
        }
        if (*next_char() != '\n') {
            fprintf(stderr, "warning: <TEXT> not followed by newline\n");
            pos--;
            offset--;
        }
        DocInfo doc;
        while (*(next = next_char()) != '\n') {
            doc.url.push_back(*next);
        }
        doc.begin = offset;
        unordered_map<string, unsigned> term_cnt;
        while (!startswith("</TEXT>", 7)) {
            word.clear();
            int len = get_utf8_char_len();  // must be called before next_char()
            next = next_char();
            while (is_al_num(next, len)) {
                if (len == 1) {
                    word.push_back((char) tolower(*next));
                } else {
                    for (int i = 0; i < len; i++) {
                        word.push_back(next[i]);
                    }
                    pos += len - 1;
                    offset += len - 1;
                }
                len = get_utf8_char_len();  // must be called before next_char()
                next = next_char();
            }
            pos += len - 1;
            offset += len - 1;
            if (!word.empty()) {
                doc.term_cnt++;
                total_term_cnt++;
                if (inv_index.find(word) == inv_index.end() || inv_index[word].doc_ids.back() != doc_id) {
                    inv_index[word].doc_ids.push_back(doc_id);  // OK in C++
                }
                term_cnt[word]++;  // OK in C++
            }
        }
        for (auto &[term, cnt]: term_cnt) {
            inv_index[term].freqs.push_back(cnt);
        }
        doc.end = offset - 7;
        docs.push_back(doc);
        if (*next_char() != '\n') {
            fprintf(stderr, "warning: </TEXT> not followed by newline\n");
            pos--;
            offset--;
        }
        if (!startswith("</DOC>", 6)) {
            fprintf(stderr, "unexpected format: </DOC> not found\n");
            exit(EXIT_FAILURE);
        }
        if (*next_char() != '\n') {
            fprintf(stderr, "warning: </DOC> not followed by newline\n");
            pos--;
            offset--;
        }
        if (pos >= end_pos) {
            break;
        }
        if (inv_index.size() > options.output_entry_size) {
            sort_and_dump_index(options.index_path, index_file_cnt++);
            inv_index.clear();
            dump_docs_info_txt(docs_info_fp);
            docs.clear();
        }
    }
    sort_and_dump_index(options.index_path, index_file_cnt);
    dump_docs_info_txt(docs_info_fp);
    printf("avg term cnt per doc: %f\n", (double) total_term_cnt / doc_id);
    printf("total doc cnt: %u\n", doc_id);
    if (options.read_zip) {
        gzclose(zip_fp);
    } else {
        fclose(dataset_fp);
    }
    fclose(docs_info_fp);
    delete[] buffer;
    auto end = std::chrono::steady_clock::now();
    cout << "total time used " << std::chrono::duration_cast<std::chrono::seconds>(end - start).count() << "s" << endl;
    return 0;
}
