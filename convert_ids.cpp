#include <iostream>
#include <fstream>
#include <cstring>
#include <vector>
#include <unordered_set>
#include <unordered_map>
#include <algorithm>
#include <filesystem>
#include <windows.h>

namespace fs = std::filesystem;
using std::cout;
using std::cerr;
using std::endl;
using std::ifstream;
using std::ofstream;
using std::string;
using std::stringstream;
using std::vector;
using std::pair;
using std::sort;
using std::unordered_set;
using std::unordered_map;

struct Options {
    const char *dataset_file_path = "fulldocs-new.trec";
} options;

bool startswith(const char *str, const char *keyword, int len) {
    return strncmp(str, keyword, len) == 0;
}

void print_usage(char *program_name) {
    printf("Usage: %s [-h] [-d dataset_file_path]\n"
           "Options:\n"
           "\t-d\tdataset path, default: fulldocs-new.trec\n"
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
            else {
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

    // Map the large dataset file to memory
    HANDLE dataset_file = CreateFileA(options.dataset_file_path, GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (dataset_file == INVALID_HANDLE_VALUE) {
        fprintf(stderr, "failed to open dataset file\n");
        exit(EXIT_FAILURE);
    }
    LARGE_INTEGER dataset_file_size_li;
    if (!GetFileSizeEx(dataset_file, &dataset_file_size_li)) {
        fprintf(stderr, "failed to get dataset file size\n");
        exit(EXIT_FAILURE);
    }
    long long dataset_file_size = dataset_file_size_li.QuadPart;
    HANDLE dataset_file_mapping = CreateFileMapping(dataset_file, nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (dataset_file_mapping == nullptr) {
        fprintf(stderr, "failed to create dataset file mapping\n");
        exit(EXIT_FAILURE);
    }
    const char *buffer = (char *) MapViewOfFile(dataset_file_mapping, FILE_MAP_READ, 0, 0, 0);
    if (buffer == nullptr) {
        fprintf(stderr, "failed to map dataset file to memory\n");
        exit(EXIT_FAILURE);
    }
    const char *cur = buffer;

    unordered_map<string, unsigned> ids;
    for (unsigned doc_id = 0;; doc_id++) {
        if (!startswith(cur, "<DOC>", 5)) {
            fprintf(stderr, "unexpected format: <DOC> not found\n");
            exit(EXIT_FAILURE);
        }
        cur += 5;
        if (*cur++ != '\n') {
            fprintf(stderr, "warning: <DOC> not followed by newline\n");
            cur--;
        }
        if (!startswith(cur, "<DOCNO>", 7)) {
            fprintf(stderr, "unexpected format: <DOCNO> not found\n");
            exit(EXIT_FAILURE);
        }
        cur += 7;
        const char *docno_begin = cur;
        while (*cur++ != '<') {
        }
        cur--;
        const char *docno_end = cur;
        string s = string(docno_begin, docno_end);
        ids[string(docno_begin, docno_end)] = doc_id;
        if (!startswith(cur, "</DOCNO>", 8)) {
            fprintf(stderr, "unexpected format: </DOCNO> not found\n");
            exit(EXIT_FAILURE);
        }
        cur += 8;
        if (*cur++ != '\n') {
            fprintf(stderr, "warning: <DOC> not followed by newline\n");
            cur--;
        }
        if (!startswith(cur, "<TEXT>", 6)) {
            fprintf(stderr, "unexpected format: <TEXT> not found\n");
            exit(EXIT_FAILURE);
        }
        cur += 6;
        while (!startswith(cur, "</TEXT>", 7)) {
            cur++;
        }
        cur += 7;
        if (*cur++ != '\n') {
            fprintf(stderr, "warning: </TEXT> not followed by newline\n");
            cur--;
        }
        if (!startswith(cur, "</DOC>", 6)) {
            fprintf(stderr, "unexpected format: </DOC> not found\n");
            exit(EXIT_FAILURE);
        }
        cur += 6;
        if (*cur++ != '\n') {
            fprintf(stderr, "warning: </DOC> not followed by newline\n");
            cur--;
        }
        if (cur - buffer >= dataset_file_size) {
            break;
        }
        if (doc_id % 10000 == 0) {
            cout << "processed " << doc_id << " documents" << endl;
        }
    }

    ifstream qrels("msmarco-doctrain-qrels.tsv");
    if (!qrels.is_open()) {
        fprintf(stderr, "failed to open qrels file\n");
        exit(EXIT_FAILURE);
    }
    ofstream qrels_new("msmarco-doctrain-qrels-idconverted.tsv");
    if (!qrels_new.is_open()) {
        fprintf(stderr, "failed to open new qrels file\n");
        exit(EXIT_FAILURE);
    }
    string line;
    while (getline(qrels, line)) {
        stringstream ss(line);
        string qid, _, docno;
        ss >> qid >> _ >> docno >> _;
        qrels_new << qid << " " << ids[docno] << endl;
    }
    qrels.close();
    qrels_new.close();

    // Unmap the dataset file
    UnmapViewOfFile(buffer);
    CloseHandle(dataset_file_mapping);
    CloseHandle(dataset_file);

    auto end = std::chrono::steady_clock::now();
    cout << "total time used " << std::chrono::duration_cast<std::chrono::seconds>(end - start).count() << "s" << endl;
    return 0;
}
