#include "pfm.h"
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <cstdio>
#include <unordered_map>

PagedFileManager &PagedFileManager::instance() {
    static PagedFileManager _pf_manager = PagedFileManager();
    return _pf_manager;
}

PagedFileManager::PagedFileManager() = default;

PagedFileManager::~PagedFileManager() = default;

PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

inline bool exists_test (const std::string& name) {
    struct stat buffer;
    return (stat (name.c_str(), &buffer) == 0);
}
RC PagedFileManager::createFile(const std::string &fileName) {

    if (exists_test(fileName)) {
        return -1;
    } else {
        std::ofstream outfile(fileName);
        outfile.close();
    }
    return 0;
}

RC PagedFileManager::destroyFile(const std::string &fileName) {
    const int result = remove(fileName.c_str());
    return result;
}

RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    if (!exists_test(fileName)) {
        return -1;
    } else {
        fileHandle.fileName = fileName;
        fileHandle.fs.open(fileName);
    }
    return 0;
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    if (fileHandle.fs.is_open()) {
        fileHandle.fs.close();
    } else {
        return -1;
    }
    return 0;
}

FileHandle::FileHandle() {
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
}

FileHandle::~FileHandle() = default;

RC FileHandle::readPage(PageNum pageNum, void *data) {
    return -1;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    return -1;
}

RC FileHandle::appendPage(const void *data) {
    return -1;
}

unsigned FileHandle::getNumberOfPages() {
    return -1;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    return -1;
}