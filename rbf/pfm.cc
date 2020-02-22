#include "pfm.h"
#include <iostream>
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
        std::fstream outfile(fileName, std::ios::out | std::ios::binary);
        unsigned x = 0;
        for (int i = 0; i < 3; i++) {
            outfile.write(reinterpret_cast<const char *>(&x), sizeof(x));
        }
        unsigned y = NOT_VALID_UNSIGNED_SIGNAL;
        outfile.write(reinterpret_cast<const char *>(&y), sizeof(y));
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
        fileHandle.fs.open(fileName, std::ios::in | std::ios::out | std::ios::binary);
        if (fileHandle.fs.is_open()) {
            fileHandle.phyReadCounterValues();
        } else {
            return -1;
        }
        return 0;
    }
}

RC PagedFileManager::closeFile(FileHandle &fileHandle) {
    if (fileHandle.fs.is_open()) {
        fileHandle.phyWriteCounterValues();
        fileHandle.fs << std::flush;
        fileHandle.fs.close();
    } else {
        return -1;
    }
    return 0;
}

FileHandle::FileHandle() {
}

FileHandle::~FileHandle() = default;

RC FileHandle::readPage(PageNum pageNum, void *data) {
    if (fs.is_open() && (pageNum + 1 <=  totalPageCounter)) {
        fs.seekg ((pageNum + 1) * PAGE_SIZE, std::ios::beg);
        fs.read(static_cast<char *>(data), PAGE_SIZE);
    } else {
        return -1;
    }
    readPageCounter = readPageCounter + 1;
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    if (fs.is_open() && (pageNum + 1 <=  totalPageCounter)) {
        fs.seekp ((pageNum + 1) * PAGE_SIZE, std::ios::beg);
        fs.write(reinterpret_cast<const char *>(data), PAGE_SIZE);
    } else {
        return -1;
    }
    writePageCounter = writePageCounter + 1;
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    if (fs.is_open()) {
        fs.seekp ((totalPageCounter + 1) * PAGE_SIZE);
        fs.write(reinterpret_cast<const char *>(data), PAGE_SIZE);
    } else {
        return -1;
    }
    appendPageCounter = appendPageCounter + 1;
    totalPageCounter = appendPageCounter;
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    return totalPageCounter;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    readPageCount = readPageCounter;
    writePageCount = writePageCounter;
    appendPageCount = appendPageCounter;

    return 0;
}

RC FileHandle::phyWriteCounterValues() {
    if (fs.is_open()) {
        fs.seekp (0, std::ios::beg);
        fs.write(reinterpret_cast<const char *>(&readPageCounter), sizeof(readPageCounter));
        fs.write(reinterpret_cast<const char *>(&writePageCounter), sizeof(writePageCounter));
        fs.write(reinterpret_cast<const char *>(&appendPageCounter), sizeof(appendPageCounter));
    } else {
        return -1;
    }

    return 0;
}

RC FileHandle::phyReadCounterValues() {
    fs.seekg (0, std::ios::beg);
    fs.read(reinterpret_cast<char *>(&readPageCounter), sizeof(readPageCounter));
    fs.read(reinterpret_cast<char *>(&writePageCounter), sizeof(writePageCounter));
    fs.read(reinterpret_cast<char *>(&appendPageCounter), sizeof(appendPageCounter));
    totalPageCounter = appendPageCounter;

};


