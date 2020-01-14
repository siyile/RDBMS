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
        fileHandle.fs.open(fileName, std::ios::in | std::ios::out | std::ios::binary);
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
    totalPageCounter = 0;
}

FileHandle::~FileHandle() = default;

RC FileHandle::readPage(PageNum pageNum, void *data) {
    unsigned rpc, wpc, apc;
    collectCounterValues(rpc, wpc, apc);
    if (this->fs.is_open() && (pageNum + 1 <=  totalPageCounter)) {
        this->fs.seekg ((pageNum + 1) * PAGE_SIZE, std::ios::beg);
        this->fs.read(static_cast<char *>(data), PAGE_SIZE);
    } else {
        return -1;
    }
    readPageCounter = readPageCounter + 1;
    phyWriteCounterValues();
    return 0;
}

RC FileHandle::writePage(PageNum pageNum, const void *data) {
    unsigned rpc, wpc, apc;
    collectCounterValues(rpc, wpc, apc);
    if (this->fs.is_open() && (pageNum + 1 <=  totalPageCounter)) {
        this->fs.seekp ((pageNum + 1) * PAGE_SIZE, std::ios::beg);
        this->fs.write(reinterpret_cast<const char *>(data), PAGE_SIZE);
    } else {
        return -1;
    }
    writePageCounter = writePageCounter + 1;
    phyWriteCounterValues();
    return 0;
}

RC FileHandle::appendPage(const void *data) {
    unsigned rpc, wpc, apc;
    collectCounterValues(rpc, wpc, apc);
    if (this->fs.is_open()) {
        this->fs.seekp ((totalPageCounter + 1) * PAGE_SIZE, std::ios::beg);
        this->fs.write(reinterpret_cast<const char *>(data), PAGE_SIZE);
    } else {
        return -1;
    }
    appendPageCounter = appendPageCounter + 1;
    phyWriteCounterValues();
    return 0;
}

unsigned FileHandle::getNumberOfPages() {
    unsigned rpc, wpc, apc;
    collectCounterValues(rpc, wpc, apc);
    return totalPageCounter;
}

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    this->fs.seekg (0, std::ios::beg);
    this->fs.read(reinterpret_cast<char *>(&readPageCount), sizeof(readPageCount));
    this->fs.read(reinterpret_cast<char *>(&writePageCount), sizeof(writePageCount));
    this->fs.read(reinterpret_cast<char *>(&appendPageCount), sizeof(appendPageCount));

//    std::cout << readPageCount << writePageCount << appendPageCount << std::endl;

    this->readPageCounter = readPageCount;
    this->writePageCounter = writePageCount;
    this->appendPageCounter = appendPageCount;
    this->totalPageCounter = appendPageCount;


//    readPageCounter = readPageCount;
//    writePageCounter = writePageCount;
//    appendPageCounter = appendPageCount;
    return 0;
}

RC FileHandle::phyWriteCounterValues() {
    this->fs.seekp (0, std::ios::beg);
    std::cout << readPageCounter << writePageCounter << appendPageCounter << std::endl;
    this->fs.write(reinterpret_cast<const char *>(&readPageCounter), sizeof(readPageCounter));
    this->fs.write(reinterpret_cast<const char *>(&writePageCounter), sizeof(writePageCounter));
    this->fs.write(reinterpret_cast<const char *>(&appendPageCounter), sizeof(appendPageCounter));
    return -1;
}

