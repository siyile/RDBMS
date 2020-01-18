#include "rbfm.h"

RecordBasedFileManager &RecordBasedFileManager::instance() {
    static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager() = default;

RecordBasedFileManager::~RecordBasedFileManager() = default;

RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

RC RecordBasedFileManager::createFile(const std::string &fileName) {
    return PagedFileManager::instance().createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
    return  PagedFileManager::instance().destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
    return PagedFileManager::instance().openFile(fileName,fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return  PagedFileManager::instance().closeFile(fileHandle);;
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, RID &rid) {
    unsigned pageNum = fileHandle.getNumberOfPages();
    unsigned curPage = pageNum - 1;
    char * voidPage;
    unsigned dataSize = getDateSize(data, recordDescriptor);
    unsigned spaceNeed = dataSize + DICT_SIZE;
    voidPage = (char *) malloc(PAGE_SIZE);
    unsigned targetPage;

    // find target page to insert
    if (pageNum == 0) {
        initiateNewPage(fileHandle, 0);
        targetPage = 0;
    } else {
        // cur fit
        if (getFreeSpace(fileHandle, curPage) <= spaceNeed) {
            targetPage = curPage;
        } else {
            // scan
            int scannedPage = scanFreeSpace(fileHandle, curPage, spaceNeed);
            // not enough space
            if (scannedPage == -1) {
                initiateNewPage(fileHandle, 0);
                targetPage = curPage + 1;
            } else {
                targetPage = scannedPage;
            }
        }
    }

    // insertRecordIntoPage
    insertRecordIntoPage(fileHandle, targetPage, dataSize);


    return -1;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    return -1;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {
    return -1;
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    return -1;
}

unsigned RecordBasedFileManager::getFreeSpace(FileHandle &fileHandle, unsigned pageNum) {
    fileHandle.fs.seekg(pageNum * PAGE_SIZE + F_POS, std::ios::beg);
    unsigned freeSpaceNum;
    fileHandle.fs.read(reinterpret_cast<char *>(&freeSpaceNum), sizeof(freeSpaceNum));
    return freeSpaceNum;
}

unsigned RecordBasedFileManager::getSlotNum(FileHandle &fileHandle, unsigned pageNum) {
    fileHandle.fs.seekg(pageNum * N_POS, std::ios::beg);
    unsigned SlotNum;
    fileHandle.fs.read(reinterpret_cast<char *>(&SlotNum), sizeof(SlotNum));
    return SlotNum;
}

int RecordBasedFileManager::scanFreeSpace(FileHandle &fileHandle, unsigned curPageNum, unsigned sizeNeed) {
    for (int i = 0; i < curPageNum; i++) {
        if (sizeNeed <= getSlotNum(fileHandle, i)) {
            return i;
        }
    }
    return -1;
}

unsigned RecordBasedFileManager::initiateNewPage(FileHandle &fileHandle, unsigned newPageNum) {
    fileHandle.fs.seekg()
    return 0;
}

unsigned RecordBasedFileManager::setSlot(FileHandle &fileHandle, unsigned newSlot) {
    return 0;
}

unsigned RecordBasedFileManager::setSpace(FileHandle &fileHandle, unsigned newSpace) {
    return 0;
}

unsigned RecordBasedFileManager::getDateSize(const void *data, const std::vector<Attribute> &recordDescriptor) {
    return 0;
}

RC RecordBasedFileManager::insertRecordIntoPage(FileHandle &fileHandle, unsigned targetPage, unsigned dataSize) {
    return 0;
};




