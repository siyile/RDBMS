#include <vector>
#include <cstring>
#include "rbfm.h"
#include <cmath>
#include <iostream>

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
    unsigned dataSize = getRecordSize(data, recordDescriptor);
    unsigned spaceNeed = dataSize + DICT_SIZE;
    unsigned targetPage;

    // find target page to insert
    if (pageNum == 0) {
        initiateNewPage(fileHandle);
        targetPage = 0;
    } else {
        if (getFreeSpace(fileHandle, curPage) <= spaceNeed) {
            // current page have enough space
            targetPage = curPage;
        } else {
            // scan
            int scannedPage = scanFreeSpace(fileHandle, curPage, spaceNeed);
            if (scannedPage == -1) {
                // not enough space
                initiateNewPage(fileHandle);
                curPage++;
                targetPage = curPage;
            } else {
                // find target page
                targetPage = scannedPage;
            }
        }
    }

    // insertRecordIntoPage
    insertRecordIntoPage(fileHandle, targetPage, dataSize, data);

    rid.pageNum = targetPage;
    rid.slotNum = getSlotNum(fileHandle, targetPage);

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    unsigned pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;

    void *pageData = nullptr;
    fileHandle.readPage(pageNum, data);

    unsigned offset;
    unsigned length;

    getOffsetAndLength(pageData, slotNum, offset, length);

    memcpy(data, (char *) pageData + offset, length);

    return 0;
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    return -1;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    unsigned size = recordDescriptor.size();
    unsigned pos = 0;

    int *attrsExist = new int[size];
    getAttrExistArray(pos, attrsExist, data, size);

    auto *value = static_cast<char *>(malloc(PAGE_SIZE));

    for (int i = 0; i < size; i++) {
        Attribute attr = recordDescriptor[i];
        std::cout << attr.name << ": ";
        bool exist = attrsExist[i];
        if (exist) {
            if (attr.type == TypeInt || attr.type == TypeReal) {
                memcpy(value, (char *) data + pos, INT_SIZE);
                std::cout.write(value, INT_SIZE);
                pos += INT_SIZE;
            } else {
                // type == TypeVarChar
                unsigned charLength;
                memcpy(&charLength, (char *) data + pos, INT_SIZE);
                pos += INT_SIZE;
                memcpy(value, (char *) data + pos, charLength);
                std::cout.write(value, charLength);
                pos += charLength;
            }
        } else {
            std::cout << "NULL";
        }
        if (i != size - 1) {
            std::cout << " ";
        }
    }

    free(value);

    return 0;
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
    char *data = nullptr;
    fileHandle.readPage(pageNum, data);
    unsigned freeSpace;
    memcpy(&freeSpace, data + F_POS, sizeof(unsigned));
    return freeSpace;
}

unsigned RecordBasedFileManager::getSlotNum(FileHandle &fileHandle, unsigned pageNum) {
    char *data = nullptr;
    fileHandle.readPage(pageNum, data);
    unsigned slotNum;
    memcpy(&slotNum, data + N_POS, sizeof(unsigned));
    return slotNum;
}

int RecordBasedFileManager::scanFreeSpace(FileHandle &fileHandle, unsigned curPageNum, unsigned sizeNeed) {
    for (int i = 0; i < curPageNum; i++) {
        if (sizeNeed <= getFreeSpace(fileHandle, i)) {
            return i;
        }
    }
    return -1;
}

unsigned RecordBasedFileManager::initiateNewPage(FileHandle &fileHandle) {
    auto *data = static_cast<unsigned int *>(malloc(PAGE_SIZE));
    *(data + F_POS / sizeof(unsigned)) = INIT_FREE_SPACE;
    *(data + N_POS / sizeof(unsigned)) = 0;

    fileHandle.appendPage(data);
    free(data);

    return 0;
}

void RecordBasedFileManager::setSlot(void *pageData, unsigned SlotNum) {

}

void RecordBasedFileManager::setSpace(void *pageData, unsigned freeSpace) {

}

unsigned RecordBasedFileManager::getRecordSize(const void *data, const std::vector<Attribute> &recordDescriptor) {
    unsigned size = recordDescriptor.size();
    unsigned pos = 0;

    int *attrsExist = new int[size];
    getAttrExistArray(pos, attrsExist, data, size);

    for (int i = 0; i < size; i++) {
        Attribute attr = recordDescriptor[i];
        bool exist = attrsExist[i];
        if (exist) {
            if (attr.type == TypeInt || attr.type == TypeReal) {
                pos += INT_SIZE;
            } else {
                // type == TypeVarChar
                unsigned charLength;
                memcpy(&charLength, (char *) data + pos, INT_SIZE);
                pos += INT_SIZE;
                pos += charLength;
            }
        }
    }

    return pos;
}

bool getBit(unsigned char byte, int position) // position in range 0-7
{
    return (byte >> position) & 0x1;
}

RC
RecordBasedFileManager::insertRecordIntoPage(FileHandle &fileHandle, unsigned pageIdx, unsigned dataSize, const void *data) {

}

void
RecordBasedFileManager::getAttrExistArray(unsigned &pos, int *attrExist, const void *data, unsigned attrSize) {
    unsigned nullIndicatorSize = (attrSize + 7) / 8;
    char *block = nullptr;
    memcpy(block, data, nullIndicatorSize);
    int idx = 0;
    for (int i = 0; i < nullIndicatorSize; i++) {
        for (int j = 0; j < 8 && idx < attrSize; j++) {
            if (getBit(block[i], j)) {
                attrExist[idx] = 1;
            } else {
                attrExist[idx] = 0;
            }
            idx++;
        }
    }
    pos += nullIndicatorSize;
}

unsigned RecordBasedFileManager::getInsertOffset(void *data, unsigned slotNum) {

}

void RecordBasedFileManager::writeData(void *pageData, const void *data, unsigned offset, unsigned length) {

}

void RecordBasedFileManager::setOffsetAndLength(void *data, unsigned offset, unsigned length, unsigned slotNum) {

}

void RecordBasedFileManager::getOffsetAndLength(void *data, unsigned slotNum, unsigned &offset, unsigned &length) {
    unsigned pos = PAGE_SIZE - 2 * UNSIGNED_SIZE - slotNum * DICT_SIZE;
    memcpy(&offset, (char *) data + pos, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;
    memcpy(&length, (char *) data + pos, UNSIGNED_SIZE);
};




