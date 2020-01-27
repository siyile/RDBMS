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
    // reformat record data
    void* recordData = malloc(PAGE_SIZE);
    unsigned recordSize;
    convertDataToRecord(data, recordData, recordSize, recordDescriptor);
    unsigned spaceNeed = recordSize + DICT_SIZE;
    unsigned targetPage;

    // find target page to insert
    if (pageNum == 0) {
        initiateNewPage(fileHandle);
        targetPage = 0;
    } else {
        if (getFreeSpaceByPageNum(fileHandle, curPage) <= spaceNeed) {
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

    // appendRecordIntoPage
    appendRecordIntoPage(fileHandle, targetPage, recordSize, recordData);

    free(recordData);

    rid.pageNum = targetPage;
    rid.slotNum = getTotalSlotByPageNum(fileHandle, targetPage);

    return 0;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data) {
    unsigned pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;

    void *pageData = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, pageData);

    unsigned offset;
    unsigned length;

    getOffsetAndLength(pageData, slotNum, offset, length);

    void *record = malloc(length);

    memcpy(record, (char *) pageData + offset, length);

    convertRecordToData(record, data, recordDescriptor);

    free(pageData);

    return 0;
}

void
RecordBasedFileManager::convertRecordToData(void *record, void *data, const std::vector<Attribute> &recordDescriptor) {
    unsigned size = recordDescriptor.size();
    unsigned pos = UNSIGNED_SIZE;
    unsigned destPos = 0;

    int *attrsExist = new int[size];
    unsigned nullIndicatorSize = (size + 7) / 8;
    memcpy((char *) data, (char *) record + pos, nullIndicatorSize);
    destPos += nullIndicatorSize;

    getAttrExistArray(pos, attrsExist, record, size, true);

    unsigned fieldStart = pos + UNSIGNED_SIZE * size;

    for (unsigned i = 0; i < size; i++) {
        Attribute attr = recordDescriptor[i];
        bool exist = attrsExist[i];
        if (exist) {
            unsigned fieldEnd;
            memcpy(&fieldEnd, (char *) record + pos, UNSIGNED_SIZE);
            pos += UNSIGNED_SIZE;
            unsigned recordLength = fieldEnd - fieldStart;
            if (attr.type == TypeInt || attr.type == TypeReal) {
                memcpy((char *) data + destPos, (char *) record + fieldStart, UNSIGNED_SIZE);
                destPos += INT_SIZE;
            } else {
                memcpy((char *) data + destPos, (char *) &recordLength, UNSIGNED_SIZE);
                destPos += UNSIGNED_SIZE;
                memcpy((char *) data + destPos, (char *) record + fieldStart, recordLength);
                destPos += recordLength;
            }
            fieldStart = fieldEnd;
        } else {
            pos += UNSIGNED_SIZE;
        }
    }
};

// data to record
void RecordBasedFileManager::convertDataToRecord(const void *data, void *record, unsigned &recordSize,
                                                 const std::vector<Attribute> &recordDescriptor) {
    unsigned size = recordDescriptor.size();
    unsigned nullIndicatorSize = (size + 7) / 8;
    // pos = pointer position of original data
    unsigned pos = 0;

    int *attrsExist = new int[size];
    getAttrExistArray(pos, attrsExist, data, size, false);

    // write attribute number into start position
    memcpy((char *) record, (char *) &size, UNSIGNED_SIZE);

    //write null flag into start position
    memcpy((char *) record + UNSIGNED_SIZE, (char *) data, nullIndicatorSize);

    // indexOffset is the offset of the recordIndex from beginning
    unsigned indexOffset = UNSIGNED_SIZE + nullIndicatorSize;

    // dataOffset is the offset of the recordData after index
    unsigned dataOffset = indexOffset + size * UNSIGNED_SIZE;

    for (int i = 0; i < size; i++) {
        Attribute attr = recordDescriptor[i];
        bool exist = attrsExist[i];
        if (exist) {
            if (attr.type == TypeInt || attr.type == TypeReal) {
                memcpy((char *)record + dataOffset, (char *) data + pos, UNSIGNED_SIZE);
                pos += UNSIGNED_SIZE;
                dataOffset += UNSIGNED_SIZE;
            } else {
                unsigned charLength;
                memcpy(&charLength, (char *) data + pos, INT_SIZE);
                pos += UNSIGNED_SIZE;

                memcpy((char *) record + dataOffset, (char *) data + pos, charLength);
                pos += charLength;
                dataOffset += charLength;
            }
        } else {
            // do nothing
        }

        // copy offset to head of record
        memcpy((char *)record + indexOffset, (char *) &dataOffset, UNSIGNED_SIZE);
        indexOffset += UNSIGNED_SIZE;
    }

    recordSize = dataOffset;
}


RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const RID &rid) {
    unsigned pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;

    // read page data into variable data
    void* data = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, data);


    unsigned offset, length;
    getOffsetAndLength(data, slotNum, offset, length);

    // set new free space & total slotNum remain unchanged
    unsigned freeSpace = getFreeSpaceByPageNum(fileHandle, pageNum);
    setSpace(data, freeSpace - length);

    // left shift
    shiftRecord(data, slotNum + 1, length, true);

    // update previous slot
    setOffsetAndLength(data, slotNum, offset, 0);

    //write into file
    fileHandle.writePage(pageNum, data);

    free(data);
    return 0;
}

RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data) {
    unsigned size = recordDescriptor.size();
    unsigned pos = 0;

    int *attrsExist = new int[size];
    getAttrExistArray(pos, attrsExist, data, size, false);

    for (int i = 0; i < size; i++) {
        Attribute attr = recordDescriptor[i];
        std::cout << attr.name << ": ";
        bool exist = attrsExist[i];
        if (exist) {
            if (attr.type == TypeInt) {
                int intValue;
                memcpy(&intValue, (char *) data + pos, INT_SIZE);
                std::cout << intValue;
                pos += INT_SIZE;
            } else if (attr.type == TypeReal) {
                float floatValue;
                memcpy(&floatValue, (char *) data + pos, INT_SIZE);
                std::cout << floatValue;
                pos += INT_SIZE;
            } else {
                // type == TypeVarChar
                unsigned charLength;
                memcpy(&charLength, (char *) data + pos, INT_SIZE);
                pos += INT_SIZE;
                auto *value = static_cast<char *>(malloc(PAGE_SIZE));
                memcpy(value, (char *) data + pos, charLength);
                std::cout.write(value, charLength);
                pos += charLength;
                free(value);
            }
        } else {
            std::cout << "NULL";
        }
        if (i != size - 1) {
            std::cout << " ";
        }
    }

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

int RecordBasedFileManager::scanFreeSpace(FileHandle &fileHandle, unsigned curPageNum, unsigned sizeNeed) {
    for (unsigned i = 0; i < curPageNum; i++) {
        if (sizeNeed <= getFreeSpaceByPageNum(fileHandle, i)) {
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

void RecordBasedFileManager::setSlot(void *pageData, unsigned slotNum) {
    memcpy((char *) pageData + N_POS, (char *) &slotNum, UNSIGNED_SIZE);
}

void RecordBasedFileManager::setSpace(void *pageData, unsigned freeSpace) {
    memcpy((char *) pageData + F_POS, (char *) &freeSpace, UNSIGNED_SIZE);
}

int getBit(unsigned char byte, int position) // position in range 0-7
{
    return (byte >> position) & 0x1;
}

void
RecordBasedFileManager::appendRecordIntoPage(FileHandle &fileHandle, unsigned pageIdx, unsigned dataSize, const void *data) {
    unsigned freeSpace = getFreeSpace(data);
    unsigned slotNum = getTotalSlot(data);

    void *pageData = static_cast<char *>(malloc(PAGE_SIZE));
    fileHandle.readPage(pageIdx, pageData);

    unsigned offset = getTargetRecordOffset(pageData, slotNum);
    writeData(pageData, data, offset, dataSize);

    slotNum++;
    freeSpace += - dataSize - DICT_SIZE;

    setSlot(pageData, slotNum);
    setSpace(pageData, freeSpace);
    setOffsetAndLength(pageData, slotNum, offset, dataSize);

    fileHandle.writePage(pageIdx, pageData);

    free(pageData);
}

void
RecordBasedFileManager::getAttrExistArray(unsigned &pos, int *attrExist, const void *data, unsigned attrSize, bool isRecord) {
    unsigned nullIndicatorSize = (attrSize + 7) / 8;
    char *block = static_cast<char *>(malloc(sizeof(char) * nullIndicatorSize));
    memcpy(block, (char *) data + (isRecord ? UNSIGNED_SIZE : 0), nullIndicatorSize);
    unsigned idx = 0;
    for (unsigned i = 0; i < nullIndicatorSize; i++) {
        for (int j = 0; j < 8 && idx < attrSize; j++) {
            if (getBit(block[i], j) == 0) {
                attrExist[idx] = 1;
            } else {
                attrExist[idx] = 0;
            }
            idx++;
        }
    }
    pos += nullIndicatorSize;
}

unsigned RecordBasedFileManager::getTargetRecordOffset(void *data, unsigned slotNum) {
    if (slotNum == 0) {
        return 0;
    }

    unsigned pos = PAGE_SIZE - 2 * UNSIGNED_SIZE - slotNum * DICT_SIZE;

    unsigned lastOffset;
    memcpy(&lastOffset, (char *) data + pos, UNSIGNED_SIZE);

    pos += UNSIGNED_SIZE;
    unsigned lastLength;
    memcpy(&lastLength, (char *) data + pos, UNSIGNED_SIZE);

    unsigned offset = lastOffset + lastLength;

    return offset;
}

void RecordBasedFileManager::writeData(void *pageData, const void *record, unsigned offset, unsigned length) {
    memcpy((char *) pageData + offset, (char *)record, length);
}

unsigned RecordBasedFileManager::getFreeSpaceByPageNum(FileHandle &fileHandle, unsigned pageNum) {
    void *data = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, data);
    unsigned freeSpace = getFreeSpace(data);
    free(data);
    return freeSpace;
}

unsigned RecordBasedFileManager::getTotalSlotByPageNum(FileHandle &fileHandle, unsigned pageNum) {
    void *data = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, data);
    unsigned slotNum = getTotalSlot(data);
    free(data);
    return slotNum;
}

unsigned RecordBasedFileManager::getFreeSpace(const void *data) {
    unsigned slotNum;
    memcpy(&slotNum, (char *)data + N_POS, sizeof(unsigned));

    return slotNum;
}

unsigned RecordBasedFileManager::getTotalSlot(const void *data) {
    unsigned freeSpace;
    memcpy(&freeSpace, (char *) data + F_POS, sizeof(unsigned));

    return freeSpace;
}

void RecordBasedFileManager::setOffsetAndLength(void *data, unsigned slotNum, unsigned offset, unsigned length) {
    unsigned pos = PAGE_SIZE - 2 * UNSIGNED_SIZE - DICT_SIZE * slotNum;

    memcpy((char *) data + pos, (char *) &offset, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;
    memcpy((char *) data + pos, (char *) &length, UNSIGNED_SIZE);
}

void RecordBasedFileManager::getOffsetAndLength(void *data, unsigned slotNum, unsigned &offset, unsigned &length) {
    unsigned pos = PAGE_SIZE - 2 * UNSIGNED_SIZE - slotNum * DICT_SIZE;
    memcpy(&offset, (char *) data + pos, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;
    memcpy(&length, (char *) data + pos, UNSIGNED_SIZE);
}

void RecordBasedFileManager::shiftRecord(void *data, unsigned slotNum, unsigned length, bool isLeftShift) {
    unsigned totalSlot = getTotalSlot(data);
    length *= isLeftShift ? -1 : 1;

    // get starting offset
    unsigned startOffset;
    unsigned _;
    getOffsetAndLength(data, slotNum, startOffset, _);

    unsigned totalLength = 0;
    // calculate total length & update offset
    for (unsigned i = slotNum; i <= totalSlot; i++) {
        unsigned recordOffset;
        unsigned recordLength;
        getOffsetAndLength(data, i, recordOffset, recordLength);
        setOffsetAndLength(data, i, recordOffset + length, recordLength);

        totalLength += recordLength;
    }

    // shift whole record
    memmove((char *)data + startOffset + length, (char *)data + startOffset, totalLength);
}



