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
    appendRecordIntoPage(fileHandle, targetPage, recordSize, recordData, rid);

    free(recordData);

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

    if (length == 0) {
        return -1;
    }

    void *record = malloc(length);

    memcpy(record, (char *) pageData + offset, length);

    convertRecordToData(record, data, recordDescriptor);

    free(pageData);

    return 0;
}

void
RecordBasedFileManager::convertRecordToData(void *record, void *data, const std::vector<Attribute> &recordDescriptor) {
    unsigned size = recordDescriptor.size();
    unsigned pos = UNSIGNED_SIZE + REDIRECT_INDICATOR_SIZE;
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

    // add redirect indicator
    unsigned char redirectIndicator = 0x0;
    memcpy(record, &redirectIndicator, REDIRECT_INDICATOR_SIZE);
    pos += REDIRECT_INDICATOR_SIZE;

    int *attrsExist = new int[size];
    getAttrExistArray(pos, attrsExist, data, size, false);

    // write attribute number into start position
    memcpy((char *) record + pos, (char *) &size, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //write null flag into start position
    memcpy((char *) record + pos, (char *) data, nullIndicatorSize);
    pos += nullIndicatorSize;

    // indexOffset is the offset of the recordIndex from beginning
    unsigned indexOffset = pos;

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

    if (length == 0) {
        return -1;
    }

    // left shift
    leftShiftRecord(data, offset, length);

    // set new free space & total slotNum remain unchanged
    unsigned freeSpace = getFreeSpace(data);
    setSpace(data, freeSpace + length);

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
                if (charLength != 0) {
                    auto *value = static_cast<char *>(malloc(PAGE_SIZE));
                    memcpy(value, (char *) data + pos, charLength);
                    std::cout.write(value, charLength);
                    pos += charLength;
                    free(value);
                }
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

bool isNullBit(unsigned char byte, int position) // position in range 0-7
{
    bool nullBit = byte & (unsigned) 1 << (unsigned) (7 - position);
    return nullBit;
}

void
RecordBasedFileManager::appendRecordIntoPage(FileHandle &fileHandle, unsigned pageIdx, unsigned dataSize,
                                             const void *record, RID &rid) {
    void *pageData = static_cast<char *>(malloc(PAGE_SIZE));
    fileHandle.readPage(pageIdx, pageData);

    unsigned freeSpace = getFreeSpace(pageData);
    unsigned slotNum = getTotalSlot(pageData);

    unsigned targetSlotNum = slotNum + 1;

    // scan page until find a empty slot.
    // ATTENTION: this is the slot that previously deleted.
    for (unsigned i = slotNum; i > 0; i--) {
        unsigned recordOffset;
        unsigned recordLength;
        getOffsetAndLength(pageData, i, recordOffset, recordLength);

        // find record is already deleted.
        if (recordLength == 0) {
            targetSlotNum = i;
            break;
        }
    }

    unsigned offset;

    // if have previously deleted slot, calculate offset by free space
    if (targetSlotNum == slotNum + 1) {
        offset = PAGE_SIZE - freeSpace - slotNum * DICT_SIZE - 2 * UNSIGNED_SIZE;
        slotNum++;
        freeSpace += - dataSize - DICT_SIZE;
    } else {
        offset = PAGE_SIZE - freeSpace - slotNum * DICT_SIZE - 2 * UNSIGNED_SIZE;
        freeSpace += - dataSize;
    }

    writeRecord(pageData, record, offset, dataSize);

    setSlot(pageData, slotNum);
    setSpace(pageData, freeSpace);
    setOffsetAndLength(pageData, targetSlotNum, offset, dataSize);

    fileHandle.writePage(pageIdx, pageData);

    rid.pageNum = pageIdx;
    rid.slotNum = targetSlotNum;

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
            if (!isNullBit(block[i], j)) {
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

void RecordBasedFileManager::writeRecord(void *pageData, const void *record, unsigned offset, unsigned length) {
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

unsigned RecordBasedFileManager::getTotalSlot(const void *data) {
    unsigned slotNum;
    memcpy(&slotNum, (char *)data + N_POS, sizeof(unsigned));

    return slotNum;
}

unsigned RecordBasedFileManager::getFreeSpace(const void *data) {
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

void RecordBasedFileManager::leftShiftRecord(void *data, unsigned startOffset, unsigned int length) {
    unsigned totalSlot = getTotalSlot(data);

    for (int i = 1; i <= totalSlot; i++) {
        unsigned recordOffset;
        unsigned recordLength;
        getOffsetAndLength(data, i, recordOffset, recordLength);
        if (recordOffset > startOffset) {
            recordOffset -= length;
            setOffsetAndLength(data, i, recordOffset, recordLength);
        }
    }

    unsigned freeSpace = getFreeSpace(data);
    unsigned totalLength = PAGE_SIZE - freeSpace - totalSlot * DICT_SIZE - 2 * UNSIGNED_SIZE - startOffset - length;

    // shift whole record
    memmove((char *)data + startOffset, (char *)data + startOffset + length, totalLength);
}

bool RecordBasedFileManager::isRedirect(void *data) {
    unsigned char redirectFlag;
    memcpy(&redirectFlag, data, REDIRECT_INDICATOR_SIZE);
    return redirectFlag == 0x1;
}


