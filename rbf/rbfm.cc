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
        if (getFreeSpaceByPageNum(fileHandle, curPage) >= spaceNeed) {
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
    unsigned recordLength;
    return readRecord(fileHandle, recordDescriptor, rid, data, false, recordLength);

}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                      const RID &rid, void *data, bool isOutputRecord, unsigned &recordLength) {
    unsigned pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;

    void *pageData = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, pageData);

    unsigned offset, length;
    getOffsetAndLength(pageData, slotNum, offset, length);

    void *record = malloc(length);
    if (readRecordFromPage(pageData, record, slotNum) == -1) {
        free(pageData);
        return -1;
    }

    free(pageData);

    // if record is redirected, then return the forwarded data
    if (isRedirected(record)) {
        RID redirectRID;
        getRIDFromRedirectedRecord(record, redirectRID);
        free(record);
        return readRecord(fileHandle, recordDescriptor, redirectRID, data, isOutputRecord, recordLength);
    } else {
        if (isOutputRecord) {
            recordLength = length;
            memcpy((char *) data, (char *) record, length);
            return 0;
        } else {
            convertRecordToData(record, data, recordDescriptor);
            free(record);
        }
    }

    return 0;
}

void
RecordBasedFileManager::convertRecordToData(void *record, void *data, const std::vector<Attribute> &recordDescriptor) {
    unsigned size = recordDescriptor.size();
    // pos = pointer position in record
    unsigned pos = UNSIGNED_SIZE + REDIRECT_INDICATOR_SIZE;
    // destPos = pointer position in data
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
    // recordPos = pointer position of record data
    unsigned recordPos = 0;

    // add redirect indicator
    unsigned char redirectIndicator = 0x0;
    memcpy(record, &redirectIndicator, REDIRECT_INDICATOR_SIZE);
    recordPos += REDIRECT_INDICATOR_SIZE;

    int *attrsExist = new int[size];
    getAttrExistArray(pos, attrsExist, data, size, false);

    // write attribute number into start position
    memcpy((char *) record + recordPos, (char *) &size, UNSIGNED_SIZE);
    recordPos += UNSIGNED_SIZE;

    //write null flag into start position
    memcpy((char *) record + recordPos, (char *) data, nullIndicatorSize);
    recordPos += nullIndicatorSize;

    // indexOffset is the offset of the recordIndex from beginning
    unsigned indexOffset = recordPos;

    // dataOffset is the offset of the recordData after index
    unsigned dataOffset = indexOffset + size * UNSIGNED_SIZE;

    for (unsigned i = 0; i < size; i++) {
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

    // if record is already deleted
    if (length == 0) {
        return -1;
    }

    // record is going to forward or not
    void *record = malloc(length);
    memcpy(record, (char *) data + offset, length);

    // if is, also delete forward record
    if (isRedirected(record)) {
        RID redirectRID;
        getRIDFromRedirectedRecord(record, redirectRID);
        deleteRecord(fileHandle, recordDescriptor, redirectRID);
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

    std::cout << std::endl;

    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                        const void *data, const RID &rid) {
    unsigned pageNum = rid.pageNum;
    unsigned slotNum = rid.slotNum;

    // read page data into variable data
    void* pageData = malloc(PAGE_SIZE);
    fileHandle.readPage(pageNum, pageData);

    unsigned newLength;
    void* newRecord = malloc(PAGE_SIZE);
    convertDataToRecord(data, newRecord, newLength, recordDescriptor); // get newLength

    void *record = malloc(PAGE_SIZE);
    readRecordFromPage(pageData, record, slotNum);

    // if this record is forwarded, recursively update forward record.
    if (isRedirected(record)) {
        RID newRID;
        readRIDFromRecord(record, newRID);
        free(pageData);
        free(newRecord);
        free(record);
        return updateRecord(fileHandle, recordDescriptor, data, newRID);
    }

    unsigned freeSpace = getFreeSpace(pageData);

    unsigned offset, oldLength;
    getOffsetAndLength(pageData, slotNum, offset, oldLength); // get information of old record

    unsigned lengthGap;
    // length do not change
    if (oldLength == newLength) {
        writeRecord(pageData, newRecord, offset, newLength);
        fileHandle.writePage(pageNum, pageData);
    } else if (newLength < oldLength) {
        lengthGap = oldLength - newLength;
        // if new record is shorter

        // update previous slot
        writeRecord(pageData, newRecord, offset, newLength);
        setOffsetAndLength(pageData, slotNum, offset, newLength);

        leftShiftRecord(pageData, offset, lengthGap);
        setSpace(pageData, freeSpace + lengthGap);

        fileHandle.writePage(pageNum, pageData);
    } else {
        // if new record is longer
        lengthGap = newLength - oldLength;
        // if there is enough number for new record
        if (freeSpace >= lengthGap) {
            // shift record to right
            rightShiftRecord(pageData, offset, oldLength, newLength);
            freeSpace -= lengthGap;
            setSpace(pageData, freeSpace);

            // update record
            writeRecord(pageData, newRecord, offset, newLength);
            setOffsetAndLength(pageData, slotNum, offset, newLength);

            fileHandle.writePage(pageNum, pageData);
        } else {
            // if there is not enough space for new record

            // set up current page
            leftShiftRecord(pageData, offset, oldLength - RID_SIZE);
            freeSpace += oldLength - RID_SIZE;
            setOffsetAndLength(pageData, slotNum, offset, RID_SIZE);
            setSpace(pageData, freeSpace);

            // get rid from new location
            RID curRid;
            insertRecord(fileHandle, recordDescriptor, data, curRid);

            // write new RID back to old page
            createRIDRecord(record, curRid);
            writeRecord(pageData, record, offset, RID_SIZE);
            fileHandle.writePage(pageNum, pageData);
        }
    }
    free(record);
    free(newRecord);
    free(pageData);
    return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                         const RID &rid, const std::string &attributeName, void *data) {
    unsigned _;
    unsigned size = recordDescriptor.size();
    void * record = malloc(PAGE_SIZE);
    readRecord(fileHandle, recordDescriptor, rid, record, true, _);

    int *attrsExist = new int[size];
    unsigned dirStartPos = UNSIGNED_SIZE + REDIRECT_INDICATOR_SIZE;
    // implicit move nullIndicatorSize
    getAttrExistArray(dirStartPos, attrsExist, record, size, true);

    bool found = false;

    unsigned dirPointerPos = dirStartPos;
    bool isFirstAttr = true;
    unsigned length;
    unsigned offset;

    for (unsigned i = 0; i < size; i++) {
        if (attrsExist[i]) {
            if (recordDescriptor[i].name == attributeName) {
                unsigned targetDataEndPos;
                memcpy(&targetDataEndPos, (char *) record + dirPointerPos, UNSIGNED_SIZE);

                // calculate offset & length
                if (isFirstAttr) {
                    // if is the starting index, get the starting data offset
                    // dataStartPos is the data start offset
                    unsigned dataStartPos = dirStartPos;
                    for (unsigned j = 0; j < size; j++) {
                        if (attrsExist[j]) {
                            dataStartPos += UNSIGNED_SIZE;
                        } else {

                        }
                    }

                    offset = dataStartPos;
                    length = targetDataEndPos - dataStartPos;
                } else {
                    unsigned targetDataStartPos;
                    memcpy(&targetDataStartPos, (char *) record + dirPointerPos - UNSIGNED_SIZE, UNSIGNED_SIZE);

                    length = targetDataEndPos - targetDataStartPos;
                    offset = targetDataStartPos;
                }

                // set found flag to true
                found = true;
            } else {
                isFirstAttr = false;
            }

            // mv dir pointer fwd
            dirPointerPos += UNSIGNED_SIZE;
        } else {
            // if attr not exist, do nothing
        }
    }

    if (!found) {
        return -1;
    } else {
        memcpy((char *) data, (char *) record + offset, length);
    }

    return 0;
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
    memcpy(block, (char *) data + (isRecord ? UNSIGNED_SIZE + REDIRECT_INDICATOR_SIZE : 0), nullIndicatorSize);
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

    for (unsigned i = 1; i <= totalSlot; i++) {
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

bool RecordBasedFileManager::isRedirected(void *record) {
    unsigned char redirectFlag;
    memcpy(&redirectFlag, (char *) record, REDIRECT_INDICATOR_SIZE);
    return redirectFlag == 0x01;
}

void RecordBasedFileManager::rightShiftRecord(void *data, unsigned startOffset, unsigned int length,
                                              unsigned int updatedLength) {
    unsigned totalSlot = getTotalSlot(data);

    for (unsigned i = 1; i <= totalSlot; i++) {
        unsigned recordOffset;
        unsigned recordLength;
        getOffsetAndLength(data, i, recordOffset, recordLength);
        if (recordOffset > startOffset) {
            recordOffset += updatedLength - length;
            setOffsetAndLength(data, i, recordOffset, recordLength);
        }
    }

    unsigned freeSpace = getFreeSpace(data);
    unsigned totalLength = PAGE_SIZE - freeSpace - totalSlot * DICT_SIZE - 2 * UNSIGNED_SIZE - startOffset - length;

    // shift whole record
    memmove((char *)data + startOffset + updatedLength, (char *)data + startOffset + length, totalLength);
}

void RecordBasedFileManager::getRIDFromRedirectedRecord(void *record, RID &rid) {
    unsigned pageNum;
    unsigned slotNum;
    memcpy(&pageNum, (char *) record + REDIRECT_INDICATOR_SIZE, UNSIGNED_SIZE);
    memcpy(&slotNum, (char *) record + REDIRECT_INDICATOR_SIZE + UNSIGNED_SIZE, UNSIGNED_SIZE);
    rid.pageNum = pageNum;
    rid.slotNum = slotNum;
}

RC RecordBasedFileManager::readRecordFromPage(void *data, void *record, unsigned slotNum) {
    unsigned offset;
    unsigned length;

    getOffsetAndLength(data, slotNum, offset, length);

    if (length == 0) {
        return -1;
    }

    memcpy(record, (char *) data + offset, length);
    return 0;
}

void RecordBasedFileManager::readRIDFromRecord(void *record, RID &rid) {
    memcpy(&rid.pageNum, (char *)record + REDIRECT_INDICATOR_SIZE, UNSIGNED_SIZE);
    memcpy(&rid.slotNum, (char *)record + REDIRECT_INDICATOR_SIZE + UNSIGNED_SIZE, UNSIGNED_SIZE);
}

void RecordBasedFileManager::createRIDRecord(void *record, RID &rid) {
    unsigned char indicator = 0x01;
    memcpy((char *)record, &indicator, REDIRECT_INDICATOR_SIZE);
    memcpy((char *)record + REDIRECT_INDICATOR_SIZE, &rid.pageNum, UNSIGNED_SIZE);
    memcpy((char *) record + REDIRECT_INDICATOR_SIZE + UNSIGNED_SIZE, &rid.slotNum, UNSIGNED_SIZE);
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                const std::vector<std::string> &attributeNames, RBFM_ScanIterator &rbfm_ScanIterator) {
    rbfm_ScanIterator.fileHandle = &fileHandle;
    rbfm_ScanIterator.rid.pageNum = SCAN_INIT_SLOT_NUM;
    rbfm_ScanIterator.rid.slotNum = SCAN_INIT_PAGE_NUM;
    rbfm_ScanIterator.attributeNames = attributeNames;
    rbfm_ScanIterator.recordDescriptor = recordDescriptor;

    return 0;
}

RBFM_ScanIterator::RBFM_ScanIterator() {
    rbfm = &RecordBasedFileManager::instance();
}

RC RBFM_ScanIterator::getNextRecord(RID &nextRID, void *data) {
    rbfm->readRecord(*fileHandle, recordDescriptor, rid, data);

    unsigned totalPageNum = fileHandle->getNumberOfPages();
    void* pageData = malloc(PAGE_SIZE);

    // move slotNum one step forward
    rid.slotNum += 1;

    while (rid.pageNum <= totalPageNum) {
        fileHandle->readPage(rid.pageNum, pageData);
        unsigned totalSlot = rbfm->getTotalSlot(pageData);

        while (rid.slotNum <= totalSlot) {
            if (isCurRIDValid(data)) {
                return 0;
            } else {
                rid.slotNum += 1;
            }
        }
        rid.pageNum += 1;
    }

    free(pageData);
    return RBFM_EOF;
};

RC RBFM_ScanIterator::close() { return -1; }

bool RBFM_ScanIterator::isCurRIDValid(void *data) {
    unsigned offset;
    unsigned length;
    rbfm->getOffsetAndLength(data, rid.slotNum, offset, length);

    return length == 0;
};

