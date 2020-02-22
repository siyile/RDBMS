#include <stack>
#include "ix.h"
#include <math.h>

IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {
    return PagedFileManager::instance().createFile(fileName);
}

RC IndexManager::destroyFile(const std::string &fileName) {
    return PagedFileManager::instance().destroyFile(fileName);
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
    RC rc = PagedFileManager::instance().openFile(fileName, ixFileHandle.fileHandle);
    ixFileHandle._readRootPageNum();
    return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
    if (ixFileHandle.fileHandle.fs.is_open()) {
        ixFileHandle._writeRootPageNum();
    } else {
        return -1;
    }
    return PagedFileManager::instance().closeFile(ixFileHandle.fileHandle);
}

/*
 * Tree logic:  a <= x < b, first value inclusive, last exclusive
 *
 * IF node is leaf node = <KEY, INDICATOR, RID> <1, key_size, 9> (bytes)
 * IF node is not leaf node = <KEY, INDICATOR, PAGE_NUM> <1, key_size, 4> (bytes)
 * INDICATOR include DELETE_INDICATOR
 * Slot <OFFSET, LENGTH>
 *
 * PAGE DESIGN
 * [LEAVE_NODE, LEAVE_NODE, ...
 *
 *
 *      ..., SLOT, SLOT, LEAF_LAYER_FLAG, Free_Space, SLOT_NUM, NEXT_PAGE_NUM]
 *
 *
 * search to find the bucket to insert record
 *
 * IF the bucket is not full
 *  Then insert record
 *  ELSE do split bucket
 *          original node have ceil((L + 1) / 2) nodes
 *          new node has floor((L + 1) / 2) nodes
 *       move the ceil((L + 1) / 2) node to parent, repeat the process
 *
 * IF the root splits, threat it as if it has an empty parent, then split
 */

/*
 * search from the pseudo root to find the leaves, save the intermediate page in a stack
 *
 *
 * WHILE there is not enough space for the node:
 *      Copy original PAGE TO PAGE1
 *      Set i = 0
 *      WHILE (i <= ceil((L + 1) / 2)):
 *          IF NOT found:
 *              IF node > node_i:
 *                  CONTINUE
 *              ELSE:
 *                  insert node into PAGE, i--, found = True
 *          ELSE:
 *              write node_i into PAGE
 *          i++
 *
 *      Get the ceil((L + 1) / 2) node value from PAGE
 *
 *      PAGE2 = MALLOC(PAGE_SIZE)
 *
 *      IF is LEAVE_NODE:
 *          Set PAGE2 next page num to PAGE's next page number
 *          Set PAGE next page num to PAGE2
 *
 *      WHILE (i <= L):
 *          IF found == FALSE && node < node_i:
 *              insert node into PAGE2, i--, found = True
 *          ELSE:
 *              write node_i into PAGE2
 *
 *      node = create new node to insert into PARENT_PAGE
 *      PAGE = read PARENT_PAGE from stack
 *  END WHILE
 *
 *  IF there must be enough space for the node:
 *      find the slot, right shift data & left shift dictionary, then insert, end
 *
 */
RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    // search from the pseudo root to get the node
    std::stack<void *> parentPage;
    std::stack<unsigned> parentPageNum;

    // get page stack
    searchNodePage(ixFileHandle, key, attribute.type, parentPage, parentPageNum);
    unsigned pageNum = parentPageNum.top();
    parentPageNum.pop();
    void* pageData = parentPage.top();
    parentPageNum.pop();

    void* nodeData = malloc(PAGE_SIZE);
    unsigned nodeLength;
    keyToLeafNode(key, rid, nodeData, nodeLength, attribute.type);
    void *fakeKey = malloc(PAGE_SIZE);
    if (attribute.type != TypeVarChar) {
        memcpy(fakeKey, key, UNSIGNED_SIZE);
    } else {
        unsigned keyLength;
        memcpy(&keyLength, key, UNSIGNED_SIZE);
        memcpy(fakeKey, key, UNSIGNED_SIZE + keyLength);
    }

    // basic init
    unsigned freeSpace = getFreeSpace(pageData);
    // page3 is a copy of page 1 to retrieve origin node
    void* page3 = malloc(PAGE_SIZE);
    void* page1 = pageData;
    void* iNode = malloc(PAGE_SIZE);
    bool isLeaf = true;

    // back-up variable
    void* nodePassToParent = malloc(PAGE_SIZE);
    unsigned nodePassToParentLength = 0;

    while (freeSpace < nodeLength + SLOT_SIZE) {
        // copy whole page to page3
        memcpy(page3, page1, PAGE_SIZE);
        // i is the node in original order, use it in page3
        // j is the node for current order, use it in page1
        unsigned i = 0, j = 0, L = getTotalSlot(page1);
        bool found = false;
        unsigned firstPageNum = ceil((L + 1) * 1.0 / 2);
        unsigned offset, length;

        // spilt process for the first page(PAGE_DATA)
        while (j < firstPageNum) {
            getSlotOffsetAndLength(page3, i, offset, length);
            getNodeData(page3, iNode, offset, length);
            if (!found) {
                int compareRes = compareMemoryBlock(fakeKey, iNode, length, attribute.type, isLeaf);
                if (compareRes > 0) {
                    i++;
                    j++;
                    continue;
                } else {
                    // insert node into PAGE
                    setSlotOffsetAndLength(page1, j, offset, nodeLength);
                    setNodeData(page1, nodeData, offset, nodeLength);
                    j++;
                    found = true;
                }
            } else {
                // write node_i into PAGE
                offset += nodeLength;
                setSlotOffsetAndLength(page1, j, offset + nodeLength, length);
                setNodeData(page1, iNode, offset, length);
                i++;
                j++;
            }
        }

        // store & prepare for insert in parent level, fill in node & node length
        // IF node is not leaf node = <INDICATOR, KEY, PAGE_NUM> <1, key_size, 4> (bytes)
        // IF node is leaf node = <INDICATOR, KEY, RID> <1, key_size, 9> (bytes)
        if (isLeaf) {
            getSlotOffsetAndLength(page1, firstPageNum - 1, offset, length);
            getNodeData(page1, nodePassToParent, offset, length);
            // substitute RID into PAGE_NUM
            memcpy((char *) nodePassToParent + length - RID_SIZE, &pageNum, UNSIGNED_SIZE);
            nodePassToParentLength = nodeLength - RID_SIZE + UNSIGNED_SIZE;
        } else {
            getSlotOffsetAndLength(page1, firstPageNum - 1, offset, length);
            getNodeData(page1, nodePassToParent, offset, length);
            memcpy((char *) nodePassToParent + length - UNSIGNED_SIZE, &pageNum, UNSIGNED_SIZE);
            nodePassToParentLength = nodeLength;
        }

        if (isLeaf) {
            // write nextPageNum for page1, page1 link to page2
            setNextPageNum(page1, ixFileHandle.getNumberOfPages());
        }

        // split process for second page(PAGE2)
        void *page2 = malloc(PAGE_SIZE);
        j = 0;
        unsigned offsetInPage1, _;
        unsigned page2ExtraOffset = 0;
        getSlotOffsetAndLength(page3, i, offsetInPage1, _);
        while (i < L) {
            getSlotOffsetAndLength(page3, i, offset, length);
            getNodeData(page3, iNode, offset, length);
            if (!found && compareMemoryBlock(fakeKey, iNode, length, attribute.type, true) < 0) {
                setSlotOffsetAndLength(page2, j, offset - offsetInPage1, nodeLength);
                setNodeData(page2, nodeData, offset - offsetInPage1, nodeLength);
                found = true;
                j++;
            }  else {
                // write node_i into PAGE2
                setSlotOffsetAndLength(page2, j, offset - offsetInPage1 + page2ExtraOffset, length);
                setNodeData(page2, iNode, offset - offsetInPage1 + page2ExtraOffset, length);
                i++;
                j++;
            }
        }

        // write page1, page2 into memory
        ixFileHandle.writePage(pageNum, page1);
        ixFileHandle.appendPage(page2);

        memcpy(nodeData, nodePassToParent, PAGE_SIZE);
        nodeLength = nodePassToParentLength;

        free(page2);
        free(page1);

        // if the root is also full, split it.
        if (parentPage.empty()) {
            // init new page assign to page1, change the rootPageNum
            initNewPage(ixFileHandle, page1, pageNum, false);
            ixFileHandle.rootPageNum = pageNum;
        } else {
            page1 = parentPage.top();
            parentPage.pop();
            pageNum = parentPageNum.top();
            parentPage.pop();
        }

        isLeaf = false;

        // convert node to key, ATTENTION: node must be none leaf node
        // IF node is not leaf node = <INDICATOR, KEY, PAGE_NUM> <1, key_size, 4> (bytes)
        if (attribute.type != TypeVarChar) {
            memcpy(fakeKey, (char *) nodeData + NODE_INDICATOR_SIZE, UNSIGNED_SIZE);
        } else {
            unsigned keyLength = nodeLength - NODE_INDICATOR_SIZE - UNSIGNED_SIZE;
            memcpy(fakeKey, &keyLength, UNSIGNED_SIZE);
            memcpy((char *) fakeKey + UNSIGNED_SIZE, (char *) nodeData + NODE_INDICATOR_SIZE, keyLength);
        }

        freeSpace = getFreeSpace(page1);
        // end main while
    }

    if (freeSpace >= nodeLength + SLOT_SIZE) {
        unsigned startSlot = searchLeafNode(page1, key, attribute.type, GT_OP);
        unsigned startSlotOffset, _;
        getSlotOffsetAndLength(page1, startSlot, startSlotOffset, _);

        // right shift slot & left shift dictionary
        rightShiftSlot(page1, startSlot, nodeLength);
        // set dictionary & slot
        setSlotOffsetAndLength(page1, startSlot, startSlotOffset, nodeLength);
        setNodeData(page1, nodeData, startSlotOffset, nodeLength);

        // write back to memory
        ixFileHandle.writePage(pageNum, page1);
    } else {
        throw std::logic_error("logic in insert entry is not right!");
    }

    // pageData = page1, no need to free
    free(page1);
    free(page3);
    free(nodeData);
    free(iNode);
    free(nodePassToParent);

    while (!parentPage.empty()) {
        free(parentPage.top());
        parentPage.pop();
    }

    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    return -1;
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {
}

RC IndexManager::searchNodePage(IXFileHandle &ixFileHandle, const void *key, AttrType type, std::stack<void *> parents,
                                std::stack<unsigned> parentsPageNum) {
    unsigned curPageNum = ixFileHandle.rootPageNum;
    void *pageData = malloc(PAGE_SIZE);
    // slot nodeData
    void *nodeData = malloc(PAGE_SIZE);
    ixFileHandle.readPage(curPageNum, pageData);

    unsigned totalSlot;
    while (!isLeafLayer(pageData)) {
        totalSlot = getTotalSlot(pageData);
        for (unsigned i = 0; i < totalSlot; i++) {
            unsigned offset, length;
            getSlotOffsetAndLength(pageData, i, offset, length);
            getNodeData(pageData, nodeData, offset, length);
            // if key <= slotData, we found next child, otherwise if current slot is last slot, must in there
            if (i == totalSlot - 1 || compareMemoryBlock(key, nodeData, length, type, false) <= 0) {
                unsigned nextPageNum = getNextPageFromNotLeafNode(nodeData);

                void *parentPage = malloc(PAGE_SIZE);
                memcpy(parentPage, pageData, PAGE_SIZE);
                parents.push(parentPage);
                parentsPageNum.push(curPageNum);

                ixFileHandle.readPage(nextPageNum, pageData);
                curPageNum = nextPageNum;
                break;
            }
        }
    }

    parents.push(pageData);
    parentsPageNum.push(curPageNum);

    unsigned slotNum = searchLeafNode(pageData, key, type, EQ_OP);
    free(nodeData);
    if (slotNum == NOT_VALID_UNSIGNED_SIGNAL)
        return 0;
    else
        return -1;
}

void IndexManager::initNewPage(IXFileHandle &ixFileHandle, void *data, unsigned &pageNum, bool isLeafLayer) {
    pageNum = ixFileHandle.getNumberOfPages();
    setFreeSpace(data, IX_INIT_FREE_SPACE);
    setTotalSlot(data, 0);
    setNextPageNum(data, NOT_VALID_UNSIGNED_SIGNAL);
    setLeafLayer(data, true);
}

unsigned IndexManager::getFreeSpace(void *data) {
    unsigned freeSpace;
    memcpy(&freeSpace, (char *) data + IX_FREE_SPACE_POS, UNSIGNED_SIZE);
    if (freeSpace >= IX_INIT_FREE_SPACE) {
        throw std::logic_error("Free space invalid");
    }
    return 0;
}

void IndexManager::setFreeSpace(void *data, unsigned freeSpace) {
    memcpy((char *) data + IX_FREE_SPACE_POS, &freeSpace, UNSIGNED_SIZE);
}

unsigned IndexManager::getTotalSlot(void *data) {
    unsigned totalSlot;
    memcpy(&totalSlot, (char *) data + IX_TOTAL_SLOT_POS, UNSIGNED_SIZE);
    if (totalSlot >= PAGE_SIZE / 2) {
        throw std::logic_error("TotalSlot number invalid.");
    }
    return totalSlot;
}

void IndexManager::setTotalSlot(void *data, unsigned totalSlot) {
    memcpy((char *) data + IX_TOTAL_SLOT_POS, &totalSlot, UNSIGNED_SIZE);
}

bool IndexManager::isLeafLayer(void *data) {
    unsigned char flag;
    memcpy(&flag, (char *) data + IX_LEAF_LAYER_FLAG_POS, UNSIGNED_CHAR_SIZE);
    return flag == LEAF_LAYER_FLAG;
}

void IndexManager::setLeafLayer(void *data, bool isLeafLayer) {
    unsigned char flag;
    if (isLeafLayer) {
        flag = LEAF_LAYER_FLAG;
    } else {
        flag = 0x00;
    }
    memcpy((char *) data + IX_LEAF_LAYER_FLAG_POS, &flag, UNSIGNED_CHAR_SIZE);
}

unsigned IndexManager::getNextPageNum(void *data) {
    unsigned nextPageNum;
    memcpy(&nextPageNum, (char *) data + IX_NEXT_PAGE_NUM_POS, UNSIGNED_SIZE);
    return nextPageNum;
}

void IndexManager::setNextPageNum(void *data, unsigned nextPageNum) {
    memcpy(&nextPageNum, (char *) data + IX_NEXT_PAGE_NUM_POS, UNSIGNED_SIZE);
}

void IndexManager::getSlotOffsetAndLength(void *data, unsigned slotNum, unsigned &offset, unsigned &length) {
    unsigned pos = IX_LEAF_LAYER_FLAG_POS - slotNum * UNSIGNED_SIZE * 2;
    memcpy(&offset, (char *) data + pos, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;
    memcpy(&length, (char *) data + pos, UNSIGNED_SIZE);
    if (offset >= PAGE_SIZE && length >= PAGE_SIZE) {
        throw std::logic_error("Slot offset or length invalid");
    }
}

void IndexManager::setSlotOffsetAndLength(void *data, unsigned slotNum, unsigned offset, unsigned length) {
    unsigned pos = IX_LEAF_LAYER_FLAG_POS - slotNum * UNSIGNED_SIZE * 2;
    memcpy((char *) data + pos, &offset, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;
    memcpy((char *) data + pos, &length, UNSIGNED_SIZE);
}

void IndexManager::getNodeData(void *pageData, void *data, unsigned offset, unsigned length) {
    memcpy(data, (char *) pageData + offset, length);
}

void IndexManager::setNodeData(void *pageData, void *data, unsigned offset, unsigned length) {
    memcpy((char *) pageData + offset, data, length);
}

/*
 * IF node is leaf node = <KEY, INDICATOR, RID> <1, key_size, 9> (bytes)
 * IF node is not leaf node = <KEY, INDICATOR, PAGE_NUM> <1, key_size, 4> (bytes)
 */
int IndexManager::compareMemoryBlock(const void *key, void *slotData, unsigned slotLength, AttrType type, bool isLeaf) {
    if (type == TypeInt) {
        int keyInt;
        int blockInt;
        memcpy(&keyInt, key, UNSIGNED_SIZE);
        memcpy(&blockInt, (char *) slotData + NODE_INDICATOR_SIZE, UNSIGNED_SIZE);
        if (keyInt > blockInt)
            return 1;
        else if (blockInt > keyInt)
            return -1;
        else
            return 0;
    } else if (type == TypeReal) {
        float keyFloat;
        float blockFloat;
        memcpy(&keyFloat, key, UNSIGNED_SIZE);
        memcpy(&blockFloat, (char *) slotData + NODE_INDICATOR_SIZE, UNSIGNED_SIZE);
        if (keyFloat > blockFloat)
            return 1;
        else if (blockFloat > keyFloat)
            return -1;
        else
            return 0;
    } else {
        unsigned keyLength;
        memcpy(&keyLength, key, UNSIGNED_SIZE);
        std::string keyString((char *) key + UNSIGNED_SIZE, keyLength);
        std::string blockString((char *) slotData + NODE_INDICATOR_SIZE,
                                slotLength - NODE_INDICATOR_SIZE - (isLeaf ? RID_SIZE : UNSIGNED_SIZE));
        int res = keyString.compare(blockString);
        if (res > 0)
            return 1;
        else if (res < 0)
            return -1;
        else
            return 0;
    }
}

// IF node is not leaf node = <KEY, INDICATOR, PAGE_NUM> <4, 1, 4> (bytes)
unsigned IndexManager::getNextPageFromNotLeafNode(void *data) {
    unsigned nextPage;
    memcpy(&nextPage, (char *) data + UNSIGNED_SIZE + NODE_INDICATOR_SIZE, UNSIGNED_SIZE);
    if (nextPage > 200000) {
        throw std::logic_error("Next page number is invalid!");
    }
    return nextPage;
}

void IndexManager::rightShiftSlot(void *data, unsigned startSlot, unsigned shiftLength) {
    unsigned totalSlot = getTotalSlot(data);
    unsigned endSlot = totalSlot - 1;

    unsigned startSlotOffset, _;
    unsigned endSlotOffset, endSlotLength;

    getSlotOffsetAndLength(data, startSlot, startSlotOffset, _);
    getSlotOffsetAndLength(data, endSlot, endSlotOffset, endSlotLength);

    // change dictionary
    for (unsigned i = startSlot; i < totalSlot; i++) {
        unsigned offset, length;
        getSlotOffsetAndLength(data, i, offset, length);
        setSlotOffsetAndLength(data, i, offset + shiftLength, length);
    }

    // shift whole slot & dictionary
    unsigned startDict = IX_LEAF_LAYER_FLAG_POS - SLOT_SIZE * totalSlot;
    unsigned endDict = IX_LEAF_LAYER_FLAG_POS - SLOT_SIZE * startSlot;

    memmove((char *) data + startSlotOffset, (char *) data + startSlotOffset + shiftLength,
            endSlotOffset + endSlotLength - startSlotOffset);
    memmove((char *) data + startDict, (char *) data + startDict - shiftLength, endDict - startDict + SLOT_SIZE);
}

unsigned IndexManager::searchLeafNode(void *data, const void *key, AttrType type, CompOp compOp) {
    unsigned totalSlot = getTotalSlot(data);
    void *slotData = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < totalSlot; i++) {
        unsigned offset, length;
        getSlotOffsetAndLength(data, i, offset, length);
        getNodeData(data, slotData, offset, length);
        if (compOp == EQ_OP && compareMemoryBlock(key, slotData, length, type, true) == 0) {
            free(slotData);
            return i;
        } else if (compOp == GT_OP && compareMemoryBlock(key, slotData, length, type, true) > 0){
            free(slotData);
            return i;
        }
    }

    if (compOp == EQ_OP) {
        free(slotData);
        return NOT_VALID_UNSIGNED_SIGNAL;
    } else if (compOp == GT_OP) {
        return totalSlot;
    } else {
        throw std::logic_error("CompOp is not valid!");
    }
}

/*
 * IF node is leaf node = <INDICATOR, KEY, RID> <1, key_size, 9> (bytes)
 * IF node is not leaf node = <INDICATOR, KEY, PAGE_NUM> <1, key_size, 4> (bytes)
 * INDICATOR include DELETE_INDICATOR
 */
void IndexManager::keyToLeafNode(const void *key, const RID &rid, void *data, unsigned &length, AttrType type) {
    unsigned keySize = 4;
    unsigned pos = 0;
    if (type == TypeVarChar) {
        memcpy(&keySize, key, UNSIGNED_SIZE);
        pos += UNSIGNED_SIZE;
    }

    // current indicator only for delete
    unsigned char indicator = 0x00;
    memcpy(data, &indicator, UNSIGNED_CHAR_SIZE);
    memcpy((char *) data + NODE_INDICATOR_SIZE, (char *) key + pos, keySize);

    pos = NODE_INDICATOR_SIZE + keySize;

    memcpy((char *) data + pos, &rid, RID_SIZE);
    pos += RID_SIZE;

    length = pos;
}

void IndexManager::keyToNoneLeafNode(const void *key, unsigned pageNum, void *data, unsigned &length, AttrType type) {
    unsigned keySize = 4;
    unsigned pos = 0;
    if (type == TypeVarChar) {
        memcpy(&keySize, key, UNSIGNED_SIZE);
        pos += UNSIGNED_SIZE;
    }

    // current indicator only for delete
    unsigned char indicator = 0x00;
    memcpy(data, &indicator, UNSIGNED_CHAR_SIZE);
    memcpy((char *) data + NODE_INDICATOR_SIZE, (char *) key + pos, keySize);

    pos = NODE_INDICATOR_SIZE + keySize;

    memcpy((char *) data + pos, &pageNum, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    length = pos;
}



IX_ScanIterator::IX_ScanIterator() {
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    return -1;
}

RC IX_ScanIterator::close() {
    return -1;
}

IXFileHandle::IXFileHandle() {
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle() {
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
    return fileHandle.collectCounterValues(ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter);
}

unsigned IXFileHandle::getNumberOfPages() {
    return fileHandle.getNumberOfPages();
}

RC IXFileHandle::readPage(PageNum pageNum, void *data) {
    return fileHandle.readPage(pageNum, data);
}

RC IXFileHandle::writePage(PageNum pageNum, const void *data) {
    return fileHandle.writePage(pageNum, data);
}

RC IXFileHandle::appendPage(const void *data) {
    return fileHandle.appendPage(data);
}

void IXFileHandle::_readRootPageNum() {
    fileHandle.fs.seekg(UNSIGNED_SIZE * 3, std::ios::beg);
    fileHandle.fs.read(reinterpret_cast<char *>(&rootPageNum), UNSIGNED_SIZE);
}

void IXFileHandle::_writeRootPageNum() {
    fileHandle.fs.seekp(UNSIGNED_SIZE * 3, std::ios::beg);
    fileHandle.fs.write(reinterpret_cast<char *>(&rootPageNum), UNSIGNED_SIZE);
}
