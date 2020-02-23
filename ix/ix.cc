#include <stack>
#include "ix.h"
#include <math.h>
#include <iostream>

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
 * IF node is leaf node = <INDICATOR, KEY, RID> <1, key_size, 9> (bytes)
 * IF node is not leaf node = <INDICATOR, KEY, PAGE_NUM> <1, key_size, 4> (bytes)
 * INDICATOR include DELETE_INDICATOR
 * Slot <OFFSET, LENGTH>
 *
 * PAGE DESIGN
 * [NODE, NODE, ...
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
    unsigned slotNum = searchLeafNodePage(ixFileHandle, key, attribute.type, parentPage, parentPageNum, true);

    unsigned pageNum = parentPageNum.top();
    parentPageNum.pop();
    void *pageData = parentPage.top();
    parentPageNum.pop();

    // if the number is not valid, just replace it
    if (slotNum != NOT_VALID_UNSIGNED_SIGNAL) {
        // already have number, return INVALID
        if (checkNodeNumValid(pageData, slotNum)) {
            free(pageData);
            freeParentsPageData(parentPage);
            return -1;
        } else {
            void* nodeData = malloc(PAGE_SIZE);
            unsigned offset, length;
            getNodeDataAndOffsetAndLength(pageData, nodeData, slotNum, offset, length);
            unsigned char indicator = NORMAL_FLAG;

            // replace indicator & rid
            memcpy(nodeData, &indicator, NODE_INDICATOR_SIZE);
            memcpy((char *) nodeData + length - RID_SIZE, &rid, RID_SIZE);

            // write node into page, write back page into file
            setNodeData(pageData, nodeData, offset, length);
            ixFileHandle.writePage(pageNum, pageData);

            free(nodeData);
            free(pageData);
            freeParentsPageData(parentPage);
            return 0;
        }
    }

    void *nodeData = malloc(PAGE_SIZE);
    unsigned nodeLength;
    keyToLeafNode(key, rid, nodeData, nodeLength, attribute.type);
    // create copy of key, since key is const
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
    void *page3 = malloc(PAGE_SIZE);
    void *page1 = pageData;
    unsigned page1Num = pageNum;
    unsigned page2Num = 0;
    bool newRootPageCreated = false;
    void *iNode = malloc(PAGE_SIZE);
    bool isLeaf = true;

    // back-up variable
    void *nodePassToParent = malloc(PAGE_SIZE);
    unsigned nodePassToParentLength = 0;

    while (freeSpace < nodeLength + SLOT_SIZE) {
        // copy page1 to page3
        memcpy(page3, page1, PAGE_SIZE);
        // i is the node in original order, use it in page3
        // j is the node for current order, use it in page1
        unsigned i = 0, j = 0, L = getTotalSlot(page1);
        bool found = false;
        unsigned firstPageNum = ceil((L + 1) * 1.0 / 2);
        unsigned offset, length;
        unsigned page1FreeSpace = IX_INIT_FREE_SPACE;

        // spilt process for the first page(PAGE1)
        while (j < firstPageNum) {
            getNodeDataAndOffsetAndLength(page3, iNode, i, offset, length);
            if (!found) {
                int compareRes = compareMemoryBlock(fakeKey, iNode, length, attribute.type, isLeaf);
                if (compareRes > 0) {
                    page1FreeSpace -= length;
                    i++;
                    j++;
                    continue;
                } else {
                    // insert node into PAGE
                    setSlotOffsetAndLength(page1, j, offset, nodeLength);
                    setNodeData(page1, nodeData, offset, nodeLength);
                    page1FreeSpace -= length;
                    j++;
                    found = true;
                }
            } else {
                // write node_i into PAGE
                offset += nodeLength;
                setSlotOffsetAndLength(page1, j, offset, length);
                setNodeData(page1, iNode, offset, length);
                page1FreeSpace -= length;
                i++;
                j++;
            }
        }

        // set page1 TotalSlot & freeSpace
        setTotalSlot(page1, firstPageNum);
        setFreeSpace(page1, page1FreeSpace);


        // store & prepare for insert in parent level, fill in node & node length
        // IF node is not leaf node = <INDICATOR, KEY, PAGE_NUM> <1, key_size, 4> (bytes)
        // IF node is leaf node = <INDICATOR, KEY, RID> <1, key_size, 9> (bytes)
        getNodeDataAndOffsetAndLength(page1, nodePassToParent, firstPageNum - 1, offset, length);
        if (isLeaf) {
            // substitute RID into PAGE_NUM
            memcpy((char *) nodePassToParent + length - RID_SIZE, &page1Num, UNSIGNED_SIZE);
            nodePassToParentLength = nodeLength - RID_SIZE + UNSIGNED_SIZE;
        } else {
            memcpy((char *) nodePassToParent + length - UNSIGNED_SIZE, &page1Num, UNSIGNED_SIZE);
            nodePassToParentLength = nodeLength;
        }

        void *page2 = malloc(PAGE_SIZE);
        page2Num = ixFileHandle.getNumberOfPages();

        if (isLeaf) {
            // write nextPageNum for page1, page1 link to page2
            setNextPageNum(page1, page2Num);
        }

        // split process for second page(PAGE2)
        j = 0;
        unsigned offsetInPage1, _;
        unsigned page2ExtraOffset = 0;
        getSlotOffsetAndLength(page3, i, offsetInPage1, _);
        while (i < L) {
            getNodeDataAndOffsetAndLength(page3, iNode, i, offset, length);
            if (!found && compareMemoryBlock(fakeKey, iNode, length, attribute.type, true) < 0) {
                addNode(page2, nodeData, j, offset - offsetInPage1, nodeLength);
                found = true;
                page2ExtraOffset = nodeLength;
                j++;
            } else {
                // write node_i into PAGE2
                addNode(page2, iNode, j, offset - offsetInPage1 + page2ExtraOffset, length);
                i++;
                j++;
            }
        }

        // write page1, page2 into memory
        ixFileHandle.writePage(page1Num, page1);
        ixFileHandle.appendPage(page2);

        // retrieve back-up data prepare for passing into parents
        memcpy(nodeData, nodePassToParent, PAGE_SIZE);
        nodeLength = nodePassToParentLength;

        free(page2);
        free(page1);

        // if the root is also full, split it.
        if (parentPage.empty()) {
            // init new page assign to page1, change the rootPageNum
            initNewPage(ixFileHandle, page1, page1Num, false, TypeVarChar);
            ixFileHandle.rootPageNum = page1Num;
            newRootPageCreated = true;
        } else {
            page1 = parentPage.top();
            parentPage.pop();
            page1Num = parentPageNum.top();
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
        // start slot is the leftest slot moving right
        unsigned startSlot;
        startSlot = searchNode(page1, fakeKey, attribute.type, LT_OP, isLeaf);

        // if we create new root page, we need to assign 1 more page number to its slot
        // i.e. add page2Num to the start slot, i.e. the MAX VALUE slot
        if (newRootPageCreated) {
            unsigned offset, length;
            void* maxNodeData = malloc(PAGE_SIZE);
            getNodeDataAndOffsetAndLength(page1, maxNodeData, startSlot, offset, length);
            // modify pageNum in none leaf node
            memcpy((char *) maxNodeData + (length - UNSIGNED_SIZE), &page2Num, UNSIGNED_SIZE);
            // write back to page1
            setNodeData(page1, maxNodeData, offset, length);
            free(maxNodeData);
        }

        unsigned startSlotOffset, _;
        getSlotOffsetAndLength(page1, startSlot, startSlotOffset, _);

        // right shift slot & left shift dictionary
        rightShiftSlot(page1, startSlot, nodeLength);
        // add new node
        addNode(page1, nodeData, startSlot, startSlotOffset, nodeLength);

        // write back to memory
        ixFileHandle.writePage(page1Num, page1);
    } else {
        throw std::logic_error("logic in insert entry is not right!");
    }

    // pageData = page1, no need to free
    free(page1);
    free(page3);
    free(nodeData);
    free(iNode);
    free(nodePassToParent);
    freeParentsPageData(parentPage);

    return 0;
}

RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    std::stack<void *> parents;
    std::stack<unsigned> parentsPageNum;
    unsigned slotNum = searchLeafNodePage(ixFileHandle, key, attribute.type, parents, parentsPageNum, false);
    if ( slotNum == NOT_VALID_UNSIGNED_SIGNAL) {
        free(parents.top());
        return -1;
    }

    unsigned pageNum = parentsPageNum.top();
    void *pageData = parents.top();

    setNodeInvalid(pageData, slotNum);
    ixFileHandle.writePage(pageNum, pageData);

    free(parents.top());

    return 0;
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    ix_ScanIterator.lowKey = lowKey;
    ix_ScanIterator.highKey = highKey;
    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;
    ix_ScanIterator.attribute = attribute;
    ix_ScanIterator.ixFileHandle = &ixFileHandle;
    ix_ScanIterator.slotNum = 0;

    std::stack<void *> parents;
    std::stack<unsigned> _;
    searchLeafNodePage(ixFileHandle, lowKey, attribute.type, parents, _, false);

    ix_ScanIterator.pageData = parents.top();

    if (lowKeyInclusive) {
        ix_ScanIterator.slotNum = searchNode(ix_ScanIterator.pageData, lowKey, attribute.type, GE_OP, true);
    } else {
        ix_ScanIterator.slotNum = searchNode(ix_ScanIterator.pageData, lowKey, attribute.type, GT_OP, true);
    }

    return 0;
}

/*
 * {"keys":["P"],
    "children":[
        {"keys":["C","G","M"],
         "children": [
            {"keys": ["A:[(1,1),(1,2)]","B:[(2,1),(2,2)]"]},
            {"keys": ["D:[(3,1),(3,2)]","E:[(4,1)]","F:[(5,1)]"]},
            {"keys": ["J:[(5,1),(5,2)]","K:[(6,1),(6,2)]","L:[(7,1)]"]},
            {"keys": ["N:[(8,1)]","O:[(9,1)]"]}
        ]},
        {"keys":["T","X"],
         "children": [
            {"keys": ["Q:[(10,1)]","R:[(11,1)]","S:[(12,1)]"]},
            {"keys": ["U:[(13,1)]","V:[(14,1)]"]},
            {"keys": ["Y:[(15,1)]","Z:[(16,1)]"]}
        ]}
    ]}
*/

/*
 * preOrder traversal
 *  IF node is leaf node = <INDICATOR, KEY, RID> <1, key_size, 9> (bytes)
 *  IF node is not leaf node = <INDICATOR, KEY, PAGE_NUM> <1, key_size, 4> (bytes)
 *
 * func preOrder:
 *  IF node is leaf node:
 *      check valid, then print all the LEAF KEY & RID
 *      RETURN
 *  IF node is none leaf node:
 *      PRINT all keys in keys.
 *      PRINT all children
 *
 *  PRINT "children ["
 *
 *  For node in nodes:
 *      preOrder(node)
 *
 *
 * */
void IndexManager::printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const {

}

void IndexManager::preOrderPrint(IXFileHandle *ixFileHandle, unsigned pageNum, AttrType type, unsigned level) {
    void* pageData = malloc(PAGE_SIZE);
    void* nodeData = malloc(PAGE_SIZE);
    ixFileHandle->readPage(pageNum, pageData);
    unsigned totalSlot = getTotalSlot(pageData);
    unsigned offset, length;
    void* key = malloc(PAGE_SIZE);
    bool leafLayer = isLeafLayer(pageData);

    std::cout<< indentation(level) << "{\"keys\": [";
    if (leafLayer) {
        // print "keys": ["Q:[(10,1)], Deleted","R:[(11,1)]","S:[(12,1)]"]
        bool fistFound = false;
        for (unsigned i = 0; i < totalSlot; ++i) {
            if (!checkNodeNumValid(pageData, i)) {
                continue;
            }
            if (fistFound) std::cout << ",";
            RID rid;
            leafNodeToKey(pageData, i, key, rid, type);
            std::cout << "\"";
            printKey(key, type);
            std::cout << ":";
            printRID(rid);
            std::cout << "\"";
            fistFound = true;
        }
        std::cout << "]}";
    } else {
        // "keys":["P", "G"],
        std::vector<unsigned> pageNums;
        for (unsigned i = 0; i < totalSlot; ++i) {
            if (i != 0) std::cout << ",";
            noneLeafNodeToKey(pageData, i, key, pageNum, type);
            pageNums.push_back(pageNum);
            std::cout << "\"";
            printKey(key, type);
            std::cout << "\"";
        }
        std::cout << "]},\n" << indentation(level) << "\"children\": [\n";
        bool first = true;
        for (auto num : pageNums) {
            if (!first) {
                std::cout << ",\n";
            }
            preOrderPrint(ixFileHandle, num, type, level + 1);
            first = false;
        }
        std::cout << "\n";

        std::cout << indentation(level) << "]}";
    }

    free(nodeData);
    free(pageData);
    free(key);
}

std::string IndexManager::indentation(unsigned num) {
    std::string string;
    for (unsigned i = 0; i < num; ++i) {
        string += "\t";
    }
    return string;
}


void IndexManager::printKey(void *key, AttrType type) {
    if (type == TypeReal) {
        float x;
        memcpy(&x, key, UNSIGNED_SIZE);
        std::cout << x;
    } else if (type == TypeInt) {
        int y;
        memcpy(&y, key, UNSIGNED_SIZE);
        std::cout << y;
    } else {
        unsigned length;
        memcpy(&length, key, UNSIGNED_SIZE);
        std::string z ((char *) key + UNSIGNED_SIZE, length);
        std::cout << z;
    }
}

// print "keys": ["Q:[(10,1)]","R:[(11,1)]","S:[(12,1)]"]
void IndexManager::printRID(RID &rid) {
    std::cout << "[(" << rid.pageNum << "," << rid.slotNum << ")]";
}


unsigned int
IndexManager::searchLeafNodePage(IXFileHandle &ixFileHandle, const void *key, AttrType type, std::stack<void *> parents,
                                 std::stack<unsigned> parentsPageNum, bool rememberParents) {
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
            getNodeDataAndOffsetAndLength(pageData, nodeData, i, offset, length);
            // if key <= slotData, we found next child, otherwise if current slot is last slot, must in there
            if (i == totalSlot - 1 || compareMemoryBlock(key, nodeData, length, type, false) <= 0) {
                unsigned nextPageNum = getNextPageFromNotLeafNode(nodeData);

                void *parentPage = malloc(PAGE_SIZE);
                memcpy(parentPage, pageData, PAGE_SIZE);
                if (rememberParents) {
                    parents.push(parentPage);
                    parentsPageNum.push(curPageNum);
                }

                ixFileHandle.readPage(nextPageNum, pageData);
                curPageNum = nextPageNum;
                break;
            }
        }
    }

    parents.push(pageData);
    parentsPageNum.push(curPageNum);

    unsigned slotNum = searchNode(pageData, key, type, EQ_OP, true);
    free(nodeData);
    if (slotNum == NOT_VALID_UNSIGNED_SIGNAL)
        return NOT_VALID_UNSIGNED_SIGNAL;
    else
        return slotNum;
}

void
IndexManager::initNewPage(IXFileHandle &ixFileHandle, void *data, unsigned &pageNum, bool isLeafLayer, AttrType type) {
    pageNum = ixFileHandle.getNumberOfPages();
    setFreeSpace(data, IX_INIT_FREE_SPACE);
    setTotalSlot(data, 0);
    setNextPageNum(data, NOT_VALID_UNSIGNED_SIGNAL);
    setLeafLayer(data, true);
    // if is none leaf layer, insert initial MAX_VALUE_SIGNAL into it
    if (!isLeafLayer) {
        unsigned offset = 0, length = 0;
        // generate MAX_VALUE data
        void* key = malloc(20);
        void* nodeData = malloc(40);
        generateMaxValueNode(key, nodeData, length, type);
        addNode(data, nodeData, 0, offset, length);
    }
}


void IndexManager::generateMaxValueNode(void *key, void *nodeData, unsigned &length, AttrType type) {
    if (type == TypeVarChar) {
        unsigned x = 6;
        std::string y = MAX_VALUE_SIGNAL_STRING;
        memcpy(key, &x, UNSIGNED_SIZE);
        memcpy((char *) key + UNSIGNED_SIZE, y.c_str(), y.size());
    } else {
        unsigned z = MAX_VALUE_SIGNAL;
        memcpy(key, &z, UNSIGNED_SIZE);
    }

    keyToNoneLeafNode(key, NOT_VALID_UNSIGNED_SIGNAL, nodeData, length, type);
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

bool IndexManager::isLeafLayer(void *pageData) {
    unsigned char flag;
    memcpy(&flag, (char *) pageData + IX_LEAF_LAYER_FLAG_POS, UNSIGNED_CHAR_SIZE);
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

void IndexManager::getNodeDataAndOffsetAndLength(void *pageData, void *nodeData, unsigned slotNum, unsigned &offset,
                                                 unsigned &length) {
    getSlotOffsetAndLength(pageData, slotNum, offset, length);
    getNodeData(pageData, nodeData, offset, length);
}

void IndexManager::addNode(void *pageData, void *nodeData, unsigned slotNum, unsigned offset,
                           unsigned length) {
    setSlotOffsetAndLength(pageData, slotNum, offset, length);
    setNodeData(pageData, nodeData, offset, length);
    unsigned freeSpace = getFreeSpace(pageData);
    setFreeSpace(pageData, freeSpace - length);
    unsigned totalSlot = getTotalSlot(pageData);
    setTotalSlot(pageData, totalSlot + 1);
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
        if (blockInt == MAX_VALUE_SIGNAL)
            return -1;
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
        if (blockFloat == MAX_VALUE_SIGNAL)
            return -1;
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
        if (blockString == MAX_VALUE_SIGNAL_STRING)
            return -1;
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

unsigned int IndexManager::searchNode(void *data, const void *key, AttrType type, CompOp compOp, bool isLeaf) {
    unsigned totalSlot = getTotalSlot(data);
    void *nodeData = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < totalSlot; i++) {
        unsigned offset, length;
        getNodeDataAndOffsetAndLength(data, nodeData, i, offset, length);
        // if already deleted
        if (isLeaf && !checkNodeValid(nodeData))
            continue;

        int compareRes = compareMemoryBlock(key, nodeData, length, type, true);

        switch (compOp) {
            case EQ_OP:
                if (compareRes == 0) {
                    free(nodeData);
                    return i;
                }
                break;
            case GT_OP:
                if (compareRes > 0) {
                    free(nodeData);
                    return i;
                }
                break;
            case GE_OP:
                if (compareRes >= 0) {
                    free(nodeData);
                    return i;
                }
                break;
            case LT_OP:
                if (compareRes < 0) {
                    free(nodeData);
                    return i;
                }
                break;
        }
    }

    free(nodeData);

    if (compOp == EQ_OP) {
        return NOT_VALID_UNSIGNED_SIGNAL;
    } else if (compOp == GT_OP || compOp == GE_OP) {
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

// IF node is leaf node = <INDICATOR, KEY, RID> <1, key_size, 9> (bytes)
void IndexManager::leafNodeToKey(void *data, unsigned slotNum, void *key, RID &rid, AttrType type) {
    unsigned offset, length;
    getSlotOffsetAndLength(data, slotNum, offset, length);
    void *nodeData = malloc(PAGE_SIZE);
    getNodeData(data, nodeData, offset, length);

    unsigned keyLength = length - NODE_INDICATOR_SIZE - RID_SIZE;

    memcpy(&rid, (char *) nodeData + NODE_INDICATOR_SIZE + keyLength, RID_SIZE);
    if (type == TypeVarChar) {
        memcpy(key, &keyLength, UNSIGNED_SIZE);
    }
    memcpy((char *) key + (type == TypeVarChar ? 4 : 0), (char *) nodeData + NODE_INDICATOR_SIZE, keyLength);

    free(nodeData);
}

// IF node is not leaf node = <KEY, INDICATOR, PAGE_NUM> <1, key_size, 4> (bytes)
void IndexManager::noneLeafNodeToKey(void *data, unsigned slotNum, void *key, unsigned &pageNum, AttrType type) {
    unsigned offset, length;
    getSlotOffsetAndLength(data, slotNum, offset, length);
    void *nodeData = malloc(PAGE_SIZE);
    getNodeData(data, nodeData, offset, length);

    unsigned keyLength = length - NODE_INDICATOR_SIZE - UNSIGNED_SIZE;

    memcpy(&pageNum, (char *) nodeData + NODE_INDICATOR_SIZE + keyLength, UNSIGNED_SIZE);
    if (type == TypeVarChar) {
        memcpy(key, &keyLength, UNSIGNED_SIZE);
    }

    memcpy((char *) key + (type == TypeVarChar ? 4 : 0), (char *) nodeData + NODE_INDICATOR_SIZE, keyLength);

    free(nodeData);
}


bool IndexManager::checkNodeNumValid(void *data, unsigned slotNum) {
    unsigned offset, length;
    getSlotOffsetAndLength(data, slotNum, offset, length);

    void *nodeData = malloc(PAGE_SIZE);
    getNodeData(data, nodeData, offset, length);

    unsigned char indicator;
    memcpy(&indicator, nodeData, NODE_INDICATOR_SIZE);

    free(nodeData);
    return indicator != DELETE_FLAG;
}

void IndexManager::setNodeInvalid(void *data, unsigned slotNum) {
    unsigned offset, length;
    getSlotOffsetAndLength(data, slotNum, offset, length);

    unsigned char deleteFlag = DELETE_FLAG;
    memcpy((char *) data + offset, &deleteFlag, NODE_INDICATOR_SIZE);
}

bool IndexManager::checkNodeValid(void *data) {
    unsigned char deleteFlag = DELETE_FLAG;
    return memcmp(data, &deleteFlag, NODE_INDICATOR_SIZE) == 0;
}

void IndexManager::freeParentsPageData(std::stack<void *> parents) {
    while (!parents.empty()) {
        free(parents.top());
        parents.pop();
    }
}

IX_ScanIterator::IX_ScanIterator() {
    im = &IndexManager::instance();
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    unsigned totalSLot = im->getTotalSlot(pageData);
    bool found = false;
    while (!found) {
        while (slotNum < totalSLot) {
            if (!im->checkNodeNumValid(pageData, slotNum)) {
                slotNum += 1;
            } else {

                unsigned offset, length;
                im->getSlotOffsetAndLength(pageData, slotNum, offset, length);
                void *nodeData = malloc(PAGE_SIZE);
                im->getNodeData(pageData, nodeData, offset, length);

                if (highKeyInclusive) {
                    if (im->compareMemoryBlock(highKey, nodeData, length, attribute.type, true) < 0) {
                        free(nodeData);
                        return IX_EOF;
                    }
                } else {
                    if (im->compareMemoryBlock(highKey, nodeData, length, attribute.type, true) <= 0) {
                        free(nodeData);
                        return IX_EOF;
                    }
                }
                free(nodeData);

                found = true;
                break;
            }
        }
        if (!found) {
            unsigned nextPage = im->getNextPageNum(pageData);
            if (nextPage == NOT_VALID_UNSIGNED_SIGNAL) {
                return IX_EOF;
            }
            ixFileHandle->readPage(nextPage, pageData);
            slotNum = 0;
        }
    }

    im->leafNodeToKey(pageData, slotNum, key, rid, attribute.type);
    slotNum += 1;

    return 0;
}

RC IX_ScanIterator::close() {
    free(pageData);
    return 0;
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
