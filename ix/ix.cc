#include <stack>
#include "ix.h"
#include <math.h>
#include <iostream>

IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {
    RC rc =  PagedFileManager::instance().createFile(fileName);
    if (rc == -1)
        return -1;
    FileHandle fileHandle;
    PagedFileManager::instance().openFile(fileName, fileHandle);

    // write pseudo root page into file
    void* pseudoPage = malloc(PAGE_SIZE);
    unsigned rootPageNum = 1;
    memcpy(pseudoPage, &rootPageNum, UNSIGNED_SIZE);
    fileHandle.appendPage(pseudoPage);
    free(pseudoPage);

    PagedFileManager::instance().closeFile(fileHandle);

    IXFileHandle ixFileHandle;
    openFile(fileName, ixFileHandle);

    void* data = malloc(PAGE_SIZE);
    unsigned _;
    initNewPage(ixFileHandle, data, _, true);
    ixFileHandle.appendPage(data);
    free(data);

    closeFile(ixFileHandle);
    return 0;
}

RC IndexManager::destroyFile(const std::string &fileName) {
    return PagedFileManager::instance().destroyFile(fileName);
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
    RC rc = PagedFileManager::instance().openFile(fileName, ixFileHandle.fileHandle);
    if (rc == -1)
        return -1;
    ixFileHandle._readRootPageNum();
    return 0;
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
 * None-Leaf layer, always have a default node MIN_NODE, e.g.
 * [ MIN, 5, 20, 30 ...
 *
 *          ...]
 *  the page number in MIN means the leaf node in that page is MIN <= x < 5
 *  the page number in 5 means the leaf node in that page is 5 <= x < 20
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
    unsigned slotNum = searchLeafNodePage(ixFileHandle, key, attribute.type, parentPage, parentPageNum, true, false);

    unsigned pageNum = parentPageNum.top();
    parentPageNum.pop();
    void *pageData = parentPage.top();
    parentPage.pop();

    /*
     * if the key is already exist, we need to check all the record with the same key, to confirm whether we already insert this node.
     *
     * if yes, we check whether is deleted or not
     *      if yes, make the valid
     *      if not, return -1
     * if no, do normal insertion
     *
     * */

    if (slotNum != NOT_VALID_UNSIGNED_SIGNAL) {
        IX_ScanIterator ixScanIterator;
        scan(ixFileHandle, attribute, key, key, true, true, ixScanIterator, pageData, pageNum);
        RID rid1;
        void* key1 = malloc(PAGE_SIZE);
        unsigned slotNum1, pageNum1;
        void* nodeData = malloc(PAGE_SIZE);
        while (ixScanIterator.getNextEntry(rid1, key1, false, slotNum1, pageNum1, nodeData) != IX_EOF) {
            // found there is same rid
            if (rid.pageNum == rid1.pageNum && rid1.slotNum == rid.slotNum) {
                RC rc;
                if (checkNodeValid(nodeData)) {
                    rc = -1;
                } else {
                    free(pageData);
                    ixFileHandle.readPage(pageNum1, pageData);
                    setNodeValid(pageData, slotNum1);
                    ixFileHandle.writePage(pageNum1, pageData);
                    rc = 0;
                }
                free(nodeData);
                freeParentsPageData(parentPage);
                free(pageData);
                ixScanIterator.close();
                free(key1);
                return rc;
            }
        }

        // node not found, do normal insertion
        ixScanIterator.close();
        free(nodeData);
        free(key1);
    }

    void *nodeData = malloc(PAGE_SIZE);
    unsigned nodeLength;
    keyToLeafNode(key, rid, nodeData, nodeLength, attribute.type);
    // create copy of key, since key is const
    void *fakeKey = malloc(PAGE_SIZE);
    generateFakeKey(fakeKey, key, attribute.type);

    // basic init
    unsigned freeSpace = getFreeSpace(pageData);
    // page3 is a copy of page 1 to retrieve origin node
    void *page3 = malloc(PAGE_SIZE);
    void *page1 = pageData;
    unsigned page1Num = pageNum;
    unsigned tmpPage1Num = page1Num;
    unsigned page2Num = 0;
    bool newRootPageCreated = false;
    void *iNode = malloc(PAGE_SIZE);
    bool isLeaf = true;

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
                    page1FreeSpace -= length + SLOT_SIZE;
                    i++;
                    j++;
                    continue;
                } else {
                    // insert node into PAGE
                    setSlotOffsetAndLength(page1, j, offset, nodeLength);
                    setNodeData(page1, nodeData, offset, nodeLength);
                    page1FreeSpace -= length + SLOT_SIZE;
                    j++;
                    found = true;
                }
            } else {
                // write node_i into PAGE
                offset += nodeLength;
                setSlotOffsetAndLength(page1, j, offset, length);
                setNodeData(page1, iNode, offset, length);
                page1FreeSpace -= length + SLOT_SIZE;
                i++;
                j++;
            }
        }

        // set page1 TotalSlot & freeSpace
        setTotalSlot(page1, firstPageNum);
        setFreeSpace(page1, page1FreeSpace);

        // generate page2 get page2Num
        void *page2 = malloc(PAGE_SIZE);
        initNewPage(ixFileHandle, page2, page2Num, isLeaf, attribute.type);

        if (isLeaf) {
            // write nextPageNum for page1, page1 link to page2, page2 link to page1's next page
            unsigned page1NextPage = getNextPageNum(page1);
            setNextPageNum(page2, page1NextPage);
            setNextPageNum(page1, page2Num);
        }

        /*
         *  split process for second page(PAGE2)
         *  if the page2 is none leaf layer, the first slot is MIN_VALUE, thus j start from 1
         *  page2ExtraOffset should also start from MIN_VALUE NODE_LENGTH
         */

        j = isLeaf? 0 : 1;
        unsigned offsetInPage1, _;
        unsigned page2ExtraOffset = isLeaf ? 0 : getMinValueNodeLength(attribute.type, false);
        getSlotOffsetAndLength(page3, i, offsetInPage1, _);
        while (i < L) {
            getNodeDataAndOffsetAndLength(page3, iNode, i, offset, length);
            if (!found && compareMemoryBlock(fakeKey, iNode, length, attribute.type, true) < 0) {
                addNode(page2, nodeData, j, offset - offsetInPage1 + page2ExtraOffset, nodeLength);
                found = true;
                page2ExtraOffset += nodeLength;
                j++;
            } else {
                // write node_i into PAGE2
                addNode(page2, iNode, j, offset - offsetInPage1 + page2ExtraOffset, length);
                i++;
                j++;
            }
        }

        // if still not found, add the node at the end of the PAGE2
        if (!found) {
            unsigned page2LastOffset;
            unsigned page2LastLength;
            unsigned page2TotalSlot = getTotalSlot(page2);
            getSlotOffsetAndLength(page2, page2TotalSlot - 1, page2LastOffset, page2LastLength);
            addNode(page2, nodeData, page2TotalSlot, page2LastOffset + page2LastLength, nodeLength);
        }

        // write page1, page2 into memory
        tmpPage1Num = page1Num;
        ixFileHandle.writePage(page1Num, page1);
        ixFileHandle.appendPage(page2);

        // store & prepare for insert in parent level, fill in node & node length
        // IF node is not leaf node = <INDICATOR, KEY, PAGE_NUM> <1, key_size, 4> (bytes)
        // IF node is leaf node = <INDICATOR, KEY, RID> <1, key_size, 8> (bytes)
        getNodeDataAndOffsetAndLength(page2, nodeData,  isLeaf ? 0 : 1, offset, length);
        if (isLeaf) {
            // substitute RID into PAGE_NUM
            memcpy((char *) nodeData + length - IX_RID_SIZE, &page2Num, UNSIGNED_SIZE);
            nodeLength = nodeLength - IX_RID_SIZE + UNSIGNED_SIZE;
        } else {
            memcpy((char *) nodeData + length - UNSIGNED_SIZE, &page2Num, UNSIGNED_SIZE);
            nodeLength = nodeLength;
        }

        free(page2);
        free(page1);

        // if the root is also full, split it.
        if (parentPage.empty()) {
            page1 = malloc(PAGE_SIZE);
            // init new page assign to page1, change the rootPageNum
            initNewPage(ixFileHandle, page1, page1Num, false, attribute.type);
            ixFileHandle.rootPageNum = page1Num;
            newRootPageCreated = true;
        } else {
            page1 = parentPage.top();
            parentPage.pop();
            page1Num = parentPageNum.top();
            parentPageNum.pop();
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
        startSlot = searchNode(page1, fakeKey, attribute.type, LT_OP, isLeaf, true);

        // if we create new root page, we need to assign 1 more page number to its slot
        // i.e. add page1Num to the start slot, i.e. the MIN VALUE slot
        if (newRootPageCreated) {
            unsigned offset, length;
            void* minNodeData = malloc(PAGE_SIZE);
            getNodeDataAndOffsetAndLength(page1, minNodeData, 0, offset, length);
            // modify pageNum in none leaf node
            memcpy((char *) minNodeData + (length - UNSIGNED_SIZE), &tmpPage1Num, UNSIGNED_SIZE);
            // write back to page1
            setNodeData(page1, minNodeData, offset, length);
            free(minNodeData);
        }

        // node to insert is larger than all the node in the page, just insert
        if (startSlot == NOT_VALID_UNSIGNED_SIGNAL) {
            unsigned totalSlotNum = getTotalSlot(page1);
            unsigned lastSlotOffset;
            unsigned lastSlotLength;
            if (totalSlotNum == 0) {
                lastSlotOffset = 0;
                lastSlotLength = 0;
            } else {
                getSlotOffsetAndLength(page1, totalSlotNum - 1, lastSlotOffset, lastSlotLength);
            }
            addNode(page1, nodeData, totalSlotNum, lastSlotOffset + lastSlotLength, nodeLength);
        } else {
            unsigned startSlotOffset, _;
            getSlotOffsetAndLength(page1, startSlot, startSlotOffset, _);

            // right shift slot & left shift dictionary
            rightShiftSlot(page1, startSlot, nodeLength);
            // add new node
            addNode(page1, nodeData, startSlot, startSlotOffset, nodeLength);
        }

        // write back to file
        RC rc;
        if (page1Num >= ixFileHandle.getNumberOfPages()) {
            rc = ixFileHandle.appendPage(page1);
        } else {
            rc = ixFileHandle.writePage(page1Num, page1);
        }
        if (rc == -1)
            throw std::logic_error("wrong write file rc");
    } else {
        throw std::logic_error("logic in insert entry is not right!");
    }

    // pageData = page1, no need to free
    free(page1);
    free(page3);
    free(nodeData);
    free(iNode);
    free(fakeKey);
    freeParentsPageData(parentPage);

    return 0;
}

/*
 * 1. found page, init scan
 * 2. found node check whether the node is valid or not
 *      if not valid, return -1
 *      if valid, make it invalid, return -1
 * 3. stop when no node found, return -1
 *
 * */
RC IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    std::stack<void *> parents;
    std::stack<unsigned> parentsPageNum;
    unsigned slotNum = searchLeafNodePage(ixFileHandle, key, attribute.type, parents, parentsPageNum, false, false);
    if (slotNum == NOT_VALID_UNSIGNED_SIGNAL) {
        free(parents.top());
        return -1;
    }

    void* pageData = parents.top();

    IX_ScanIterator ixScanIterator;
    scan(ixFileHandle, attribute, key, key, true, true, ixScanIterator, pageData, parentsPageNum.top());
    RID rid1;
    void* key1 = malloc(PAGE_SIZE);
    unsigned slotNum1, pageNum1;
    void* nodeData = malloc(PAGE_SIZE);
    while (ixScanIterator.getNextEntry(rid1, key1, false, slotNum1, pageNum1, nodeData) != IX_EOF) {
        // found there is same rid
        if (rid.pageNum == rid1.pageNum && rid1.slotNum == rid.slotNum) {
            RC rc;
            if (checkNodeValid(nodeData)) {
                if (pageNum1 != parentsPageNum.top()) {
                    ixFileHandle.readPage(pageNum1, pageData);
                }
                setNodeInvalid(pageData, slotNum1);
                ixFileHandle.writePage(pageNum1, pageData);
                rc = 0;
            } else {
                rc = -1;
            }

            free(key1);
            freeParentsPageData(parents);
            ixScanIterator.close();
            free(nodeData);
            return rc;
        }
    }

    // not found, return -1
    free(key1);
    freeParentsPageData(parents);
    ixScanIterator.close();
    free(nodeData);
    return -1;
}

RC IndexManager::scan(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey, const void *highKey,
                      bool lowKeyInclusive, bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator, void *pageData,
                      unsigned pageNum) {
    if (!ixFileHandle.isOpen()) {
        return -1;
    }
    ix_ScanIterator.lowKey = malloc(PAGE_SIZE);
    ix_ScanIterator.highKey = malloc(PAGE_SIZE);
    if (lowKey == nullptr) {
        generateLowKey(ix_ScanIterator.lowKey, attribute.type);
    } else {
        // copy low key
        generateFakeKey(ix_ScanIterator.lowKey, lowKey, attribute.type);
    }

    if (highKey == nullptr) {
        generateHighKey(ix_ScanIterator.highKey, attribute.type);
    } else {
        // copy high key
        generateFakeKey(ix_ScanIterator.highKey, highKey, attribute.type);
    }

    ix_ScanIterator.lowKeyInclusive = lowKeyInclusive;
    ix_ScanIterator.highKeyInclusive = highKeyInclusive;
    ix_ScanIterator.attribute = attribute;
    ix_ScanIterator.ixFileHandle = &ixFileHandle;
    ix_ScanIterator.slotNum = 0;

    if (pageData == nullptr) {
        std::stack<void *> parents;
        std::stack<unsigned> parentsPageNum;
        searchLeafNodePage(ixFileHandle, ix_ScanIterator.lowKey, attribute.type, parents, parentsPageNum, false, true);
        ix_ScanIterator.pageData = malloc(PAGE_SIZE);
        memcpy(ix_ScanIterator.pageData, parents.top(), PAGE_SIZE);
        ix_ScanIterator.pageNum = parentsPageNum.top();
        free(parents.top());
    } else {
        ix_ScanIterator.pageData = malloc(PAGE_SIZE);
        memcpy(ix_ScanIterator.pageData, pageData, PAGE_SIZE);
        ix_ScanIterator.pageNum = pageNum;
    }

    if (lowKeyInclusive) {
        ix_ScanIterator.slotNum = searchNode(ix_ScanIterator.pageData, ix_ScanIterator.lowKey, attribute.type, LE_OP,
                                             true, true);
    } else {
        ix_ScanIterator.slotNum = searchNode(ix_ScanIterator.pageData, ix_ScanIterator.lowKey, attribute.type, LT_OP,
                                             true, true);
    }

    return 0;
}

RC IndexManager::scan(IXFileHandle &ixFileHandle,
                      const Attribute &attribute,
                      const void *lowKey,
                      const void *highKey,
                      bool lowKeyInclusive,
                      bool highKeyInclusive,
                      IX_ScanIterator &ix_ScanIterator) {
    return scan(ixFileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive, ix_ScanIterator, nullptr,
                0);
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
    preOrderPrint(&ixFileHandle, ixFileHandle.rootPageNum, attribute.type, 0);
    std::cout << "\n";
}

void IndexManager::preOrderPrint(IXFileHandle *ixFileHandle, unsigned pageNum, AttrType type, unsigned level) {
    void* pageData = malloc(PAGE_SIZE);
    void* nodeData = malloc(PAGE_SIZE);
    ixFileHandle->readPage(pageNum, pageData);
    unsigned totalSlot = getTotalSlot(pageData);
    unsigned offset, length;
    void* key = malloc(PAGE_SIZE);
    bool leafLayer = isLeafLayer(pageData);
    void* startKey = malloc(PAGE_SIZE);

    std::cout<< indentation(level) << "{\"keys\": [";
    if (leafLayer) {
        // print "keys": ["Q:[(10,1),(11,2)]"]
        bool fistFound = false;
        RID rid;
        for (unsigned i = 0; i < totalSlot; ) {
            // find same key range
            unsigned start = i, end = start + 1;
            memset(key, 0, PAGE_SIZE);
            leafNodeToKey(pageData, i, key, rid, type);
            memcpy(startKey, key, PAGE_SIZE);
            while (end < totalSlot) {
                if (!checkNodeNumValid(pageData, end)) {
                    end++;
                    continue;
                }
                memset(key, 0, PAGE_SIZE);
                leafNodeToKey(pageData, end, key, rid, type);
                if (memcmp(startKey, key, PAGE_SIZE) == 0) {
                    end++;
                } else {
                    break;
                }
            }
            leafNodeToKey(pageData, start, key, rid, type);
            if (fistFound)
                std::cout << ",";
            std::cout << "\"";
            printKey(key, type);
            std::cout << ":[";
            bool firstFoundInner = false;
            for (unsigned j = start; j < end; j++) {
                if (!checkNodeNumValid(pageData, j)) {
                    continue;
                }
                leafNodeToKey(pageData, j, key, rid, type);
                if (firstFoundInner)
                    std::cout << ",";
                printRID(rid);
                firstFoundInner = true;
            }
            std::cout << "]\"";
            fistFound = true;

            i = end;
        }
        std::cout << "]}";
    } else {
        // "keys":["P", "G"],
        std::vector<unsigned> pageNums;
        for (unsigned i = 0; i < totalSlot; ++i) {
            if (i > 1) std::cout << ",";
            noneLeafNodeToKey(pageData, i, key, pageNum, type);
            pageNums.push_back(pageNum);
            if (i != 0) {
                std::cout << "\"";
                printKey(key, type);
                std::cout << "\"";
            }
        }
        std::cout << "],\n" << indentation(level) << "\"children\": [\n";
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
    free(startKey);
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
    std::cout << "(" << rid.pageNum << "," << rid.slotNum << ")";
}


unsigned int
IndexManager::searchLeafNodePage(IXFileHandle &ixFileHandle, const void *key, AttrType type,
                                 std::stack<void *> &parents,
                                 std::stack<unsigned int> &parentsPageNum, bool rememberParents,
                                 bool checkDelete) {
    unsigned curPageNum = ixFileHandle.rootPageNum;
    void *pageData = malloc(PAGE_SIZE);
    // slot nodeData
    void *nodeData = malloc(PAGE_SIZE);
    RC rc = ixFileHandle.readPage(curPageNum, pageData);
    if (rc == -1)
        throw std::logic_error("wrong rc");

    unsigned totalSlot;
    while (!isLeafLayer(pageData)) {
        totalSlot = getTotalSlot(pageData);
        for (unsigned i = totalSlot - 1; i >= 0; i--) {
            unsigned offset, length;
            getNodeDataAndOffsetAndLength(pageData, nodeData, i, offset, length);
            // if key > slotData, we found next child, otherwise if current slot is last slot, must in there
            if (i == 0 || compareMemoryBlock(key, nodeData, length, type, false) > 0) {
                unsigned nextPageNum = getNextPageFromNotLeafNode(nodeData);

                void *parentPage = malloc(PAGE_SIZE);
                memcpy(parentPage, pageData, PAGE_SIZE);
                if (rememberParents) {
                    parents.push(parentPage);
                    parentsPageNum.push(curPageNum);
                } else {
                    free(parentPage);
                }

                ixFileHandle.readPage(nextPageNum, pageData);
                curPageNum = nextPageNum;
                break;
            }
        }
    }

    parents.push(pageData);
    parentsPageNum.push(curPageNum);

    unsigned slotNum = searchNode(pageData, key, type, EQ_OP, true, checkDelete);
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
    setLeafLayer(data, isLeafLayer);
    // if is none leaf layer, insert initial MIN_VALUE_SIGNAL into it
    if (!isLeafLayer) {
        unsigned offset = 0, length = 0;
        // generate MIN_VALUE data
        void* key = malloc(20);
        void* nodeData = malloc(40);
        generateMinValueNode(key, nodeData, length, type);
        addNode(data, nodeData, 0, offset, length);
        free(nodeData);
        free(key);
    }
}


void IndexManager::generateMinValueNode(void *key, void *nodeData, unsigned &length, AttrType type) {
    if (type == TypeVarChar) {
        unsigned x = 6;
        std::string y = MIN_STRING;
        memcpy(key, &x, UNSIGNED_SIZE);
        memcpy((char *) key + UNSIGNED_SIZE, y.c_str(), y.size());
    } else if (type == TypeInt) {
        unsigned z = MIN_INT;
        memcpy(key, &z, UNSIGNED_SIZE);
    } else {
        float a = MIN_FLOAT;
        memcpy(key, &a, UNSIGNED_SIZE);
    }

    keyToNoneLeafNode(key, NOT_VALID_UNSIGNED_SIGNAL, nodeData, length, type);
}

unsigned IndexManager::getFreeSpace(void *data) {
    unsigned freeSpace;
    memcpy(&freeSpace, (char *) data + IX_FREE_SPACE_POS, UNSIGNED_SIZE);
    if (freeSpace > IX_INIT_FREE_SPACE) {
        throw std::logic_error("Free space invalid");
    }
    return freeSpace;
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
    memcpy((char *) data + IX_NEXT_PAGE_NUM_POS, &nextPageNum, UNSIGNED_SIZE);
}

void IndexManager::getSlotOffsetAndLength(void *data, unsigned slotNum, unsigned &offset, unsigned &length) {
    unsigned pos = IX_LEAF_LAYER_FLAG_POS - (slotNum + 1) * UNSIGNED_SIZE * 2;
    memcpy(&offset, (char *) data + pos, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;
    memcpy(&length, (char *) data + pos, UNSIGNED_SIZE);
    if (offset >= PAGE_SIZE && length >= PAGE_SIZE) {
        throw std::logic_error("Slot offset or length invalid");
    }
}

void IndexManager::setSlotOffsetAndLength(void *data, unsigned slotNum, unsigned offset, unsigned length) {
    unsigned pos = IX_LEAF_LAYER_FLAG_POS - (slotNum + 1) * UNSIGNED_SIZE * 2;
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
    setFreeSpace(pageData, freeSpace - length - 2 * UNSIGNED_SIZE);
    unsigned totalSlot = getTotalSlot(pageData);
    setTotalSlot(pageData, totalSlot + 1);
}

/*
 * IF node is leaf node = <KEY, INDICATOR, RID> <1, key_size, 8> (bytes)
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
                                slotLength - NODE_INDICATOR_SIZE - (isLeaf ? IX_RID_SIZE : UNSIGNED_SIZE));
        if (blockString == MAX_STRING)
            return -1;
        if (blockString == MIN_STRING)
            return 1;
        int res = keyString.compare(blockString);
        if (res > 0)
            return 1;
        else if (res < 0)
            return -1;
        else
            return 0;
    }
}

// IF node is not leaf node = <INDICATOR, KEY, PAGE_NUM> <1, 4, 4> (bytes)
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

    // shift whole slot & directory
    unsigned startDir = IX_LEAF_LAYER_FLAG_POS - SLOT_SIZE * totalSlot;
    unsigned endDir = IX_LEAF_LAYER_FLAG_POS - SLOT_SIZE * startSlot;

    // move node
    memmove((char *) data + startSlotOffset + shiftLength, (char *) data + startSlotOffset,
            endSlotOffset + endSlotLength - startSlotOffset);
    // move dir to left
    memmove((char *) data + startDir - UNSIGNED_SIZE * 2, (char *) data + startDir, endDir - startDir);
}

unsigned int
IndexManager::searchNode(void *data, const void *key, AttrType type, CompOp compOp, bool isLeaf, bool checkDelete) {
    unsigned totalSlot = getTotalSlot(data);
    if (totalSlot == 0)
        return NOT_VALID_UNSIGNED_SIGNAL;
    void *nodeData = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < totalSlot; i++) {
        unsigned offset, length;
        getNodeDataAndOffsetAndLength(data, nodeData, i, offset, length);
        // if already deleted
        if (isLeaf && checkDelete && !checkNodeValid(nodeData))
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
            case LE_OP:
                if (compareRes <= 0) {
                    free(nodeData);
                    return i;
                }
                break;
            case NE_OP:
                break;
            case NO_OP:
                break;
        }
    }

    free(nodeData);

    if (compOp == EQ_OP || compOp == GT_OP || compOp == GE_OP || compOp == LT_OP || compOp == LE_OP) {
        return NOT_VALID_UNSIGNED_SIGNAL;
//    } else if (compOp == GT_OP || compOp == GE_OP || compOp == LT_OP) {
//        return totalSlot;
    } else {
        throw std::logic_error("CompOp is not valid!");
    }
}

/*
 * IF node is leaf node = <INDICATOR, KEY, RID> <1, key_size, 8> (bytes)
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

    memcpy((char *) data + pos, &rid.pageNum, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;
    memcpy((char *) data + pos, &rid.slotNum, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

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

    unsigned keyLength = length - NODE_INDICATOR_SIZE - IX_RID_SIZE;

    memcpy(&rid.pageNum, (char *) nodeData + NODE_INDICATOR_SIZE + keyLength, UNSIGNED_SIZE);
    memcpy(&rid.slotNum, (char *) nodeData + NODE_INDICATOR_SIZE + keyLength + UNSIGNED_SIZE, UNSIGNED_SIZE);

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

void IndexManager::setNodeValid(void *data, unsigned slotNum) {
    unsigned offset, length;
    getSlotOffsetAndLength(data, slotNum, offset, length);

    unsigned char deleteFlag = NORMAL_FLAG;
    memcpy((char *) data + offset, &deleteFlag, NODE_INDICATOR_SIZE);
}


bool IndexManager::checkNodeValid(void *data) {
    unsigned char deleteFlag = DELETE_FLAG;
    return memcmp(data, &deleteFlag, NODE_INDICATOR_SIZE) != 0;
}

void IndexManager::freeParentsPageData(std::stack<void *> &parents) {
    while (!parents.empty()) {
        free(parents.top());
        parents.pop();
    }
}

void IndexManager::generateLowKey(void *key, AttrType type) {
    if (type == TypeInt) {
        int minInt = MIN_INT;
        memcpy(key, &minInt, UNSIGNED_SIZE);
    } else if (type == TypeReal) {
        float minFloat = MIN_FLOAT;
        memcpy(key, &minFloat, UNSIGNED_SIZE);
    } else {
        std::string minString = MIN_STRING;
        unsigned length = minString.length();
        memcpy(key, &length, UNSIGNED_SIZE);
        memcpy((char *) key + UNSIGNED_SIZE, minString.c_str(), length);
    }
}

void IndexManager::generateHighKey(void *key, AttrType type) {
    if (type == TypeInt) {
        int maxInt = MAX_INT;
        memcpy(key, &maxInt, UNSIGNED_SIZE);
    } else if (type == TypeReal) {
        float maxFloat = MAX_FLOAT;
        memcpy(key, &maxFloat, UNSIGNED_SIZE);
    } else {
        std::string maxString = MAX_STRING;
        unsigned length = maxString.length();
        memcpy(key, &length, UNSIGNED_SIZE);
        memcpy((char *) key + UNSIGNED_SIZE, maxString.c_str(), length);
    }
}

unsigned int IndexManager::getMinValueNodeLength(AttrType type, bool isLeaf) {
    unsigned length = 0;
    if (type == TypeVarChar) {
        std::string string = MIN_STRING;
        length += string.length() + UNSIGNED_SIZE;
    } else {
        length += UNSIGNED_SIZE;
    }
    length += isLeaf ? 9 : 5;
    return length;
}

void IndexManager::generateFakeKey(void *fakeKey, const void *key, AttrType type) {
    if (type != TypeVarChar) {
        memcpy(fakeKey, key, UNSIGNED_SIZE);
    } else {
        unsigned keyLength;
        memcpy(&keyLength, key, UNSIGNED_SIZE);
        memcpy(fakeKey, key, UNSIGNED_SIZE + keyLength);
    }
}

IX_ScanIterator::IX_ScanIterator() {
    im = &IndexManager::instance();
}

IX_ScanIterator::~IX_ScanIterator() {
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
    unsigned _, __;
    RC rc = getNextEntry(rid, key, true, _, __, nullptr);
    return rc;
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key, bool checkDeleted, unsigned &returnSlotNum, unsigned &returnPageNum,
                                 void *returnNodeData) {
    unsigned totalSLot = im->getTotalSlot(pageData);
    bool found = false;
    while (!found) {
        while (slotNum < totalSLot) {
            if (checkDeleted && !im->checkNodeNumValid(pageData, slotNum)) {
                slotNum += 1;
            } else {

                unsigned offset, length;
                im->getSlotOffsetAndLength(pageData, slotNum, offset, length);
                void *nodeData = malloc(PAGE_SIZE);
                im->getNodeData(pageData, nodeData, offset, length);
                returnSlotNum = slotNum;
                returnPageNum = pageNum;

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

                if (!checkDeleted) {
                    memcpy(returnNodeData, nodeData, PAGE_SIZE);
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
            pageNum = nextPage;
            ixFileHandle->readPage(nextPage, pageData);
            slotNum = 0;
            totalSLot = im->getTotalSlot(pageData);
        }
    }

    im->leafNodeToKey(pageData, slotNum, key, rid, attribute.type);
    slotNum += 1;

    return 0;
}

RC IX_ScanIterator::close() {
    free(pageData);
    free(lowKey);
    free(highKey);
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
    RC rc = fileHandle.collectCounterValues(ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter);
    readPageCount = ixReadPageCounter;
    writePageCount = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return rc;
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
    void* data = malloc(PAGE_SIZE);
    readPage(0, data);
    unsigned pageNum;
    memcpy(&pageNum, data, UNSIGNED_SIZE);
    free(data);
    rootPageNum = pageNum;
}

void IXFileHandle::_writeRootPageNum() {
    fileHandle.fs.seekp(UNSIGNED_SIZE * 3, std::ios::beg);
    fileHandle.fs.write(reinterpret_cast<char *>(&rootPageNum), UNSIGNED_SIZE);
}

bool IXFileHandle::isOpen() {
    return fileHandle.fs.is_open();
}
