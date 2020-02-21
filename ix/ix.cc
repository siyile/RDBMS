#include "ix.h"

IndexManager &IndexManager::instance() {
    static IndexManager _index_manager = IndexManager();
    return _index_manager;
}

RC IndexManager::createFile(const std::string &fileName) {
    return -1;
}

RC IndexManager::destroyFile(const std::string &fileName) {
    return -1;
}

RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
    return -1;
}

RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
    return -1;
}

/*
 * IF node is leaf node = <KEY, INDICATOR, RID>
 * LID = (pageNum, slotNum)
 * IF node is not leaf node = <KEY, INDICATOR, PAGE_NUM>
 * INDICATOR include LEAVE_INDICATOR, DELETE_INDICATOR
 *
 * PAGE DESIGN
 * [LEAVE_NODE, LEAVE_NODE, ...
 *
 *
 *      ..., SLOT, SLOT, Free_Space, SLOT_NUM, NEXT_PAGE_NUM]
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
 * IF there is enough space for the node:
 *      find the slot, right shift data & left shift dictionary, then insert, end
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
 *
 *
 *
 */
RC IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
    return -1;
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
    return -1;
}

