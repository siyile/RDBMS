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

