#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

# define IX_EOF (-1)  // end of the index scan
#define IX_INIT_FREE_SPACE 4083
#define IX_FREE_SPACE_POS 4084
#define IX_TOTAL_SLOT_POS 4088
#define IX_LEAF_LAYER_FLAG_POS 4083
#define IX_NEXT_PAGE_NUM_POS 4092
#define LEAF_LAYER_FLAG 0x01
#define NODE_INDICATOR_SIZE 1
#define SLOT_SIZE 8
#define DELETE_FLAG 0x80
#define NORMAL_FLAG 0x00

class IX_ScanIterator;

class IXFileHandle;

class IndexManager {

public:
    static IndexManager &instance();

    // Create an index file.
    RC createFile(const std::string &fileName);

    // Delete an index file.
    RC destroyFile(const std::string &fileName);

    // Open an index and return an ixFileHandle.
    RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

    // Close an ixFileHandle for an index.
    RC closeFile(IXFileHandle &ixFileHandle);

    // Insert an entry into the given index that is indicated by the given ixFileHandle.
    RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Delete an entry from the given index that is indicated by the given ixFileHandle.
    RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

    // Initialize and IX_ScanIterator to support a range search
    RC scan(IXFileHandle &ixFileHandle,
            const Attribute &attribute,
            const void *lowKey,
            const void *highKey,
            bool lowKeyInclusive,
            bool highKeyInclusive,
            IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

    // return the page of the required leave node
    // if node not found, return -1
    static unsigned int searchLeafNodePage(IXFileHandle &ixFileHandle, const void *key, AttrType type, std::stack<void *> parents,
                                           std::stack<unsigned> parentsPageNum, bool rememberParents = true);

    static void initNewPage(IXFileHandle &ixFileHandle, void *data, unsigned &pageNum, bool isLeafLayer);

    static unsigned getFreeSpace(void *data);

    static void setFreeSpace(void *data, unsigned freeSpace);

    static unsigned getTotalSlot(void *data);

    static void setTotalSlot(void *data, unsigned totalSlot);

    static bool isLeafLayer(void *data);

    static void setLeafLayer(void *data, bool isLeafLayer);

    static unsigned getNextPageNum(void *data);

    static void setNextPageNum(void *data, unsigned nextPageNum);

    static void getSlotOffsetAndLength(void *data, unsigned slotNum, unsigned &offset, unsigned &length);

    static void setSlotOffsetAndLength(void *data, unsigned slotNum, unsigned offset, unsigned length);

    static void getNodeData(void *pageData, void *data, unsigned offset, unsigned length);

    static void setNodeData(void *pageData, void *data, unsigned offset, unsigned length);

    // return 1 if key > block, -1 key < block, 0 key == block
    static int compareMemoryBlock(const void *key, void *slotData, unsigned slotLength, AttrType type, bool isLeaf);

    static unsigned getNextPageFromNotLeafNode(void *data);

    static void rightShiftSlot(void *data, unsigned startSlot, unsigned shiftLength);

    static unsigned searchLeafNode(void* data, const void* key, AttrType type, CompOp compOp);

    static void keyToLeafNode(const void *key, const RID &rid, void *data, unsigned &length, AttrType type);

    static void keyToNoneLeafNode(const void *key, unsigned pageNum, void *data, unsigned &length, AttrType type);

    static void leafNodeToKey(void *data, unsigned slotNum, void* key, RID &rid, AttrType type);

    static bool checkNodeNumValid(void *data, unsigned slotNum);

    static bool checkNodeValid(void *data);

    static void setNodeInvalid(void *data, unsigned slotNum);

    static void freeParentsPageData(std::stack<void *> parents);

protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

};

class IX_ScanIterator {
public:

    IndexManager* im;

    const void *lowKey;
    const void *highKey;
    bool lowKeyInclusive;
    bool highKeyInclusive;
    Attribute attribute;
    IXFileHandle *ixFileHandle;

    unsigned slotNum;
    void* pageData;

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();
};

class IXFileHandle {
public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    unsigned rootPageNum;

    FileHandle fileHandle;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    // Put the current counter values of associated PF FileHandles into variables
    RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    // routine from pfm
    unsigned getNumberOfPages();
    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page

    void _readRootPageNum();
    void _writeRootPageNum();
};

#endif
