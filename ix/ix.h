#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <stack>

#include "../rbf/rbfm.h"
#include "../rbf/pfm.h"

# define IX_EOF (-1)  // end of the index scan
#define IX_INIT_FREE_SPACE 4087
#define IX_LEAF_LAYER_FLAG_POS 4087
#define IX_FREE_SPACE_POS 4088
#define IX_TOTAL_SLOT_POS 4090
#define IX_NEXT_PAGE_NUM_POS 4092

#define LEAF_LAYER_FLAG 0x01
#define NODE_INDICATOR_SIZE 1
#define SLOT_SIZE 4
#define DELETE_FLAG 0x80
#define NORMAL_FLAG 0x00
#define IX_RID_SIZE 6
#define MIN_INT -2147483648
#define MAX_INT 2147483647
#define MIN_FLOAT 1.17549e-038
#define MAX_FLOAT 3.40282e+038
#define MIN_STRING "MIN_STRING"
#define MAX_STRING "HIGH_STRING"
#define NOT_VALID_UNSIGNED_SHORT_SIGNAL 65534
#define NOT_VALID_UNSIGNED_SIGNAL 987654321

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
    static RC scan(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey, const void *highKey,
                   bool lowKeyInclusive, bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator, void *pageData,
                   unsigned pageNum);

    RC scan(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *lowKey, const void *highKey,
            bool lowKeyInclusive, bool highKeyInclusive, IX_ScanIterator &ix_ScanIterator);

    // Print the B+ tree in pre-order (in a JSON record format)
    void printBtree(IXFileHandle &ixFileHandle, const Attribute &attribute) const;

    // return the page of the required leave node
    // if node not found, return -1
    static unsigned short searchLeafNodePage(IXFileHandle &ixFileHandle, const void *key, AttrType type,
                                             std::stack<void *> &parents,
                                             std::stack<unsigned int> &parentsPageNum, bool rememberParents,
                                             bool checkDelete);

    static void initNewPage(IXFileHandle &ixFileHandle, void *data, unsigned &pageNum, bool isLeafLayer, AttrType type = TypeInt);

    static unsigned short getFreeSpace(void *data);

    static void setFreeSpace(void *data, unsigned short freeSpace);

    static unsigned short getTotalSlot(void *data);

    static void setTotalSlot(void *data, unsigned short totalSlot);

    static bool isLeafLayer(void *pageData);

    static void setLeafLayer(void *data, bool isLeafLayer);

    static unsigned getNextPageNum(void *data);

    static void setNextPageNum(void *data, unsigned nextPageNum);

    static void getSlotOffsetAndLength(void *data, unsigned short slotNum, unsigned short &offset, unsigned short &length);

    static void setSlotOffsetAndLength(void *data, unsigned short slotNum, unsigned short offset, unsigned short length);

    static void getNodeData(void *pageData, void *data, unsigned short offset, unsigned short length);

    static void setNodeData(void *pageData, void *data, unsigned short offset, unsigned short length);

    static void getNodeDataAndOffsetAndLength(void* pageData, void* nodeData, unsigned short slotNum, unsigned short &offset, unsigned short &length);

    static void addNode(void* pageData, void* nodeData, unsigned short slotNum, unsigned short offset, unsigned short length);

    // return 1 if key > block, -1 key < block, 0 key == block
    static int compareMemoryBlock(const void *key, void *slotData, unsigned short slotLength, AttrType type, bool isLeaf);

    static unsigned int getNextPageFromNotLeafNode(void *data, unsigned nodeLength);

    static void rightShiftSlot(void *data, unsigned short startSlot, unsigned short shiftLength);

    static unsigned short
    searchNode(void *data, const void *key, AttrType type, CompOp compOp, bool isLeaf, bool checkDelete);

    static void keyToLeafNode(const void *key, const RID &rid, void *data, unsigned short &length, AttrType type);

    static void keyToNoneLeafNode(const void *key, unsigned pageNum, void *data, unsigned short &length, AttrType type);

    static void leafNodeToKey(void *data, unsigned short slotNum, void* key, RID &rid, AttrType type);

    static void noneLeafNodeToKey(void *data, unsigned short slotNum, void* key, unsigned &pageNum, AttrType type);

    static void generateMinValueNode(void *key, void *nodeData, unsigned short &length, AttrType type);

    static bool checkNodeNumValid(void *data, unsigned short slotNum);

    static bool checkNodeValid(void *data);

    static void setNodeInvalid(void *data, unsigned short slotNum);

    static void setNodeValid(void *data, unsigned short slotNum);

    static void freeParentsPageData(std::stack<void *> &parents);

    static void generateLowKey(void* key, AttrType type);

    static void generateHighKey(void* key, AttrType type);

    static void preOrderPrint(IXFileHandle *ixFileHandle, unsigned pageNum, AttrType type, unsigned level);

    static std::string indentation(unsigned num);

    static void printKey(void *key, AttrType type);

    static void printRID(RID &rid);

    static unsigned short getMinValueNodeLength(AttrType type, bool isLeaf);

    static void generateFakeKey(void* fakeKey, const void* key, AttrType type);

protected:
    IndexManager() = default;                                                   // Prevent construction
    ~IndexManager() = default;                                                  // Prevent unwanted destruction
    IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
    IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment
};

class IX_ScanIterator {
public:

    IndexManager* im;

    void *lowKey;
    void *highKey;
    bool lowKeyInclusive;
    bool highKeyInclusive;
    Attribute attribute;
    IXFileHandle *ixFileHandle;

    unsigned short slotNum;
    unsigned pageNum;
    void* pageData;

    // Constructor
    IX_ScanIterator();

    // Destructor
    ~IX_ScanIterator();

    // Get next matching entry
    RC getNextEntry(RID &rid, void *key);

    // Terminate index scan
    RC close();

    RC getNextEntry(RID &rid, void *key, bool checkDeleted, unsigned short &returnSlotNum, unsigned &returnPageNum,
                    void *returnNodeData);
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

    bool isOpen();

    void _readRootPageNum();
    void _writeRootPageNum();
};

#endif
