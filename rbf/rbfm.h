#ifndef _rbfm_h_
#define _rbfm_h_

#include "pfm.h"
#include <vector>

#define F_POS 4094
#define N_POS 4092
#define DICT_SIZE 4
#define INIT_FREE_SPACE 4092
#define INT_SIZE 4
#define UNSIGNED_CHAR_SIZE 1
#define UNSIGNED_SIZE 4
#define UNSIGNED_SHORT_SIZE 2
#define REDIRECT_INDICATOR_SIZE 1
#define RID_SIZE 7
#define SCAN_INIT_PAGE_NUM 0
#define SCAN_INIT_SLOT_NUM 0
#define NULL_INDICATOR_UNIT_SIZE 1

// Record ID
typedef struct {
    unsigned pageNum;    // page number
    unsigned short slotNum;    // slot number in the page
} RID;

// Attribute
typedef enum {
    TypeInt = 0, TypeReal, TypeVarChar
}  AttrType;

typedef unsigned AttrLength;

struct Attribute {
    std::string name;  // attribute name
    AttrType type;     // attribute type
    AttrLength length; // attribute length
};

// Comparison Operator (NOT needed for part 1 of the project)
typedef enum {
    EQ_OP = 0, // no condition// =
    LT_OP,      // <
    LE_OP,      // <=
    GT_OP,      // >
    GE_OP,      // >=
    NE_OP,      // !=
    NO_OP       // no condition
} CompOp;


/********************************************************************
* The scan iterator is NOT required to be implemented for Project 1 *
********************************************************************/

# define RBFM_EOF (-1)  // end of a scan operator

//  RBFM_ScanIterator is an iterator to go through records
//  The way to use it is like the following:
//  RBFM_ScanIterator rbfmScanIterator;
//  rbfm.open(..., rbfmScanIterator);
//  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
//    process the data;
//  }
//  rbfmScanIterator.close();

class RecordBasedFileManager;

class RBFM_ScanIterator {
public:
    FileHandle *fileHandle;
    std::vector<std::string> attributeNames;
    std::vector<Attribute> recordDescriptor;
    std::string conditionAttribute;
    CompOp compOp;
    const void* value;
    RID rid;

    RBFM_ScanIterator();

    ~RBFM_ScanIterator() = default;

    // Never keep the results in the memory. When getNextRecord() is called,
    // a satisfying record needs to be fetched from the file.
    // "data" follows the same format as RecordBasedFileManager::insertRecord().
    RC getNextRecord(RID &nextRID, void *data);

    RC close();

    bool isCurRIDValid(void *data);

    bool checkConditionalAttr();

private:
    RecordBasedFileManager* rbfm;

};

class RecordBasedFileManager {
public:
    static RecordBasedFileManager &instance();                          // Access to the _rbf_manager instance

    RC createFile(const std::string &fileName);                         // Create a new record-based file

    RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

    RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

    RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

    //  Format of the data passed into the function is the following:
    //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
    //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
    //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
    //     Each bit represents whether each field value is null or not.
    //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
    //     If k-th bit from the left is set to 0, k-th field contains non-null values.
    //     If there are more than 8 fields, then you need to find the corresponding byte first,
    //     then find a corresponding bit inside that byte.
    //  2) Actual data is a concatenation of values of the attributes.
    //  3) For Int and Real: use 4 bytes to store the value;
    //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
    //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
    // For example, refer to the Q8 of Project 1 wiki page.

    // Insert a record into a file
    RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data, RID &rid);

    // Read a record identified by the given rid.
    RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

    RC readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                  const RID &rid, void *data, bool isOutputRecord, unsigned short &recordLength);

    // Print the record that is passed to this utility method.
    // This method will be mainly used for debugging/testing.
    // The format is as follows:
    // field1-name: field1-value  field2-name: field2-value ... \n
    // (e.g., age: 24  height: 6.1  salary: 9000
    //        age: NULL  height: 7.5  salary: 7500)
    RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data);

    /*****************************************************************************************************
    * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
    * are NOT required to be implemented for Project 1                                                   *
    *****************************************************************************************************/
    // Delete a record identified by the given rid.
    RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

    // Assume the RID does not change after an update
    RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                    const RID &rid);

    // Read an attribute given its name and the rid.
    RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                     const std::string &attributeName, void *data);

    RC readAttributes(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                      const std::vector<std::string> &attributeNames, void *data);

    static void setNullIndicatorToExist(void *data, int i);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    RC scan(FileHandle &fileHandle,
            const std::vector<Attribute> &recordDescriptor,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparision type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RBFM_ScanIterator &rbfm_ScanIterator);

    static unsigned short getFreeSpaceByPageNum(FileHandle &fileHandle, unsigned pageNum);

    static unsigned short getTotalSlot(const void *data);

    static unsigned short getFreeSpace(const void *data);

    // return -1 for none space remain, otherwise the page can insert
    int scanFreeSpace(FileHandle &fileHandle, unsigned curPageNum, unsigned sizeNeed);

    // write FreeSpace & SlotNum into new page
    static unsigned initiateNewPage(FileHandle &fileHandle);

    static void setSlot(void *pageData, unsigned short slotNum);

    static void setSpace(void *pageData, unsigned short freeSpace);

    static void getOffsetAndLength(void *data, unsigned short slotNum, unsigned short &offset, unsigned short &length);

    static void setOffsetAndLength(void *data, unsigned short slotNum, unsigned short offset, unsigned short length);

    static void convertDataToRecord(const void *data, void *record, unsigned short &recordSize,
                             const std::vector<Attribute> &recordDescriptor);

    static void getAttrExistArray(unsigned short &pos, int *attrExist, const void *data, unsigned attrSize, bool isRecord);

    static void appendRecordIntoPage(FileHandle &fileHandle, unsigned pageIdx, unsigned short dataSize,
                              const void *record, RID &rid);

    static void writeRecord(void *pageData, const void *record, unsigned short offset, unsigned short length);

    static void convertRecordToData(void *record, void *data, const std::vector<Attribute> &recordDescriptor);

    void leftShiftRecord(void *data, unsigned short startOffset, unsigned short oldLength,
                         unsigned short newLength);

    void rightShiftRecord(void *data, unsigned short startOffset, unsigned short length,
                          unsigned short updatedLength);

    static bool isRedirected(void *record);

    static void getRIDFromRedirectedRecord(void* record, RID &rid);

    RC readRecordFromPage(void* data, void* record, unsigned short slotNum);

    static void readRIDFromRecord(void* record, RID &rid);

    static void createRIDRecord(void *record, RID &rid);



protected:
    RecordBasedFileManager();                                                   // Prevent construction
    ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
    RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
    RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment
};

#endif // _rbfm_h_


