#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unordered_map>
#include <unordered_set>

#include "../ix/ix.h"

# define RM_EOF (-1)  // end of a scan operator

# define TABLES_FILE_NAME "Tables.tbl"
# define COLUMNS_FILE_NAME "Columns.tbl"
# define INDEX_FILE_NAME "Index.tbl"
# define TABLES_NAME "Tables"
# define COLUMNS_NAME "Columns"
# define INDEX_NAME "Index"
# define EXT ".tbl"
# define IDX_EXT ".idx"
# define SM_BLOCK 500

#define NULL_STRING ""

#define TABLES_ATTRIBUTE_SIZE 4
#define COLUMNS_ATTRIBUTE_SIZE 5
#define SYSTEM_INDICATOR_SIZE 4

class RelationManager;

// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
    RM_ScanIterator();

    ~RM_ScanIterator();

    RBFM_ScanIterator rbfmsi;
    FileHandle fileHandle;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data);
    RC close();
};

// RM_IndexScanIterator is an iterator to go through index entries
class RM_IndexScanIterator {
public:
    RM_IndexScanIterator();    // Constructor
    ~RM_IndexScanIterator() {};    // Destructor

    IX_ScanIterator ixsi;
    IXFileHandle ixFileHandle;

    // "key" follows the same format as in IndexManager::insertEntry()
    RC getNextEntry(RID &rid, void *key);    // Get next matching entry
    RC close();                        // Terminate index scan

private:
    IndexManager *im;
};


// Relation Manager
class RelationManager {
public:
    static RelationManager &instance();

    unsigned curTableID;

    std::vector<Attribute> tableAttr;
    std::vector<Attribute> columnAttr;
    std::vector<Attribute> indexAttr;
    
    std::vector<std::string> tableAttributeNames;
    std::vector<std::string> columnAttributeNames;
    std::vector<std::string> indexAttrNames;


    // tableName -> fileName
    std::unordered_map<std::string, std::string> tableNameToFileMap;
    // tableName -> TableID
    std::unordered_map<std::string, unsigned> tableNameToIdMap;
    // TableID -> tableName
    std::unordered_map<unsigned, std::string> idToTableNameMap;
    // tableName -> system
    std::unordered_map<std::string, bool> tableNameToIsSystemTableMap;

    // tableName -> vector<Attribute>
    std::unordered_map<std::string, std::vector<Attribute>> tableNameToAttrMap;

    /*
     * index related map
     * */
    //tableName_attributeName ->index file name
    std::unordered_map<std::string, std::string> tNANToIndexFile;
    // tableName -> vector<Attribute Name>
    std::unordered_map<std::string, std::unordered_set<int>> indexMap;

    static void appendAttr(std::vector<Attribute> &attrArr, std::string name, AttrType type, AttrLength len);

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

    RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs, bool isSystemTable);

    RC deleteTable(const std::string &tableName);

    RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

    RC insertTuple(const std::string &tableName, const void *data, RID &rid);

    RC insertTuple(const std::string &tableName, const void *data, RID &rid, bool isInternalCall);

    RC deleteTuple(const std::string &tableName, const RID &rid);

    RC deleteTuple(const std::string &tableName, const RID &rid, bool isInternalCall);

    RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

    RC readTuple(const std::string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const std::vector <Attribute> &attrs, const void *data);

    RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const std::string &tableName,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparison type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);

    static void generateTablesData(unsigned id, const std::string& tableName, std::string fileName, void *data,
                            bool isSystemTable);

    static void generateColumnsData(unsigned id, Attribute attr, unsigned position, void *data);

    static void generateIndexData(void* data, const std::string & tableName, const std::string & attrName, const int index, const std::string & fileName);

    void initScanTablesOrColumns(bool isTables);

    void initScanIndex();

    static void parseTablesData(void *data, std::string &tableName, std::string &fileName, unsigned int &id,
                         bool &isSystemTable);

    static void parseColumnsData(void *data, unsigned int &id, Attribute &attr, unsigned &position);

    static void
    parseIndexData(void *data, std::string &tableName, std::string &attrName, int &index, std::string &fileName);

    // Extra credit work (10 points)
    RC addAttribute(const std::string &tableName, const Attribute &attr);

    RC dropAttribute(const std::string &tableName, const std::string &attributeName);

    // QE IX related
    RC createIndex(const std::string &tableName, const std::string &attributeName);

    RC destroyIndex(const std::string &tableName, const std::string &attributeName);

    static std::string getIndexNameHash(const std::string& tableName, const std::string& attrName);

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator);




protected:
    RelationManager();                                                  // Prevent construction
    ~RelationManager();                                                 // Prevent unwanted destruction
    RelationManager(const RelationManager &);                           // Prevent construction by copying
    RelationManager &operator=(const RelationManager &);                // Prevent assignment

private:
    RecordBasedFileManager *rbfm;
    IndexManager *im;
    PagedFileManager *pfm;
    static RelationManager *_relation_manager;
};

#endif