#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>
#include <unordered_map>

#include "../rbf/rbfm.h"

# define RM_EOF (-1)  // end of a scan operator

# define TABLES_FILE_NAME "Tables.tbl"
# define COLUMNS_FILE_NAME "Columns.tbl"
# define TABLES_NAME "Tables"
# define COLUMNS_NAME "Columns"
# define EXT ".tbl"
# define SM_BLOCK 500

#define NULL_STRING ""

#define TABLES_ATTRIBUTE_SIZE 4
#define COLUMNS_ATTRIBUTE_SIZE 5
#define SYSTEM_INDICATOR_SIZE 4
#define NULL_INDICATOR_UNIT_SIZE 1

typedef enum {
    INSERT = 0,
    DELETE,
    UPDATE,
    PRINT,
} RBFM_OP;


// RM_ScanIterator is an iterator to go through tuples
class RM_ScanIterator {
public:
    RM_ScanIterator();

    ~RM_ScanIterator() = default;

    RBFM_ScanIterator rbfmsi;
    FileHandle fileHandle;

    // "data" follows the same format as RelationManager::insertTuple()
    RC getNextTuple(RID &rid, void *data);

    RC close();
};

// Relation Manager
class RelationManager {
public:
    static RelationManager &instance();

    unsigned curTableID;

    std::vector<Attribute> tableAttr;
    std::vector<Attribute> columnAttr;
    
    std::vector<std::string> tableAttributeNames;
    std::vector<std::string> columnAttributeNames;


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

    void appendAttr(std::vector<Attribute> &attrArr, std::string name, AttrType type, AttrLength len);

    RC createCatalog();

    RC deleteCatalog();

    RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

    RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs, bool isSystemTable);

    RC deleteTable(const std::string &tableName);

    RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

    RC insertTuple(const std::string &tableName, const void *data, RID &rid);

    RC deleteTuple(const std::string &tableName, const RID &rid);

    RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

    RC readTuple(const std::string &tableName, const RID &rid, void *data);

    // Print a tuple that is passed to this utility method.
    // The format is the same as printRecord().
    RC printTuple(const std::vector<Attribute> &attrs, const void *data);

    RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

    // Scan returns an iterator to allow the caller to go through the results one by one.
    // Do not store entire results in the scan iterator.
    RC scan(const std::string &tableName,
            const std::string &conditionAttribute,
            const CompOp compOp,                  // comparison type such as "<" and "="
            const void *value,                    // used in the comparison
            const std::vector<std::string> &attributeNames, // a list of projected attributes
            RM_ScanIterator &rm_ScanIterator);

    void generateTablesData(unsigned id, std::string tableName, std::string fileName, void *data,
                            bool isSystemTable);

    void generateColumnsData(unsigned id, Attribute attr, unsigned position, void *data);

    void initScanTablesOrColumns(bool isTables);

    void parseTablesData(void *data, std::string &tableName, std::string &fileName, unsigned int &id,
                         bool &isSystemTable);

    void parseColumnsData(void *data, unsigned int &id, Attribute &attr, unsigned &position);

// Extra credit work (10 points)
    RC addAttribute(const std::string &tableName, const Attribute &attr);

    RC dropAttribute(const std::string &tableName, const std::string &attributeName);



protected:
    RelationManager();                                                  // Prevent construction
    ~RelationManager();                                                 // Prevent unwanted destruction
    RelationManager(const RelationManager &);                           // Prevent construction by copying
    RelationManager &operator=(const RelationManager &);                // Prevent assignment

private:
    RecordBasedFileManager *rbfm;
};

#endif