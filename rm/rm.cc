#include "rm.h"
#include <sys/stat.h>
#include <utility>

inline bool exists_test (const std::string& name) {
    struct stat buffer;
    return (stat (name.c_str(), &buffer) == 0);
}

RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager() {
    rbfm = &RecordBasedFileManager::instance();


        // initiate table attribute
        appendAttr(tableAttr, "table-id", TypeInt, 4);
        appendAttr(tableAttr, "table-name", TypeVarChar, 50);
        appendAttr(tableAttr, "file-name", TypeVarChar, 50);
        appendAttr(tableAttr, "system-table", TypeInt, 4);

        // initiate column attribute
        appendAttr(columnAttr, "table-id", TypeInt, 4);
        appendAttr(columnAttr, "column-name", TypeVarChar, 50);
        appendAttr(columnAttr, "column-type", TypeInt, 4);
        appendAttr(columnAttr, "column-length", TypeInt, 4);
        appendAttr(columnAttr, "column-position", TypeInt, 4);

    if (exists_test(TABLES_FILE_NAME)) {
        // read physical file into memory hashmap
        scanTablesOrColumns(true);
        scanTablesOrColumns(false);
    } else {
        // do nothing
    }

};

RelationManager::~RelationManager() = default;

RelationManager::RelationManager(const RelationManager &) = default;

RelationManager &RelationManager::operator=(const RelationManager &) = default;

/*
 * we need to hard code tables & columns in table.tbl.
 * */

RC RelationManager::createCatalog() {
    // set tables and column attribute (hard coding)
    // insert tuple into table.tbl & columns.tbl, using insertRecord (thus we need to mock data manually)

    // if already exist
    if (!rbfm->createFile(TABLES_NAME) || !rbfm->createFile(COLUMNS_NAME)) {
        return -1;
    }

    createTable(TABLES_NAME, tableAttr, true);
    createTable(COLUMNS_NAME, columnAttr, true);

    return -1;
}

// TODO: release fileHandle of table & column
RC RelationManager::deleteCatalog() {
    // iterate hashMap, delete all files
    return -1;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    return createTable(tableName, attrs, 0);
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs, bool isSystem) {
    // insert table info into hashMap
    attrMap[tableName] = attrs;
    std::string fileName = tableName + ".tbl";
    fileMap[tableName] = fileName;
    idMap[tableName] = curTableID;
    systemTableMap[tableName] = isSystem;

    // insert tuple into Table & Columns
    RID _;
    void* data = malloc(SM_BLOCK);
    generateTableData(curTableID, tableName, fileName, data);
    insertTuple(TABLES_NAME, data, _);

    for (unsigned i = 0; i < attrs.size(); i++) {
        Attribute attr = attrs[i];
        generateColumnData(curTableID, attr, i + 1, data);
        insertTuple(COLUMNS_NAME, data, _);
    }

    curTableID++;
    free(data);

    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    return -1;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    // if attr not exist
    if (attrMap.count(tableName) == 0) {
        return -1;
    }
    attrs = attrMap[tableName];
    return 0;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    // find all information from the hashmap, then just insert
    return -1;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    return -1;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    return -1;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    return -1;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    return -1;
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    return -1;
}

// Extra credit work
RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

// Extra credit work
RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}

void RelationManager::appendAttr(std::vector<Attribute> &attrArr, std::string name, AttrType type, AttrLength len) {
    Attribute attr;
    attr.name = std::move(name);
    attr.type = type;
    attr.length = len;
    attrArr.push_back(attr);
}

void RelationManager::generateTableData(unsigned id, std::string tableName, std::string fileName, void *data) {

}

void RelationManager::generateColumnData(unsigned id, Attribute attr, unsigned position, void *data) {

}

// totally using rbfm::scan
RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    std::string fileName = fileMap[tableName];
    FileHandle fileHandle;
    rbfm->openFile(fileName, fileHandle);

    std::vector<Attribute> recordDescriptor = attrMap[tableName];

    rbfm->scan(fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfmsi);

    return 0;
}


void RelationManager::scanTablesOrColumns(bool isTables) {
    RM_ScanIterator rmsi;
    std::vector<std::string> attributeNames;
    scan(isTables ? TABLES_NAME : COLUMNS_NAME, NULL_STRING, NO_OP, nullptr, attributeNames, rmsi);

    RID rid;
    void* data = malloc(SM_BLOCK);
    while (rmsi.getNextTuple(rid, data) != RM_EOF) {
        if (isTables) {
            parseTablesData(data);
        } else {
            parseColumnsData(data);
        }
    }
    free(data);
}

// convert data to table related hashmap
void RelationManager::parseTablesData(void *data) {

}

// convert data to column related hashmap
void RelationManager::parseColumnsData(void *data) {

}

RM_ScanIterator::RM_ScanIterator(){

}

RC RM_ScanIterator::getNextTuple(RID &nextRID, void *data) {
    return rbfmsi.getNextRecord(nextRID, data);
};

RC RM_ScanIterator::close() {
    return 0;
};