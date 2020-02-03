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
    // iterate hashMap, delete all files & hashmap
    return -1;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    return createTable(tableName, attrs, 0);
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs, bool isSystemTable) {
    // insert table info into hashMap
    attrMap[tableName] = attrs;
    std::string fileName = tableName + ".tbl";
    fileMap[tableName] = fileName;
    idMap[tableName] = curTableID;
    systemTableMap[tableName] = isSystemTable;

    // insert tuple into Table & Columns
    RID _;
    void* data = malloc(SM_BLOCK);
    generateTablesData(curTableID, tableName, fileName, data, isSystemTable);
    insertTuple(TABLES_NAME, data, _);

    for (unsigned i = 0; i < attrs.size(); i++) {
        Attribute attr = attrs[i];
        generateColumnsData(curTableID, attr, i + 1, data);
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

////Extra credit work
//RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
//    return -1;
//}
//
//// Extra credit work
//RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
//    return -1;
//}

void RelationManager::appendAttr(std::vector<Attribute> &attrArr, std::string name, AttrType type, AttrLength len) {
    Attribute attr;
    attr.name = std::move(name);
    attr.type = type;
    attr.length = len;
    attrArr.push_back(attr);
}

void RelationManager::generateTablesData(unsigned id, std::string tableName, std::string fileName, void *data,
                                         bool isSystemTable) {
    unsigned size = TABLES_ATTRIBUTE_SIZE;
    unsigned nullIndicatorSize = (size + 7) / 8;
    //pos indicates current position
    unsigned pos = 0;

    //write null indicator into start position
    unsigned nullIndicator = 0X00;
    memcpy(data, &nullIndicator, nullIndicatorSize);
    pos += nullIndicatorSize;

    //write id into corresponding position
    memcpy((char *) data + pos, &id, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //write tableName into corresponding position
    unsigned tableNameSize =  tableName.size();
    memcpy((char *) data + pos, &tableNameSize, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy((char *) data + pos, &tableName, tableNameSize);
    pos += tableNameSize;

    //write fileName into corresponding position
    unsigned fileNameSize =  fileName.size();
    memcpy((char *) data + pos, &fileNameSize, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy((char *) data + pos, &fileName, fileNameSize);
    pos += fileNameSize;

    //write isSystemTable into corresponding position
    memcpy((char *) data + pos, &isSystemTable, SYSTEM_INDICATOR_SIZE);

}

void RelationManager::generateColumnsData(unsigned id, Attribute attr, unsigned position, void *data) {
    unsigned size = COLUMNS_ATTRIBUTE_SIZE;
    unsigned nullIndicatorSize = (size + 7) / 8;
    //pos indicates current position
    unsigned pos = 0;

    unsigned nullIndicator = 0X00;
    memcpy(data, &nullIndicator, nullIndicatorSize);
    pos += nullIndicatorSize;

    //write id into corresponding position
    memcpy((char *) data + pos, &id, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //write attr into corresponding position
    unsigned attrNameSize =  attr.name.size();
    memcpy((char *) data + pos, &attrNameSize, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy((char *) data + pos, &attr.name, attrNameSize);
    pos += attrNameSize;

    memcpy((char *) data + pos, &attr.type, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;


    memcpy((char *) data + pos, &attr.length, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //write position into corresponding position
    memcpy((char *) data + pos, &position, UNSIGNED_SIZE);
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

    unsigned id;
    std::string tableName;
    std::string fileName;
    bool isSystemTable;

    //pos start after null indicator
    unsigned pos = UNSIGNED_SIZE;

    //get id from corresponding position
    memcpy(&id, (char *) data + pos,UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //get tableName from corresponding position
    unsigned tableNameLength;
    memcpy(&tableNameLength, (char *) data + pos, INT_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy(&tableName, (char *) data + pos, tableNameLength);
    pos += tableNameLength;

    //get fileName from corresponding position
    unsigned fileNameLength;
    memcpy(&fileNameLength, (char *) data + pos, INT_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy(&fileName, (char *) data + pos, fileNameLength);
    pos += fileNameLength;

    //get isSystemTable from corresponding position
    memcpy(&isSystemTable, (char *) data + pos, SYSTEM_INDICATOR_SIZE);

    fileName = tableName + ".tbl";
    fileMap[tableName] = fileName;
    idMap[tableName] = id;
    systemTableMap[tableName] = isSystemTable;
}

// convert data to column related hashmap
void RelationManager::parseColumnsData(void *data) {

    unsigned id;
    Attribute attr;
    unsigned position;

    //pos start after null indicator
    unsigned pos = UNSIGNED_SIZE;

    //get id from corresponding position
    memcpy(&id, (char *) data + pos, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    std::string tableName;
    ////怎么拿到tablename啊 是不是还需要一个key=id, value= tablename 的map

    //write attr into corresponding position
    unsigned attrNameLength;
    memcpy(&attrNameLength, (char *) data + pos, INT_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy( &attr.name, (char *) data + pos, attr.name.size());
    pos += attrNameLength;

    memcpy(&attr.type, (char *) data + pos, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy(&attr.length, (char *) data + pos, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //get position from corresponding position
    memcpy( &position, (char *) data + pos, UNSIGNED_SIZE);

    //add new attr into corresponding attribute vector
    attrMap[tableName].push_back(attr);
}

RM_ScanIterator::RM_ScanIterator(){

}

RC RM_ScanIterator::getNextTuple(RID &nextRID, void *data) {
    return rbfmsi.getNextRecord(nextRID, data);
};

RC RM_ScanIterator::close() {
    return 0;
};