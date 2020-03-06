#include "rm.h"
#include <sys/stat.h>
#include <utility>
#include <map>
#include <algorithm>

inline bool exists_test(const std::string &name) {
    struct stat buffer;
    return (stat(name.c_str(), &buffer) == 0);
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

    tableNameToAttrMap[TABLES_NAME] = tableAttr;
    tableNameToAttrMap[COLUMNS_NAME] = columnAttr;

    tableNameToFileMap[TABLES_NAME] = TABLES_FILE_NAME;
    tableNameToFileMap[COLUMNS_NAME] = COLUMNS_FILE_NAME;

    for (auto attr : tableAttr) {
        tableAttributeNames.push_back(attr.name);
    }

    for (auto attr: columnAttr) {
        columnAttributeNames.push_back(attr.name);
    }

    if (exists_test(TABLES_FILE_NAME)) {
        // read physical file into memory hashmap
        initScanTablesOrColumns(true);
        initScanTablesOrColumns(false);
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
    if (rbfm->createFile(TABLES_FILE_NAME) || rbfm->createFile(COLUMNS_FILE_NAME)) {
        return -1;
    }

    // two essential map should be built, if not rebuild them
    if (tableNameToAttrMap.count(TABLES_NAME) == 0) {
        tableNameToAttrMap[TABLES_NAME] = tableAttr;
        tableNameToAttrMap[COLUMNS_NAME] = columnAttr;

        tableNameToFileMap[TABLES_NAME] = TABLES_FILE_NAME;
        tableNameToFileMap[COLUMNS_NAME] = COLUMNS_FILE_NAME;
    }

    createTable(TABLES_NAME, tableAttr, true);
    createTable(COLUMNS_NAME, columnAttr, true);

    return 0;
}

RC RelationManager::deleteCatalog() {
    rbfm->destroyFile(TABLES_FILE_NAME);
    rbfm->destroyFile(COLUMNS_FILE_NAME);

    // delete all files
    for (std::pair<std::string, std::string> element : tableNameToFileMap) {
        rbfm->destroyFile(element.second);
    }

    // clear all hashmap
    tableNameToFileMap.clear();
    tableNameToIdMap.clear();
    idToTableNameMap.clear();
    tableNameToIsSystemTableMap.clear();
    tableNameToAttrMap.clear();

    return 0;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    return createTable(tableName, attrs, false);
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs, bool isSystemTable) {
    // insert table info into hashMap
    std::string fileName = tableName + EXT;
    if (tableNameToAttrMap.find(tableName) == tableNameToAttrMap.end()) {
        tableNameToAttrMap[tableName] = attrs;
        tableNameToFileMap[tableName] = fileName;
    }
    tableNameToIdMap[tableName] = curTableID;
    idToTableNameMap[curTableID] = tableName;
    tableNameToIsSystemTableMap[tableName] = isSystemTable;

    // insert tuple into Table & Columns
    RID _;
    void *data = malloc(SM_BLOCK);
    generateTablesData(curTableID, tableName, fileName, data, isSystemTable);
    insertTuple(TABLES_NAME, data, _, true);

    for (unsigned i = 0; i < attrs.size(); i++) {
        Attribute attr = attrs[i];
        generateColumnsData(curTableID, attr, i + 1, data);
        insertTuple(COLUMNS_NAME, data, _, true);
    }

    // create file if not exist
    if (!exists_test(fileName)) {
        rbfm->createFile(fileName);
    }

    curTableID++;
    free(data);

    return 0;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    if (tableNameToFileMap.count(tableName) == 0) {
        return -1;
    }
    bool isSystemTable = tableNameToIsSystemTableMap[tableName];
    if (isSystemTable) {
        return -1;
    }

    // delete table tuple in TABLES
    RM_ScanIterator rmsi_table;

    scan(TABLES_NAME, NULL_STRING, NO_OP, nullptr, tableAttributeNames, rmsi_table);

    RID rid;
    void *data = malloc(PAGE_SIZE);
    while (rmsi_table.getNextTuple(rid, data) != RM_EOF) {
        std::string tupleTableName, fileName;
        unsigned id;
        bool _;
        parseTablesData(data, tupleTableName, fileName, id, _);

        // if find the tuple in TABLES, delete the record
        if (tableName == tupleTableName) {
            deleteTuple(TABLES_NAME, rid, true);
        }
    }

    unsigned id = tableNameToIdMap[tableName];
    // delete column tuple in COLUMNS
    RM_ScanIterator rmsi_column;
    scan(COLUMNS_NAME, NULL_STRING, NO_OP, nullptr, columnAttributeNames, rmsi_column);
    while (rmsi_column.getNextTuple(rid, data) != RM_EOF) {
        std::string tupleTableName, fileName;
        Attribute attr;
        unsigned tupleID, position;
        parseColumnsData(data, tupleID, attr, position);

        // if find the tuple in COLUMNS, delete the record
        if (id == tupleID) {
            deleteTuple(COLUMNS_NAME, rid, true);
        }
    }
    free(data);

    std::string fileName = tableNameToFileMap[tableName];
    rbfm->destroyFile(fileName);
    tableNameToFileMap.erase(tableName);

    tableNameToIdMap.erase(tableName);
    idToTableNameMap.erase(id);

    tableNameToIsSystemTableMap.erase(tableName);
    tableNameToAttrMap.erase(tableName);

    return 0;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    // if attr not exist
    if (tableNameToAttrMap.count(tableName) == 0) {
        return -1;
    }
    attrs = tableNameToAttrMap[tableName];
    return 0;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    return insertTuple(tableName, data, rid, false);
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid, bool isInternalCall) {
    if (tableNameToAttrMap.count(tableName) == 0) {
        return -1;
    }

    if (!isInternalCall && tableNameToIsSystemTableMap[tableName]) {
        return -1;
    }

    std::string fileName = tableNameToFileMap[tableName];
    FileHandle fileHandle;
    rbfm->openFile(fileName, fileHandle);

    auto attrs = tableNameToAttrMap[tableName];
    if (rbfm->insertRecord(fileHandle, attrs, data, rid) != 0) {
        rbfm->closeFile(fileHandle);
        return -1;
    };

    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
    return deleteTuple(tableName, rid, false);
}

RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid, bool isInternalCall) {
    if (tableNameToAttrMap.count(tableName) == 0) {
        return -1;
    }

    if (!isInternalCall && tableNameToIsSystemTableMap[tableName]) {
        return -1;
    }

    std::string fileName = tableNameToFileMap[tableName];
    FileHandle fileHandle;
    rbfm->openFile(fileName, fileHandle);

    auto attrs = tableNameToAttrMap[tableName];
    if (rbfm->deleteRecord(fileHandle, attrs, rid) != 0) {
        rbfm->closeFile(fileHandle);
        return -1;
    };

    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
    if (tableNameToAttrMap.count(tableName) == 0) {
        return -1;
    }

    if (tableNameToIsSystemTableMap[tableName]) {
        return -1;
    }

    std::string fileName = tableNameToFileMap[tableName];
    FileHandle fileHandle;
    rbfm->openFile(fileName, fileHandle);

    auto attrs = tableNameToAttrMap[tableName];
    if (rbfm->updateRecord(fileHandle, attrs, data, rid) != 0) {
        rbfm->closeFile(fileHandle);
        return -1;
    };

    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
    if (tableNameToAttrMap.count(tableName) == 0) {
        return -1;
    }

    std::string fileName = tableNameToFileMap[tableName];
    FileHandle fileHandle;
    rbfm->openFile(fileName, fileHandle);

    auto attrs = tableNameToAttrMap[tableName];
    if (rbfm->readRecord(fileHandle, attrs, rid, data) != 0) {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    rbfm->closeFile(fileHandle);
    return 0;
}

RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data) {
    return rbfm->printRecord(attrs, data);
}

RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                  void *data) {
    if (tableNameToAttrMap.count(tableName) == 0) {
        return -1;
    }

    std::string fileName = tableNameToFileMap[tableName];
    FileHandle fileHandle;
    rbfm->openFile(fileName, fileHandle);

    auto attrs = tableNameToAttrMap[tableName];
    if (rbfm->readAttribute(fileHandle, attrs, rid, attributeName, data) != 0) {
        rbfm->closeFile(fileHandle);
        return -1;
    }

    rbfm->closeFile(fileHandle);
    return 0;
}


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
    unsigned char nullIndicator = 0x00;
    memcpy(data, &nullIndicator, nullIndicatorSize * NULL_INDICATOR_UNIT_SIZE);
    pos += nullIndicatorSize * NULL_INDICATOR_UNIT_SIZE;

    //write id into corresponding position
    memcpy((char *) data + pos, &id, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //write tableName into corresponding position
    unsigned tableNameSize = tableName.length();
    memcpy((char *) data + pos, &tableNameSize, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy((char *) data + pos, tableName.c_str(), tableNameSize);
    pos += tableNameSize;

    //write fileName into corresponding position
    unsigned fileNameSize = fileName.length();
    memcpy((char *) data + pos, &fileNameSize, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy((char *) data + pos, fileName.c_str(), fileNameSize);
    pos += fileNameSize;

    //write isSystemTable into corresponding position
    int systemTable = isSystemTable ? 1 : 0;
    memcpy((char *) data + pos, &systemTable, SYSTEM_INDICATOR_SIZE);
}

void RelationManager::generateColumnsData(unsigned id, Attribute attr, unsigned position, void *data) {
    unsigned size = COLUMNS_ATTRIBUTE_SIZE;
    unsigned nullIndicatorSize = (size + 7) / 8;
    //pos indicates current position
    unsigned pos = 0;

    unsigned char nullIndicator = 0x00;
    memcpy(data, &nullIndicator, nullIndicatorSize * NULL_INDICATOR_UNIT_SIZE);
    pos += nullIndicatorSize * NULL_INDICATOR_UNIT_SIZE;

    //write id into corresponding position
    memcpy((char *) data + pos, &id, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //write attr into corresponding position
    unsigned attrNameSize = attr.name.length();
    memcpy((char *) data + pos, &attrNameSize, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy((char *) data + pos, attr.name.c_str(), attrNameSize);
    pos += attrNameSize;

    memcpy((char *) data + pos, &attr.type, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy((char *) data + pos, &attr.length, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //write position into corresponding position
    memcpy((char *) data + pos, &position, UNSIGNED_SIZE);
}

void RelationManager::initScanTablesOrColumns(bool isTables) {
    RM_ScanIterator rmsi;

    scan(isTables ? TABLES_NAME : COLUMNS_NAME, NULL_STRING, NO_OP, nullptr,
         isTables ? tableAttributeNames : columnAttributeNames, rmsi);

    // init a temp RMAttribute vector
    std::unordered_map<std::string, std::vector<RMAttribute>> tempAttrMap;

    RID rid;
    void *data = malloc(PAGE_SIZE);
    while (rmsi.getNextTuple(rid, data) != RM_EOF) {
        if (isTables) {
            std::string tableName, fileName;
            unsigned id;
            bool isSystemTable;
            parseTablesData(data, tableName, fileName, id, isSystemTable);

            // write into hashMap
            tableNameToFileMap[tableName] = fileName;
            tableNameToIdMap[tableName] = id;
            idToTableNameMap[id] = tableName;
            tableNameToIsSystemTableMap[tableName] = isSystemTable;

            // prepare to get the max tableID
            curTableID = std::max(curTableID, id);
        } else {
            unsigned id;
            Attribute attr;
            unsigned position;
            parseColumnsData(data, id, attr, position);

            //add new attr into corresponding attribute vector
            std::string tableName = idToTableNameMap[id];
            if (tableName != TABLES_NAME && tableName != COLUMNS_NAME) {
                if (tempAttrMap.find(tableName) == tempAttrMap.end()) {
                    std::vector<RMAttribute> vector;
                    tempAttrMap[tableName] = vector;
                }
                RMAttribute rmAttr;
                rmAttr.pos = position;
                rmAttr.attribute = attr;
                tempAttrMap[tableName].push_back(rmAttr);
            }
        }
    }

    // sort column by position number
    for (auto & it : tempAttrMap) {
        auto tableName = it.first;
        // Using lambda expressions in C++11
        sort(it.second.begin(), it.second.end(), [](const RMAttribute& lhs, const RMAttribute& rhs) {
            return lhs.pos < rhs.pos;
        });

        std::vector<Attribute> attrs;
        for (auto & it1 : it.second) {
            attrs.push_back(it1.attribute);
        }
        tableNameToAttrMap[tableName] = attrs;
    }

    // after scan table set current tableID + 1
    if (isTables) {
        curTableID += 1;
    }

    free(data);
}

// convert data to table related hashmap
void RelationManager::parseTablesData(void *data, std::string &tableName, std::string &fileName, unsigned int &id,
                                      bool &isSystemTable) {
    //pos start after null indicator
    unsigned pos = NULL_INDICATOR_UNIT_SIZE;

    //get id from corresponding position
    memcpy(&id, (char *) data + pos, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //get tableName from corresponding position
    unsigned tableNameLength;
    memcpy(&tableNameLength, (char *) data + pos, INT_SIZE);
    pos += UNSIGNED_SIZE;

    tableName.assign((char *)data + pos, tableNameLength);
    pos += tableNameLength;

    //get fileName from corresponding position
    unsigned fileNameLength;
    memcpy(&fileNameLength, (char *) data + pos, INT_SIZE);
    pos += UNSIGNED_SIZE;

    fileName.assign((char *)data + pos, fileNameLength);
    pos += fileNameLength;

    //get isSystemTable from corresponding position
    int systemTable;
    memcpy(&systemTable, (char *) data + pos, SYSTEM_INDICATOR_SIZE);
    isSystemTable = systemTable != 0;
}

// convert data to column related hashmap
void RelationManager::parseColumnsData(void *data, unsigned int &id, Attribute &attr, unsigned &position) {
    //pos start after null indicator
    unsigned pos = NULL_INDICATOR_UNIT_SIZE;

    //get id from corresponding position
    memcpy((char *) &id, (char *) data + pos, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //write attr into corresponding position
    unsigned attrNameLength;
    memcpy((char *) &attrNameLength, (char *) data + pos, INT_SIZE);
    pos += UNSIGNED_SIZE;

    attr.name.assign((char *) data + pos, attrNameLength);
    pos += attrNameLength;

    memcpy((char *) &attr.type, (char *) data + pos, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    memcpy((char *) &attr.length, (char *) data + pos, UNSIGNED_SIZE);
    pos += UNSIGNED_SIZE;

    //get position from corresponding position
    memcpy((char *) &position, (char *) data + pos, UNSIGNED_SIZE);
}

// totally using rbfm::scan
RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
    std::string fileName = tableNameToFileMap[tableName];
    rbfm->openFile(fileName, rm_ScanIterator.fileHandle);

    std::vector<Attribute> recordDescriptor = tableNameToAttrMap[tableName];

    rbfm->scan(rm_ScanIterator.fileHandle, recordDescriptor, conditionAttribute, compOp, value, attributeNames,
               rm_ScanIterator.rbfmsi);

    return 0;
}

RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
    return -1;
}

RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

RM_ScanIterator::RM_ScanIterator() {

}

RC RM_ScanIterator::getNextTuple(RID &nextRID, void *data) {
    return rbfmsi.getNextRecord(nextRID, data);
};

RC RM_ScanIterator::close() {
    return rbfmsi.close();
};

// QE IX related
RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName) {
    return -1;
}

RC RelationManager::indexScan(const std::string &tableName,
                              const std::string &attributeName,
                              const void *lowKey,
                              const void *highKey,
                              bool lowKeyInclusive,
                              bool highKeyInclusive,
                              RM_IndexScanIterator &rm_IndexScanIterator) {
    return -1;
}