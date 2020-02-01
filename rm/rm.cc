#include "rm.h"

#include <utility>

RelationManager &RelationManager::instance() {
    static RelationManager _relation_manager = RelationManager();
    return _relation_manager;
}

RelationManager::RelationManager() {
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

    RecordBasedFileManager::instance().createFile(TABLES_NAME);
    RecordBasedFileManager::instance().createFile(COLUMNS_NAME);

    RecordBasedFileManager::instance().openFile(TABLES_NAME, tableFileHandle);
    RecordBasedFileManager::instance().openFile(COLUMNS_NAME, columnFileHandle);

    RecordBasedFileManager::instance().insertRecord(tableFileHandle, tableAttr, data, rid);


    return -1;
}

// TODO: release fileHandle of table & column
RC RelationManager::deleteCatalog() {
    // find files in table.tbl
    // delete all files
    return -1;
}

RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
    // insert tuple into table.tbl & column.tbl
    return -1;
}

RC RelationManager::deleteTable(const std::string &tableName) {
    return -1;
}

RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
    return -1;
}

RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
    // find corresponding table in table.tbl
    // find attribute information in column.tbl
    // call insertRecord
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

RC RelationManager::scan(const std::string &tableName,
                         const std::string &conditionAttribute,
                         const CompOp compOp,
                         const void *value,
                         const std::vector<std::string> &attributeNames,
                         RM_ScanIterator &rm_ScanIterator) {
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

void RelationManager::generateTableData(unsigned id, std::string tableName, std::string fileName) {

}

void RelationManager::generateColumnData(unsigned id, std::string name, std::string type,
                                         unsigned length, unsigned position) {

}




