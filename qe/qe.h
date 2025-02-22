#ifndef _qe_h_
#define _qe_h_

#include "../rm/rm.h"

#define QE_EOF (-1)  // end of the index scan

typedef enum {
    MIN = 0, MAX, COUNT, SUM, AVG
} AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void *data;             // value
};

struct Condition {
    std::string lhsAttr;        // left-hand side attribute
    CompOp op;                  // comparison operator
    bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
};

class Iterator {
    // All the relational operators and access methods are iterators.
public:

    virtual RC getNextTuple(void *data) = 0;

    virtual void getAttributes(std::vector<Attribute> &attrs) const = 0;

    virtual ~Iterator() = default;

    static void getLengthAndDataFromTuple(void *tuple, std::vector<Attribute> const &attrs, const std::string &attrName, unsigned index, unsigned short &length, void *data);

    static unsigned getAttributesEstLength(std::vector<Attribute> const &attrs);

    static unsigned getTupleLength(std::vector<Attribute> const &attrs, void *data);

    static void concatenateTuple(void *data, void *left, void *right, std::vector<Attribute> const &leftAttrs,
                                 std::vector<Attribute> const &rightAttrs);

    static void addTupleToHashMap(void *data, void *key, AttrType attrType,
                                  std::unordered_map<int, std::vector<void *>> &intMap,
                                  std::unordered_map<std::string, std::vector<void *>> &stringMap);

};

class TableScan : public Iterator {
    // A wrapper inheriting Iterator over RM_ScanIterator
public:
    RelationManager &rm;
    RM_ScanIterator *iter;
    std::string tableName;
    std::vector<Attribute> attrs;
    std::vector<std::string> attrNames;
    RID rid{};

    TableScan(RelationManager &rm, const std::string &tableName, const char *alias = NULL) : rm(rm) {
        //Set members
        this->tableName = tableName;

        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Get Attribute Names from RM
        for (Attribute &attr : attrs) {
            // convert to char *
            attrNames.push_back(attr.name);
        }

        // Call RM scan to get an iterator
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new compOp and value
    void setIterator() {
        iter->close();
        delete iter;
        iter = new RM_ScanIterator();
        rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);
    };

    RC getNextTuple(void *data) override {
        return iter->getNextTuple(rid, data);
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~TableScan() override {
        iter->close();
    };
};

class IndexScan : public Iterator {
    // A wrapper inheriting Iterator over IX_IndexScan
public:
    RelationManager &rm;
    RM_IndexScanIterator *iter;
    std::string tableName;
    std::string attrName;
    std::vector<Attribute> attrs;
    char key[PAGE_SIZE]{};
    RID rid{};

    IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName, const char *alias = NULL)
            : rm(rm) {
        // Set members
        this->tableName = tableName;
        this->attrName = attrName;


        // Get Attributes from RM
        rm.getAttributes(tableName, attrs);

        // Call rm indexScan to get iterator
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

        // Set alias
        if (alias) this->tableName = alias;
    };

    // Start a new iterator given the new key range
    void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
        iter->close();
        delete iter;
        iter = new RM_IndexScanIterator();
        rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, *iter);
    };

    RC getNextTuple(void *data) override {
        int rc = iter->getNextEntry(rid, key);
        if (rc == 0) {
            rc = rm.readTuple(tableName.c_str(), rid, data);
        }
        return rc;
    };

    void getAttributes(std::vector<Attribute> &attributes) const override {
        attributes.clear();
        attributes = this->attrs;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        for (Attribute &attribute : attributes) {
            std::string tmp = tableName;
            tmp += ".";
            tmp += attribute.name;
            attribute.name = tmp;
        }
    };

    ~IndexScan() override {
        iter->close();
    };
};

class Filter : public Iterator {
    // Filter operator
public:
    std::vector<Attribute> relAttrs;
    std::string targetAttrName;
    Attribute targetAttribute;
    int targetAttrIndex;
    int rc = 0;

    std::string lhsAttr;        // left-hand side attribute
    CompOp op;                  // comparison operator
    bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE

    void* currentTuple;

    Iterator *input;

    Filter(Iterator *input,               // Iterator of input Rconst
            Condition &condition     // Selection condition
    );
    bool isTupleSatisfied();

    ~Filter() override;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

class Project : public Iterator {
    // Projection operator
public:
    std::vector<Attribute> relAttrs;
    std::unordered_map<std::string, Attribute> attrNameToAttrMap;
    std::unordered_map<int, int> targetIndexToTupleIndexMap;
    std::unordered_map<int, int> tupleIndexToOffsetMap;
    std::vector<std::string> targetAttributesNames;
    std::vector<Attribute> targetAttributes;

    void* currentTuple;
    Iterator *input;

    Project(Iterator *input,                    // Iterator of input R
            const std::vector<std::string> &attrNames);   // std::vector containing attribute names
    ~Project() override;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;
};

class BNLJoin : public Iterator {
    // Block nested-loop join operator
public:
    unsigned memoryLimit;
    unsigned currentMemory;

    int lrc;
    int rrc;

    Condition condition;
    Iterator *leftIt;
    TableScan *rightIt;
    AttrType type;

    std::vector<Attribute> leftAttrs;
    std::vector<Attribute> rightAttrs;

    Attribute leftAttr;

    unsigned leftAttrsEstLength;
    int leftAttrsIndex;
    int rightAttrsIndex;

    std::unordered_map<int, std::vector<void *>> intMap;
    std::unordered_map<std::string, std::vector<void *>> stringMap;

    std::stack<void *> outBuffer;
    void* tuple1;

    BNLJoin(Iterator *leftIn,            // Iterator of input R
            TableScan *rightIn,           // TableScan Iterator of input S
            const Condition &condition,   // Join condition
            const unsigned numPages       // # of pages that can be loaded into memory,
            //   i.e., memory block size (decided by the optimizer)
    );

    ~BNLJoin() override;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;

    void clean();

private:
    RecordBasedFileManager *rbfm;
};

class INLJoin : public Iterator {
    // Index nested-loop join operator
public:

    Iterator *leftIt;
    IndexScan *rightIt;

    Condition condition;

    std::vector<Attribute> leftAttrs;
    std::vector<Attribute> rightAttrs;

    int leftAttrIndex;
    int rightAttrIndex;

    int lrc;
    int rrc;

    void* tuple;

    INLJoin(Iterator *leftIn,           // Iterator of input R
            IndexScan *rightIn,          // IndexScan Iterator of input S
            const Condition &condition   // Join condition
    );

    ~INLJoin() override = default;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;

private:
    RecordBasedFileManager *rbfm;
};

// Optional for everyone. 10 extra-credit points
class GHJoin : public Iterator {
    // Grace hash join operator
public:

    int numPartitions;
    std::vector<std::string> leftPartitionNames;
    std::vector<std::string> rightPartitionNames;


    int lrc;
    int rrc;

    int leftAttrIndex;
    int rightAttrIndex;

    // index
    int nextpart;

    std::unordered_map<int, std::vector<void *>> intMap;
    std::unordered_map<std::string, std::vector<void *>> stringMap;

    std::stack<void *> outBuffer;

    std::vector<Attribute> leftAttrs;
    std::vector<Attribute> rightAttrs;

    std::vector<std::string> leftAttrNames;
    std::vector<std::string> rightAttrNames;

    Attribute leftAttr;
    Attribute rightAttr;

    FileHandle rightFileHandle;
    RBFM_ScanIterator rightRBFMSI;
    void* tuple1;

    AttrType attrType;

    GHJoin(Iterator *leftIn,               // Iterator of input R
           Iterator *rightIn,               // Iterator of input S
           const Condition &condition,      // Join condition (CompOp is always EQ)
           const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
    );

    ~GHJoin() override;

    RC getNextTuple(void *data) override;

    // For attribute in std::vector<Attribute>, name it as rel.attr
    void getAttributes(std::vector<Attribute> &attrs) const override;

    static std::string getFileName(int i, bool isLeft, std::string &attrName);

    void scanThenAddToPartitionFile(Iterator *iter, const std::vector<std::string> &partitionFileNames,
                                    const std::vector<Attribute> &attributes, int attrIndex);

    void clean();

private:
    RecordBasedFileManager *rbfm;
};

class Aggregate : public Iterator {
    // Aggregation operator
public:
    // Mandatory
    // Basic aggregation
    Aggregate(Iterator *input,          // Iterator of input R
              const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
              AggregateOp op            // Aggregate operation
    );

    Attribute aggAttr;
    AggregateOp op;
    Iterator *input;

    int aggrIndex;
    int groupIndex;
    std::vector<Attribute> attributes;

    float minValue;
    float maxValue;
    float totalCount;
    float valueSum;
    float valueAvg;

    bool endFlag = false;

    std::unordered_map<std::string, float> totalCountMap;
    std::unordered_map<std::string, float> minMap;
    std::unordered_map<std::string, float> maxMap;
    std::unordered_map<std::string, float> sumMap;
    std::unordered_map<std::string, float> avgMap;

    int outputIndex = 0;
    std::vector<std::string> groups;
    std::vector<std::vector<float>> aggregations;

    Attribute groupAttr;

    // Optional for everyone: 5 extra-credit points
    // Group-based hash aggregation
    Aggregate(Iterator *input,             // Iterator of input R
              const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
              const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
              AggregateOp op              // Aggregate operation
    );

    ~Aggregate() = default;

    RC getNextTuple(void *data) override;

    RC getNextTupleGroupBy(void *data);

    // Please name the output attribute as aggregateOp(aggAttr)
    // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
    // output attrname = "MAX(rel.attr)"
    void getAttributes(std::vector<Attribute> &attrs) const override;
};



#endif
