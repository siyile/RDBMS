
#include <sstream>
#include "qe.h"


Filter::Filter(Iterator *input, Condition &condition) {
    this->targetAttrName = condition.lhsAttr;
    input->getAttributes(this->relAttrs);
    this->input = input;
    op = condition.op;
    rhsValue = condition.rhsValue;
    targetAttrIndex = RecordBasedFileManager::getAttrIndex(relAttrs, targetAttrName);

    currentTuple = malloc(PAGE_SIZE);

    for (const Attribute& attr : relAttrs) {
        if (attr.name == targetAttrName) {
            this->targetAttribute = attr;
            break;
        }
    }
}

RC Filter::getNextTuple(void *data) {
    while (rc != QE_EOF) {
        rc = input->getNextTuple(currentTuple);
        // break when satisfied tuple
        if (rc == QE_EOF || isTupleSatisfied()) {
            break;
        }
    }

    if (rc == QE_EOF) {
        return QE_EOF;
    }

    memcpy(data, currentTuple, Iterator::getTupleLength(relAttrs, currentTuple));

    return 0;
}

bool Filter::isTupleSatisfied() {
    // get data from tuple
    void* data = malloc(PAGE_SIZE);
    RecordBasedFileManager::readAttributeFromRawData(currentTuple, data, relAttrs, "", targetAttrIndex);

    bool res = RecordBasedFileManager::compareValue(rhsValue.data, data, op, targetAttribute.type);

    free(data);
    return res;
}

void Filter::getAttributes(std::vector<Attribute> &attrs) const {
    for (auto const & it : relAttrs) {
        attrs.push_back(it);
    }
}

Filter::~Filter() {
    free(currentTuple);
}


Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
    input->getAttributes(this->relAttrs);
    currentTuple = malloc(PAGE_SIZE);

    this->targetAttributesNames.insert(targetAttributesNames.begin(), attrNames.begin(), attrNames.end());

    this->input = input;
    for (int i = 0; i < relAttrs.size(); i++) {
        // PositionToAttrMap[i] = relAttrs[i];
        attrNameToAttrMap[relAttrs[i].name] = relAttrs[i];
        for (int j = 0; j < targetAttributesNames.size(); j++) {
            if (targetAttributesNames[j] == relAttrs[i].name) {
                targetIndexToTupleIndexMap[j] = i;
            }
        }
    }

    for (int i = 0; i < attrNames.size(); i++) {
        int tupleIndex = targetIndexToTupleIndexMap[i];
        targetAttributes.push_back(relAttrs[tupleIndex]);
    }
}

Project::~Project() {
    free(currentTuple);
}

RC Project::getNextTuple(void *data) {
    int rc = input->getNextTuple(currentTuple);
    if (rc == QE_EOF) {
        return QE_EOF;
    }

    unsigned short size = relAttrs.size();
    unsigned short pos = 0;

    int *attrsExist = new int[size];

    RecordBasedFileManager::getAttrExistArray(pos, attrsExist, currentTuple, size, false);

    unsigned short nullIndicatorSize = (targetAttributesNames.size() + 7) / 8;
    auto *nullIndicator = new unsigned char[nullIndicatorSize];
    // set nullIndicator all to 1
    memset(nullIndicator, 0xff, nullIndicatorSize);

    for (int i = 0; i < size; i++){
        if (attrsExist[i] != 1) {
            continue;
        }

        tupleIndexToOffsetMap[i] = pos;
        if (relAttrs[i].type != TypeVarChar) {
            pos += UNSIGNED_SIZE;
        } else {
            unsigned length;
            memcpy(&length, (char *)currentTuple + pos, UNSIGNED_SIZE);
            pos += UNSIGNED_SIZE + length;
        }
    }

    unsigned dataPos = nullIndicatorSize;
    unsigned offset;
    int tupleIndex;

    for (int i = 0; i < targetAttributesNames.size(); i++) {
        tupleIndex = targetIndexToTupleIndexMap[i];
        if (attrsExist[tupleIndex] != 1) {
            continue;
        } else {
            offset = tupleIndexToOffsetMap[tupleIndex];
            if (relAttrs[tupleIndex].type != TypeVarChar) {
                memcpy((char*)data + dataPos, (char*)currentTuple + offset, UNSIGNED_SIZE);
                dataPos += UNSIGNED_SIZE;
            } else {
                unsigned length;
                memcpy(&length, (char*)currentTuple + offset, UNSIGNED_SIZE);
                memcpy((char*)data + dataPos, (char*) currentTuple + offset, length + UNSIGNED_SIZE);
                dataPos += UNSIGNED_SIZE + length;
            }
        }
        RecordBasedFileManager::setNullIndicator(nullIndicator, i, 0);
    }
    memcpy(data, nullIndicator, nullIndicatorSize);
    delete [] attrsExist;
    delete [] nullIndicator;
    return 0;
}

void Project::getAttributes(std::vector<Attribute> &attrs) const {
    for (auto const & it : targetAttributes) {
        attrs.push_back(it);
    }
}

unsigned Iterator::getAttributesEstLength(std::vector<Attribute> const &attrs) {
    unsigned length = 0;
    for (auto const & it : attrs) {
        length += it.length;
    }
    return length;
}

unsigned Iterator::getTupleLength(std::vector<Attribute> const &attrs, void *data) {
    unsigned short length = 0;
    int *attrsExist = new int[attrs.size()];
    RecordBasedFileManager::getAttrExistArray(length, attrsExist, data, attrs.size(), false);
    for (int i = 0; i < attrs.size(); i++) {
        if (attrsExist[i]) {
            if (attrs[i].type == TypeInt || attrs[i].type == TypeReal) {
                length += UNSIGNED_SIZE;
            } else {
                unsigned stringLength;
                memcpy(&stringLength, (char *) data + length, UNSIGNED_SIZE);
                length += UNSIGNED_SIZE;
                length += stringLength;
            }
        }
    }
    delete[](attrsExist);
    return length;
}

void Iterator::concatenateTuple(void *data, void *left, void *right, std::vector<Attribute> const &leftAttrs,
                                std::vector<Attribute> const &rightAttrs) {
    int lSize = leftAttrs.size();
    int rSize = rightAttrs.size();

    int leftNullIndicatorSize = (lSize + 7) / 8;
    int rightNullIndicatorSize = (rSize + 7) / 8;

    int nullIndicatorSize = (lSize + rSize + 7) / 8;

    // copy left null indicator
    memcpy(data, left, leftNullIndicatorSize);

    // continue copy indicator
    int l = lSize;

    for (int r = 0; r < rSize; r++, l++) {
        RecordBasedFileManager::setNullIndicator(data, l, RecordBasedFileManager::getNullIndicator(right, r));
    }

    unsigned pos = nullIndicatorSize;
    unsigned leftLength = getTupleLength(leftAttrs, left) - leftNullIndicatorSize;
    memcpy((char *) data + pos, (char *) left + leftNullIndicatorSize, leftLength);
    pos += leftLength;
    unsigned rightLength = getTupleLength(rightAttrs, right) - rightNullIndicatorSize;
    memcpy((char *) data + pos, (char *) right + rightNullIndicatorSize, rightLength);
}

void Iterator::addTupleToHashMap(void *data, void *key, AttrType attrType,
                                 std::unordered_map<int, std::vector<void *>> &intMap,
                                 std::unordered_map<std::string, std::vector<void *>> &stringMap) {
    if (attrType != TypeVarChar) {
        int intKey;
        memcpy(&intKey, key, INT_SIZE);
        if (intMap.find(intKey) == intMap.end()) {
            std::vector<void *> vector;
            intMap[intKey] = vector;
        }
        intMap[intKey].push_back(data);
    } else {
        unsigned stringLength;
        memcpy(&stringLength, key, UNSIGNED_SIZE);
        std::string stringKey((char *) key + UNSIGNED_SIZE, stringLength);
        if (stringMap.find(stringKey) == stringMap.end()) {
            std::vector<void *> vector;
            stringMap[stringKey] = vector;
        }
        stringMap[stringKey].push_back(data);
    }
}

/*
 * 1. read tuple from leftIt, till the memory limit
 *      - hash the tuple by key : data
 *
 *      - getNextTuple, calculate length and get key from the tuple
 *      - insert key into hashMap
 *
 *
 * 2. start table scan rightIt, scan all the tuple
 *      - if there is data in hash map, connect them
 *
 *  use leftRC(lrc) & right RC(rrc) to indicate whether the join is over
 *
 * */
BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {
    rbfm = &RecordBasedFileManager::instance();

    memoryLimit = (numPages - 2) * PAGE_SIZE;
    currentMemory = 0;

    lrc = 0;
    rrc = 0;

    if (!condition.bRhsIsAttr) {
        throw std::logic_error("check right hand Attr");
    }
    this->condition = condition;
    this->leftIt = leftIn;
    this->rightIt = rightIn;

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);

    leftAttrsEstLength = getAttributesEstLength(leftAttrs);
    leftAttrsIndex = RecordBasedFileManager::getAttrIndex(leftAttrs, condition.lhsAttr);
    rightAttrsIndex = RecordBasedFileManager::getAttrIndex(rightAttrs, condition.rhsAttr);

    tuple1 = malloc(PAGE_SIZE);


}

BNLJoin::~BNLJoin() {
    clean();
    free(tuple1);
}


RC BNLJoin::getNextTuple(void *data) {
    void* key = malloc(PAGE_SIZE);
    // outBuffer is empty, need to get something in it.
    // when lrc == rrc == QE_EOF, we end
    bool foundRight = false;
    while (!foundRight && outBuffer.empty() && (lrc != QE_EOF || rrc != QE_EOF)) {
        // if current memory is 0, need to start scan to fill input buffer
        if (currentMemory == 0) {
            while (currentMemory + leftAttrsEstLength <= memoryLimit) {
                void *tuple = malloc(PAGE_SIZE);
                lrc = leftIt->getNextTuple(tuple);
                if (lrc == QE_EOF) {
                    free(tuple);
                    break;
                }
                // calculate Length & get key from tuple
                unsigned short tupleLength;
                getLengthAndDataFromTuple(tuple, leftAttrs, "", leftAttrsIndex, tupleLength, key);
                currentMemory += tupleLength;

                // add key to hashMap
                addTupleToHashMap(tuple, key, leftAttr.type, intMap, stringMap);
            }
        }

        // search in tableScan
        while (!foundRight) {
            rrc = rightIt->getNextTuple(tuple1);
            if (rrc == QE_EOF) {
                break;
            }
            unsigned short tupleLength;
            getLengthAndDataFromTuple(tuple1, rightAttrs, "", rightAttrsIndex, tupleLength, key);
            if (leftAttr.type != TypeVarChar) {
                int intKey;
                memcpy(&intKey, key, INT_SIZE);
                if (intMap.find(intKey) != intMap.end()) {
                    for (auto const & it : intMap[intKey]) {
                        outBuffer.push(it);
                    }
                    intMap.erase(intKey);
                    foundRight = true;
                }
            } else {
                unsigned stringLength;
                memcpy(&stringLength, key, UNSIGNED_SIZE);
                std::string stringKey ((char *) key + UNSIGNED_SIZE, stringLength);
                if (stringMap.find(stringKey) != stringMap.end()) {
                    for (auto const & it : stringMap[stringKey]) {
                        outBuffer.push(it);
                    }
                    stringMap.erase(stringKey);
                    foundRight = true;
                }
            }
        }

        // if right search is end, clear all hashMap & release memory
        if (rrc == QE_EOF && lrc != QE_EOF) {
            currentMemory = 0;
            //restart iteration
            rightIt->setIterator();
            rrc = 0;
            clean();
        }
    }

    free(key);
    // get record from out buffer
    if (outBuffer.empty()) {
        return QE_EOF;
    } else {
        void* tuple = outBuffer.top();
        outBuffer.pop();
        concatenateTuple(data, tuple, tuple1, leftAttrs, rightAttrs);
        free(tuple);
        return 0;
    }
}

void BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
    for (auto const & it : leftAttrs) {
        attrs.push_back(it);
    }

    for (auto const & it : rightAttrs) {
        attrs.push_back(it);
    }
}

void BNLJoin::clean() {
    for (auto & it : intMap) {
        for (auto & it1 : it.second) {
            free(it1);
        }
    }
    intMap.clear();
    for (auto & it : stringMap) {
        for (auto & it1 : it.second) {
            free(it1);
        }
    }
    stringMap.clear();
}

void Iterator::getLengthAndDataFromTuple(void *tuple, std::vector<Attribute> const &attrs, const std::string &attrName,
                                         unsigned index, unsigned short &length, void *data) {
    length = 0;
    int *attrsExist = new int[attrs.size()];
    RecordBasedFileManager::getAttrExistArray(length, attrsExist, tuple, attrs.size(), false);
    for (int i = 0; i < attrs.size(); i++) {
        if (i == index || (i == -1 && attrName == attrs[i].name)) {
            if (attrsExist[i] == 0) {
                throw std::logic_error("NULL KEY APPEAR!");
            }
            if (attrs[i].type == TypeInt || attrs[i].type == TypeReal) {
                memcpy(data, (char *) tuple + length, UNSIGNED_SIZE);
            } else {
                unsigned stringLength;
                memcpy(&stringLength, (char *) tuple + length, UNSIGNED_SIZE);
                memcpy(data, (char *) tuple + length, stringLength + UNSIGNED_SIZE);
            }
        }
        if (attrsExist[i]) {
            if (attrs[i].type == TypeInt || attrs[i].type == TypeReal) {
                length += UNSIGNED_SIZE;
            } else {
                unsigned stringLength;
                memcpy(&stringLength, (char *) tuple + length, UNSIGNED_SIZE);
                length += UNSIGNED_SIZE;
                length += stringLength;
            }
        }
    }

    delete[](attrsExist);
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    rbfm = &RecordBasedFileManager::instance();

    leftIt = leftIn;
    rightIt = rightIn;
    this->condition = condition;

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);

    lrc = 0;
    rrc = QE_EOF;

    leftAttrIndex = RecordBasedFileManager::getAttrIndex(leftAttrs, condition.lhsAttr);
    rightAttrIndex = RecordBasedFileManager::getAttrIndex(rightAttrs, condition.rhsAttr);

    tuple = malloc(PAGE_SIZE);
}

RC INLJoin::getNextTuple(void *data) {
    void* key = malloc(PAGE_SIZE);
    void* tuple1 = malloc(PAGE_SIZE);
    while (lrc != QE_EOF || rrc != QE_EOF) {
        // if rightIt is over, leftIt get next, restart rightIt scan
        if (rrc == QE_EOF) {
            // leftIt get Next
            unsigned short _;
            lrc = leftIt->getNextTuple(tuple);
            if (lrc == QE_EOF) {
                break;
            }
            getLengthAndDataFromTuple(tuple, leftAttrs, "", leftAttrIndex, _, key);

            //reset rightIt
            rightIt->setIterator(key, key, true, true);
        }

        // left data is in tuple now, get right next tuple now
        rrc = rightIt->getNextTuple(tuple1);
        // get the tuple, concatenate them
        if (rrc != QE_EOF) {
            concatenateTuple(data, tuple, tuple1, leftAttrs, rightAttrs);
            // found, just break!
            break;
        }
    }

    free(key);
    free(tuple1);
    if (lrc == QE_EOF && rrc == QE_EOF) {
        free(tuple);
        return QE_EOF;
    }
    return 0;
}

void INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
    for (auto const & it : leftAttrs) {
        attrs.push_back(it);
    }

    for (auto const & it : rightAttrs) {
        attrs.push_back(it);
    }
}

/*
 * 1. do the partition, write into file, using rbfm
 * 2. using rbfm scan to read file, load all left tuple, then the right
 * 3. output
 *
 * */
GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned numPartitions) {
    rbfm = &RecordBasedFileManager::instance();

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);

    this->numPartitions = (int) numPartitions;

    std::string leftAttrName = condition.lhsAttr;
    std::string rightAttrName = condition.rhsAttr;

    leftAttrIndex = RecordBasedFileManager::getAttrIndex(leftAttrs, condition.lhsAttr);
    rightAttrIndex = RecordBasedFileManager::getAttrIndex(rightAttrs, condition.rhsAttr);

    for (auto const & it : leftAttrs) {
        leftAttrNames.push_back(it.name);
    }

    for (auto const & it : rightAttrs) {
        rightAttrNames.push_back(it.name);
    }

    leftAttr = leftAttrs[leftAttrIndex];
    rightAttr = rightAttrs[rightAttrIndex];

    attrType = leftAttr.type;

    nextpart = 0;
    lrc = 0;
    rrc = RBFM_EOF;

    tuple1 = malloc(PAGE_SIZE);

    /*
     * start partition in the following code
     * */
    // create file
    for (int i = 0; i < numPartitions; i++) {
        leftPartitionNames.push_back(getFileName(i, true, leftAttrName));
        rbfm->createFile(leftPartitionNames[i]);

        rightPartitionNames.push_back(getFileName(i, false, rightAttrName));
        rbfm->createFile(rightPartitionNames[i]);
    }

    // partition
    scanThenAddToPartitionFile(leftIn, leftPartitionNames, leftAttrs, leftAttrIndex);
    scanThenAddToPartitionFile(rightIn, rightPartitionNames, rightAttrs, rightAttrIndex);
}

GHJoin::~GHJoin() {
    free(tuple1);
    clean();
}

RC GHJoin::getNextTuple(void *data) {
    void *key = malloc(PAGE_SIZE);
    RID rid;
    bool foundRight = false;
    while (!foundRight && outBuffer.empty() && (nextpart != numPartitions || rrc != RBFM_EOF)) {
        // right end, scan left next & right next
        if (rrc == RBFM_EOF) {
            clean();
            RBFM_ScanIterator rbfmsi;
            FileHandle fileHandle;
            rbfm->openFile(getFileName(nextpart, true, leftAttr.name), fileHandle);
            rbfm->scan(fileHandle, leftAttrs, "", NO_OP, nullptr, leftAttrNames, rbfmsi);
            void *tuple = malloc(PAGE_SIZE);
            while (rbfmsi.getNextRecord(rid, tuple) != RBFM_EOF) {
                RecordBasedFileManager::readAttributeFromRawData(tuple, key, leftAttrs, "", leftAttrIndex);
                addTupleToHashMap(tuple, key, attrType, intMap, stringMap);
                tuple = malloc(PAGE_SIZE);
            }
            free(tuple);
            rbfmsi.close();

            // restart scan right
            rbfm->openFile(getFileName(nextpart, false, rightAttr.name), rightFileHandle);
            rbfm->scan(rightFileHandle, rightAttrs, "", NO_OP, nullptr, rightAttrNames, rightRBFMSI);
            rrc = 0;

            nextpart += 1;
        }

        // search in right part
        while (!foundRight) {
            RID rid1;
            rrc = rightRBFMSI.getNextRecord(rid1, tuple1);
            if  (rrc == RBFM_EOF) {
                rightRBFMSI.close();
                break;
            }

            RecordBasedFileManager::readAttributeFromRawData(tuple1, key, rightAttrs, "", rightAttrIndex);
            if (attrType != TypeVarChar) {
                int intKey;
                memcpy(&intKey, key, INT_SIZE);
                if (intMap.find(intKey) != intMap.end()) {
                    for (auto const & it : intMap[intKey]) {
                        outBuffer.push(it);
                    }
                    intMap.erase(intKey);
                    foundRight = true;
                }
            } else {
                unsigned stringLength;
                memcpy(&stringLength, key, UNSIGNED_SIZE);
                std::string stringKey ((char *) key + UNSIGNED_SIZE, stringLength);
                if (stringMap.find(stringKey) != stringMap.end()) {
                    for (auto const & it : stringMap[stringKey]) {
                        outBuffer.push(it);
                    }
                    stringMap.erase(stringKey);
                    foundRight = true;
                }
            }
        }
    }

    free(key);
    if (outBuffer.empty()) {
        clean();
        for (const auto & it : leftPartitionNames) {
            rbfm->destroyFile(it);
        }

        for (const auto & it : rightPartitionNames) {
            rbfm->destroyFile(it);
        }
        return QE_EOF;
    } else {
        void* tuple = outBuffer.top();
        outBuffer.pop();
        concatenateTuple(data, tuple, tuple1, leftAttrs, rightAttrs);
        free(tuple);
        return 0;
    }
}

void GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
    for (auto const & it : leftAttrs) {
        attrs.push_back(it);
    }

    for (auto const & it : rightAttrs) {
        attrs.push_back(it);
    }
}

std::string GHJoin::getFileName(int i, bool isLeft, std::string &attrName) {
    std::ostringstream stringStream;
    if (isLeft) {
        stringStream << "left_join";
    } else {
        stringStream << "right_join";
    }
    stringStream << i << "_" << attrName << ".pat";
    return stringStream.str();
}

void GHJoin::scanThenAddToPartitionFile(Iterator *iter, const std::vector<std::string> &partitionFileNames,
                                        const std::vector<Attribute> &attributes, int attrIndex) {

    void *tuple = malloc(PAGE_SIZE);

    std::hash<std::string> str_hash;

    RID rid;
    void *data = malloc(PAGE_SIZE);
    while (iter->getNextTuple(tuple) != RM_EOF) {
        RecordBasedFileManager::readAttributeFromRawData(tuple, data, attributes, "", attrIndex);
        int remainder;
        if (attrType != TypeVarChar) {
            int dataInt;
            memcpy(&dataInt, tuple, UNSIGNED_SIZE);
            if (dataInt < 0) {
                dataInt += 1;
                dataInt *= -1;
            }
            remainder = dataInt % numPartitions;
        } else {
            unsigned length;
            memcpy(&length, data, UNSIGNED_SIZE);
            std::string string ((char *) data + UNSIGNED_SIZE, length);
            int hashcode = str_hash(string);
            if (hashcode < 0) {
                hashcode += 1;
                hashcode *= -1;
            }
            remainder = hashcode % numPartitions;
        }
        FileHandle fileHandle;
        rbfm->openFile(partitionFileNames[remainder], fileHandle);
        rbfm->insertRecord(fileHandle, attributes, tuple, rid);
        rbfm->closeFile(fileHandle);
    }

    free(tuple);
    free(data);
}

void GHJoin::clean() {
    for (auto & it : intMap) {
        for (auto & it1 : it.second) {
            free(it1);
        }
    }
    intMap.clear();
    for (auto & it : stringMap) {
        for (auto & it1 : it.second) {
            free(it1);
        }
    }
    stringMap.clear();
}

Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
    this->aggAttr =aggAttr;
    this->op = op;
    this->input = input;
    this->maxValue = MIN_FLOAT;
    this->minValue = MAX_FLOAT;
    this->totalCount = 0;
    this->groupAttr = aggAttr;

    input->getAttributes(attributes);
    aggrIndex = RecordBasedFileManager::getAttrIndex(attributes, aggAttr.name);
}

RC Aggregate::getNextTuple(void *data) {

    if (groupAttr.name != aggAttr.name) {
        return getNextTupleGroupBy(data);
    }

    void *currentTuple = malloc(PAGE_SIZE);
    void* attrData = malloc(PAGE_SIZE);

    while (input->getNextTuple(currentTuple) != QE_EOF) {
        RecordBasedFileManager::readAttributeFromRawData(currentTuple, attrData, attributes, "", aggrIndex);

        float dataValue = 0;
        int intValue;

        if (aggAttr.type == TypeInt) {
            memcpy(&intValue, attrData, UNSIGNED_SIZE);
            dataValue += (float) intValue;
        } else {
            memcpy(&dataValue, attrData, UNSIGNED_SIZE);
        }

        totalCount++;
        minValue = dataValue < minValue ? dataValue : minValue;
        maxValue = dataValue > maxValue ? dataValue : maxValue;
        valueSum += dataValue;
        valueAvg = valueSum / totalCount;
    }

    free(currentTuple);
    free(attrData);

    int pos = 0;
    unsigned char nullIndicator = 0x00;
    memcpy((char *) data + pos, &nullIndicator, NULL_INDICATOR_UNIT_SIZE);
    pos += NULL_INDICATOR_UNIT_SIZE;

    switch (op) {
        case MAX:
            memcpy((char *) data + pos, &maxValue, UNSIGNED_SIZE);
            break;
        case MIN:
            memcpy((char *) data + pos, &minValue, UNSIGNED_SIZE);
            break;
        case COUNT:
            memcpy((char *) data + pos, &totalCount, UNSIGNED_SIZE);
            break;
        case SUM:
            memcpy((char *) data + pos, &valueSum, UNSIGNED_SIZE);
            break;
        case AVG:
            memcpy((char *) data + pos, &valueAvg, UNSIGNED_SIZE);
            break;
    }

    if (!endFlag) {
        endFlag = true;
        return 0;
    } else {
        return QE_EOF;
    }

}

//group by
Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {
    this->groupAttr = groupAttr;
    this->aggAttr =aggAttr;
    this->op = op;
    this->input = input;
    this->maxValue = MIN_FLOAT;
    this->minValue = MAX_FLOAT;
    this->totalCount = 0;

    input->getAttributes(attributes);
    aggrIndex = RecordBasedFileManager::getAttrIndex(attributes, aggAttr.name);
    groupIndex = RecordBasedFileManager::getAttrIndex(attributes, groupAttr.name);

    void *currentTuple = malloc(PAGE_SIZE);

    while (input->getNextTuple(currentTuple) != QE_EOF) {
        void *key = malloc(PAGE_SIZE);
        void *attrData = malloc(PAGE_SIZE);

        RecordBasedFileManager::readAttributeFromRawData(currentTuple, key, attributes, "", groupIndex);
        RecordBasedFileManager::readAttributeFromRawData(currentTuple, attrData, attributes, "", aggrIndex);

        std::string keyString;

        if (groupAttr.type == TypeInt) {
            int keyInt;
            memcpy(&keyInt, key, UNSIGNED_SIZE);
            keyString = std::to_string(keyInt);
        } else if (groupAttr.type == TypeReal) {
            float keyFloat;
            memcpy(&keyFloat, key, UNSIGNED_SIZE);
            keyString = std::to_string(keyFloat);
        } else {
            unsigned length;
            memcpy(&length, key, UNSIGNED_SIZE);
            keyString.assign((char *) key + UNSIGNED_SIZE, length);
        }

        float dataValue = 0;
        int intValue;

        if (aggAttr.type == TypeInt) {
            memcpy(&intValue, attrData, UNSIGNED_SIZE);
            dataValue += (float) intValue;
        } else {
            memcpy(&dataValue, attrData, UNSIGNED_SIZE);
        }

        free(key);
        free(attrData);

        totalCountMap[keyString] += 1;

        if (minMap.count(keyString) == 0) {
            minMap[keyString] = dataValue;
        } else {
            minMap[keyString] = std::min(minMap[keyString], dataValue);
        }

        if (maxMap.count(keyString) == 0) {
            maxMap[keyString] = dataValue;
        } else {
            maxMap[keyString] = std::max(maxMap[keyString], dataValue);
        }

        sumMap[keyString] += dataValue;
        avgMap[keyString] = sumMap[keyString] / totalCountMap[keyString];

    }

    for (const auto & it : totalCountMap) {
        std::vector<float> vector {minMap[it.first], maxMap[it.first], it.second, sumMap[it.first], avgMap[it.first]};
        aggregations.push_back(vector);
        groups.push_back(it.first);
    }

    free(currentTuple);
}

RC Aggregate::getNextTupleGroupBy(void *data) {
    if (outputIndex == groups.size())
        return QE_EOF;

    int pos = 0;

    unsigned char nullIndicator = 0x00;
    memcpy((char *) data + pos, &nullIndicator, NULL_INDICATOR_UNIT_SIZE);
    pos += NULL_INDICATOR_UNIT_SIZE;

    // stringKey back to original
    if (groupAttr.type == TypeInt) {
        int intKey = std::stoi(groups[outputIndex]);
        memcpy((char *) data + pos, &intKey, UNSIGNED_SIZE);
        pos += UNSIGNED_SIZE;
    } else if (groupAttr.type == TypeReal) {
        float floatKey = std::stof(groups[outputIndex]);
        memcpy((char *) data + pos, &floatKey, UNSIGNED_SIZE);
        pos += UNSIGNED_SIZE;
    } else {
        unsigned length = groups[outputIndex].size();
        memcpy((char *) data + pos, &length, UNSIGNED_SIZE);
        pos += UNSIGNED_SIZE;
        memcpy((char *) data + pos, groups[outputIndex].c_str(), length);
        pos += (int) length;
    }

    memcpy((char *) data + pos, &aggregations[outputIndex][op], UNSIGNED_SIZE);

    outputIndex += 1;
    return 0;
}

void Aggregate::getAttributes(std::vector<Attribute> &attrs) const {

    std::string operatorString;
    switch (op) {
        case MIN:
            operatorString = "MIN";
            break;
        case MAX:
            operatorString = "MAX";
            break;
        case COUNT:
            operatorString = "COUNT";
            break;
        case SUM:
            operatorString = "SUM";
            break;
        case AVG:
            operatorString = "AVG";
            break;
    }

    Attribute tmpAttr = aggAttr;
    std::string tmp = operatorString;
    tmp += "(";
    tmp += tmpAttr.name;
    tmp += ")";

    tmpAttr.name = tmp;
    attrs.push_back(tmpAttr);

}

