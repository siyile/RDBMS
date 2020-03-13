
#include "qe.h"


/*
condition leftattr ->attrName->indexfile
        index scan until satisfy condition
        then get next tuple
*/


// ... the rest of your implementations go here

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

                // add to hashMap
                void *tmpData = tuple;
                if (leftAttr.type == TypeInt) {
                    int intKey;
                    memcpy(&intKey, key, INT_SIZE);
                    if (intMap.find(intKey) == intMap.end()) {
                        std::vector<void *> vector;
                        intMap[intKey] = vector;
                    }
                    intMap[intKey].push_back(tmpData);
                } else if (leftAttr.type == TypeReal) {
                    float floatKey;
                    memcpy(&floatKey, key, INT_SIZE);
                    if (realMap.find(floatKey) == realMap.end()) {
                        std::vector<void *> vector;
                        realMap[floatKey] = vector;
                    }
                    realMap[floatKey].push_back(tmpData);
                } else {
                    unsigned stringLength;
                    memcpy(&stringLength, key, UNSIGNED_SIZE);
                    std::string stringKey((char *) key + UNSIGNED_SIZE, stringLength);
                    if (stringMap.find(stringKey) == stringMap.end()) {
                        std::vector<void *> vector;
                        stringMap[stringKey] = vector;
                    }
                    stringMap[stringKey].push_back(tmpData);
                }
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
            if (leftAttr.type == TypeInt) {
                int intKey;
                memcpy(&intKey, key, INT_SIZE);
                if (intMap.find(intKey) != intMap.end()) {
                    for (auto const & it : intMap[intKey]) {
                        outBuffer.push(it);
                    }
                    foundRight = true;
                }
            } else if (leftAttr.type == TypeReal) {
                float floatKey;
                memcpy(&floatKey, key, INT_SIZE);
                if (realMap.find(floatKey) != realMap.end()) {
                    for (auto const & it : realMap[floatKey]) {
                        outBuffer.push(it);
                    }
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
    intMap.clear();
    realMap.clear();
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

    leftAttrsIndex = RecordBasedFileManager::getAttrIndex(leftAttrs, condition.lhsAttr);
    rightAttrsIndex = RecordBasedFileManager::getAttrIndex(rightAttrs, condition.rhsAttr);

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
            getLengthAndDataFromTuple(tuple, leftAttrs, "", leftAttrsIndex, _, key);

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

Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
    this->aggAttr =aggAttr;
    this->op = op;
    this->input = input;
    this->maxValue = MIN_FLOAT;
    this->minValue = MAX_FLOAT;
    this->totalCount = 0;
    this->groupAttr = aggAttr;
    this->aggAttrVector.push_back(aggAttr.name);
    this->proj = new Project(input, aggAttrVector);
}

RC Aggregate::getNextTuple(void *data) {

    if(groupAttr.name != aggAttr.name) {
        return getNextTupleGroupBy(data);
    }

    int rc = proj->getNextTuple(currentTuple);

    if (rc == QE_EOF) {
        return QE_EOF;
    }

    float dataValue;
    memcpy(&dataValue, currentTuple, UNSIGNED_SIZE);

    totalCount++;
    minValue = dataValue < minValue ? dataValue : minValue;
    maxValue = dataValue > maxValue ? dataValue : maxValue;
    valueSum += dataValue;
    valueAvg = valueSum / totalCount;

    switch (op) {
        case MAX:
            memcpy(data, &maxValue, UNSIGNED_SIZE);
            break;
        case MIN:
            memcpy(data, &minValue, UNSIGNED_SIZE);
            break;
        case COUNT:
            memcpy(data, &totalCount, UNSIGNED_SIZE);
            break;
        case SUM:
            memcpy(data, &valueSum, UNSIGNED_SIZE);
            break;
        case AVG:
            memcpy(data, &valueAvg, UNSIGNED_SIZE);
            break;
    }

    return 0;
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
    this->aggAttrVector.push_back(aggAttr.name);
    this->aggAttrVector.push_back(groupAttr.name);
    this->proj = new Project(input, aggAttrVector);
}

RC Aggregate::getNextTupleGroupBy(void *data) {

    int rc = proj->getNextTuple(currentTuple);

    if (rc == QE_EOF) {
        return QE_EOF;
    }

    std::string currentGroupByVarCharValue;
    unsigned currentGroupByVarCharValueLength;
    unsigned currentGroupByIntValue;
    unsigned currentGroupByRealValue;

    if (groupAttr.type == TypeVarChar) {
        std::string groupByValue;
        unsigned length;
        memcpy(&length, currentTuple, UNSIGNED_SIZE);
        memcpy(&groupByValue, (char*) currentTuple + UNSIGNED_SIZE, length);
        float dataValue;
        memcpy(&dataValue, (char*) currentTuple + UNSIGNED_SIZE + length, UNSIGNED_SIZE);
        currentGroupByVarCharValue = groupByValue;
        currentGroupByVarCharValueLength = length;

        if(groupByVarCharAttrValue.find(groupByValue) == groupByVarCharAttrValue.end()) {
            groupByVarCharAttrValue.insert(groupByValue);
            groupByVarCharAttrTotalCount[groupByValue] = 1;
            groupByVarCharAttrMinValue[groupByValue] = dataValue;
            groupByVarCharAttrMaxValue[groupByValue] = dataValue;
            groupByVarCharAttrValueSum[groupByValue] = dataValue;
            groupByVarCharAttrValueAvg[groupByValue] = dataValue;
        } else{
            groupByVarCharAttrTotalCount[groupByValue]++;
            groupByVarCharAttrMinValue[groupByValue] =
                        dataValue < groupByVarCharAttrMinValue[groupByValue] ? dataValue
                                                                             : groupByVarCharAttrMinValue[groupByValue];
            groupByVarCharAttrMaxValue[groupByValue] =
                        dataValue > groupByVarCharAttrMaxValue[groupByValue] ? dataValue
                                                                             : groupByVarCharAttrMaxValue[groupByValue];
            groupByVarCharAttrValueSum[groupByValue] += dataValue;
            groupByVarCharAttrValueAvg[groupByValue] = groupByVarCharAttrValueSum[groupByValue] / groupByVarCharAttrTotalCount[groupByValue];
        }
    } else if (groupAttr.type == TypeInt) {
        unsigned groupByValue;
        memcpy(&groupByValue, currentTuple, UNSIGNED_SIZE);
        float dataValue;
        memcpy(&dataValue, (char*) currentTuple + UNSIGNED_SIZE, UNSIGNED_SIZE);
        currentGroupByIntValue = groupByValue;

        if(groupByIntAttrValue.find(groupByValue) == groupByIntAttrValue.end()) {
            groupByIntAttrValue.insert(groupByValue);
            groupByIntAttrTotalCount[groupByValue] = 1;
            groupByIntAttrMinValue[groupByValue] = dataValue;
            groupByIntAttrMaxValue[groupByValue] = dataValue;
            groupByIntAttrValueSum[groupByValue] = dataValue;
            groupByIntAttrValueAvg[groupByValue] = dataValue;
        } else {
            groupByIntAttrTotalCount[groupByValue]++;
            groupByIntAttrMinValue[groupByValue] = dataValue < groupByIntAttrMinValue[groupByValue] ? dataValue
                                                                                                    : groupByIntAttrMinValue[groupByValue];
            groupByIntAttrMaxValue[groupByValue] = dataValue > groupByIntAttrMaxValue[groupByValue] ? dataValue
                                                                                                    : groupByIntAttrMaxValue[groupByValue];
            groupByIntAttrValueSum[groupByValue] += dataValue;
            groupByIntAttrValueAvg[groupByValue] =
                    groupByIntAttrValueSum[groupByValue] / groupByIntAttrTotalCount[groupByValue];
        }
    } else {
        unsigned groupByValue;
        memcpy(&groupByValue, currentTuple, UNSIGNED_SIZE);
        float dataValue;
        memcpy(&dataValue, (char*) currentTuple + UNSIGNED_SIZE, UNSIGNED_SIZE);
        currentGroupByRealValue = groupByValue;
        if(groupByRealAttrValue.find(groupByValue) == groupByRealAttrValue.end()) {
            groupByRealAttrValue.insert(groupByValue);
            groupByRealAttrTotalCount[groupByValue] = 1;
            groupByRealAttrMinValue[groupByValue] = dataValue;
            groupByRealAttrMaxValue[groupByValue] = dataValue;
            groupByRealAttrValueSum[groupByValue] = dataValue;
            groupByRealAttrValueAvg[groupByValue] = dataValue;
        } else {
            groupByRealAttrTotalCount[groupByValue]++;
            groupByRealAttrMinValue[groupByValue] =
                    dataValue < groupByRealAttrMinValue[groupByValue] ? dataValue
                                                                      : groupByRealAttrMinValue[groupByValue];
            groupByRealAttrMaxValue[groupByValue] =
                    dataValue > groupByRealAttrMaxValue[groupByValue] ? dataValue
                                                                      : groupByRealAttrMaxValue[groupByValue];
            groupByRealAttrValueSum[groupByValue] += dataValue;
            groupByRealAttrValueAvg[groupByValue] =
                    groupByRealAttrValueSum[groupByValue] / groupByRealAttrTotalCount[groupByValue];
        }
    }

    unsigned pos = UNSIGNED_SIZE;

    switch (op) {
        case MAX:
            if (groupAttr.type == TypeVarChar) {
                memcpy(data, &currentGroupByVarCharValueLength, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &currentGroupByVarCharValue, currentGroupByVarCharValueLength);
                pos += currentGroupByVarCharValueLength;
                memcpy((char*)data + pos, &groupByVarCharAttrMaxValue[currentGroupByVarCharValue], UNSIGNED_SIZE);
            } else if (groupAttr.type == TypeInt) {
                memcpy(data, &currentGroupByIntValue, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &groupByIntAttrMaxValue[currentGroupByIntValue], UNSIGNED_SIZE);
            } else {
                memcpy(data, &currentGroupByRealValue, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &groupByRealAttrMaxValue[currentGroupByRealValue], UNSIGNED_SIZE);
            }
            break;
        case MIN:
            if (groupAttr.type == TypeVarChar) {
                memcpy(data, &currentGroupByVarCharValueLength, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &currentGroupByVarCharValue, currentGroupByVarCharValueLength);
                pos += currentGroupByVarCharValueLength;
                memcpy((char*)data + pos, &groupByVarCharAttrMinValue[currentGroupByVarCharValue], UNSIGNED_SIZE);
            } else if (groupAttr.type == TypeInt) {
                memcpy(data, &currentGroupByIntValue, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &groupByIntAttrMinValue[currentGroupByIntValue], UNSIGNED_SIZE);
            } else {
                memcpy(data, &currentGroupByRealValue, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &groupByRealAttrMinValue[currentGroupByRealValue], UNSIGNED_SIZE);
            }
            break;
        case COUNT:
            if (groupAttr.type == TypeVarChar) {
                memcpy(data, &currentGroupByVarCharValueLength, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &currentGroupByVarCharValue, currentGroupByVarCharValueLength);
                pos += currentGroupByVarCharValueLength;
                memcpy((char*)data + pos, &groupByVarCharAttrTotalCount[currentGroupByVarCharValue], UNSIGNED_SIZE);
            } else if (groupAttr.type == TypeInt) {
                memcpy(data, &currentGroupByIntValue, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &groupByIntAttrTotalCount[currentGroupByIntValue], UNSIGNED_SIZE);
            } else {
                memcpy(data, &currentGroupByRealValue, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &groupByRealAttrTotalCount[currentGroupByRealValue], UNSIGNED_SIZE);
            }
            break;
        case SUM:
            if (groupAttr.type == TypeVarChar) {
                memcpy(data, &currentGroupByVarCharValueLength, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &currentGroupByVarCharValue, currentGroupByVarCharValueLength);
                pos += currentGroupByVarCharValueLength;
                memcpy((char*)data + pos, &groupByVarCharAttrValueSum[currentGroupByVarCharValue], UNSIGNED_SIZE);
            } else if (groupAttr.type == TypeInt) {
                memcpy(data, &currentGroupByIntValue, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &groupByIntAttrValueSum[currentGroupByIntValue], UNSIGNED_SIZE);
            } else {
                memcpy(data, &currentGroupByRealValue, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &groupByRealAttrValueSum[currentGroupByRealValue], UNSIGNED_SIZE);
            }
            break;
        case AVG:
            if (groupAttr.type == TypeVarChar) {
                memcpy(data, &currentGroupByVarCharValueLength, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &currentGroupByVarCharValue, currentGroupByVarCharValueLength);
                pos += currentGroupByVarCharValueLength;
                memcpy((char*)data + pos, &groupByVarCharAttrValueAvg[currentGroupByVarCharValue], UNSIGNED_SIZE);
            } else if (groupAttr.type == TypeInt) {
                memcpy(data, &currentGroupByIntValue, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &groupByIntAttrValueAvg[currentGroupByIntValue], UNSIGNED_SIZE);
            } else {
                memcpy(data, &currentGroupByRealValue, UNSIGNED_SIZE);
                memcpy((char*)data + pos, &groupByRealAttrValueAvg[currentGroupByRealValue], UNSIGNED_SIZE);
            }
            break;
    }


//    while(proj->getNextTuple(currentTuple) != QE_EOF) {
//
//        if (groupAttr.type == TypeVarChar) {
//            std::string groupByValue;
//            unsigned length;
//            memcpy(&length, currentTuple, UNSIGNED_SIZE);
//            memcpy(&groupByValue, (char *) currentTuple + UNSIGNED_SIZE, length);
//            float dataValue;
//            memcpy(&dataValue, (char *) currentTuple + UNSIGNED_SIZE + length, UNSIGNED_SIZE);
//
//            if (groupByVarCharAttrValue.find(groupByValue) == groupByVarCharAttrValue.end()) {
//                groupByVarCharAttrValue.insert(groupByValue);
//                currentGroupByVarCharValue = groupByValue;
//                currentGroupByVarCharValueLength = length;
//
//                groupByVarCharAttrTotalCount[groupByValue] = 1;
//                groupByVarCharAttrMinValue[groupByValue] = dataValue;
//                groupByVarCharAttrMaxValue[groupByValue] = dataValue;
//                groupByVarCharAttrValueSum[groupByValue] = dataValue;
//                groupByVarCharAttrValueAvg[groupByValue] = dataValue;
//
//                break;
//            }
//        } else if (groupAttr.type == TypeInt) {
//            unsigned groupByValue;
//            memcpy(&groupByValue, currentTuple, UNSIGNED_SIZE);
//            float dataValue;
//            memcpy(&dataValue, (char *) currentTuple + UNSIGNED_SIZE, UNSIGNED_SIZE);
//            if (groupByIntAttrValue.find(groupByValue) == groupByIntAttrValue.end()) {
//                groupByIntAttrValue.insert(groupByValue);
//                currentGroupByIntValue = groupByValue;
//
//                groupByIntAttrTotalCount[groupByValue] = 1;
//                groupByIntAttrMinValue[groupByValue] = dataValue;
//                groupByIntAttrMaxValue[groupByValue] = dataValue;
//                groupByIntAttrValueSum[groupByValue] = dataValue;
//                groupByIntAttrValueAvg[groupByValue] = dataValue;
//
//                break;
//            }
//
//        } else {
//            unsigned groupByValue;
//            memcpy(&groupByValue, currentTuple, UNSIGNED_SIZE);
//            float dataValue;
//            memcpy(&dataValue, (char *) currentTuple + UNSIGNED_SIZE, UNSIGNED_SIZE);
//            if (groupByRealAttrValue.find(groupByValue) == groupByRealAttrValue.end()) {
//                groupByRealAttrValue.insert(groupByValue);
//                currentGroupByRealValue = groupByValue;
//                groupByRealAttrTotalCount[groupByValue] = 1;
//                groupByRealAttrMinValue[groupByValue] = dataValue;
//                groupByRealAttrMaxValue[groupByValue] = dataValue;
//                groupByRealAttrValueSum[groupByValue] = dataValue;
//                groupByRealAttrValueAvg[groupByValue] = dataValue;
//            }
//        }
//    }
//
//        while(proj->getNextTuple(currentTuple) != QE_EOF) {
//
//            if (groupAttr.type == TypeVarChar) {
//                std::string groupByValue;
//                unsigned length;
//                memcpy(&length, currentTuple, UNSIGNED_SIZE);
//                memcpy(&groupByValue, (char *) currentTuple + UNSIGNED_SIZE, length);
//                float dataValue;
//                memcpy(&dataValue, (char *) currentTuple + UNSIGNED_SIZE + length, UNSIGNED_SIZE);
//
//                if (groupByValue == currentGroupByVarCharValue) {
//                    groupByVarCharAttrTotalCount[groupByValue]++;
//                    groupByVarCharAttrMinValue[groupByValue] =
//                            dataValue < groupByVarCharAttrMinValue[groupByValue] ? dataValue
//                                                                                 : groupByVarCharAttrMinValue[groupByValue];
//                    groupByVarCharAttrMaxValue[groupByValue] =
//                            dataValue > groupByVarCharAttrMaxValue[groupByValue] ? dataValue
//                                                                                 : groupByVarCharAttrMaxValue[groupByValue];
//                    groupByVarCharAttrValueSum[groupByValue] += dataValue;
//                    groupByVarCharAttrValueAvg[groupByValue] =
//                            groupByVarCharAttrValueSum[groupByValue] / groupByVarCharAttrTotalCount[groupByValue];
//
//                }
//            } else if (groupAttr.type == TypeInt) {
//                unsigned groupByValue;
//                memcpy(&groupByValue, currentTuple, UNSIGNED_SIZE);
//                float dataValue;
//                memcpy(&dataValue, (char *) currentTuple + UNSIGNED_SIZE, UNSIGNED_SIZE);
//
//                if (groupByValue == currentGroupByIntValue) {
//                    groupByIntAttrTotalCount[groupByValue]++;
//                    groupByIntAttrMinValue[groupByValue] = dataValue < groupByIntAttrMinValue[groupByValue] ? dataValue
//                                                                                                            : groupByIntAttrMinValue[groupByValue];
//                    groupByIntAttrMaxValue[groupByValue] = dataValue > groupByIntAttrMaxValue[groupByValue] ? dataValue
//                                                                                                            : groupByIntAttrMaxValue[groupByValue];
//                    groupByIntAttrValueSum[groupByValue] += dataValue;
//                    groupByIntAttrValueAvg[groupByValue] =
//                            groupByIntAttrValueSum[groupByValue] / groupByIntAttrTotalCount[groupByValue];
//                }
//            } else {
//                unsigned groupByValue;
//                memcpy(&groupByValue, currentTuple, UNSIGNED_SIZE);
//                float dataValue;
//                memcpy(&dataValue, (char *) currentTuple + UNSIGNED_SIZE, UNSIGNED_SIZE);
//
//                if (groupByValue == currentGroupByRealValue) {
//                    groupByRealAttrTotalCount[groupByValue]++;
//                    groupByRealAttrMinValue[groupByValue] =
//                            dataValue < groupByRealAttrMinValue[groupByValue] ? dataValue
//                                                                              : groupByRealAttrMinValue[groupByValue];
//                    groupByRealAttrMaxValue[groupByValue] =
//                            dataValue > groupByRealAttrMaxValue[groupByValue] ? dataValue
//                                                                              : groupByRealAttrMaxValue[groupByValue];
//                    groupByRealAttrValueSum[groupByValue] += dataValue;
//                    groupByRealAttrValueAvg[groupByValue] =
//                            groupByRealAttrValueSum[groupByValue] / groupByRealAttrTotalCount[groupByValue];
//                }
//            }
//        }

//        unsigned pos = UNSIGNED_SIZE;
//
//        switch (op) {
//            case MAX:
//                if (groupAttr.type == TypeVarChar) {
//                    memcpy(data, &currentGroupByVarCharValueLength, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &currentGroupByVarCharValue, currentGroupByVarCharValueLength);
//                    pos += currentGroupByVarCharValueLength;
//                    memcpy((char*)data + pos, &groupByVarCharAttrMaxValue[currentGroupByVarCharValue], UNSIGNED_SIZE);
//                } else if (groupAttr.type == TypeInt) {
//                    memcpy(data, &currentGroupByIntValue, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &groupByIntAttrMaxValue[currentGroupByIntValue], UNSIGNED_SIZE);
//                } else {
//                    memcpy(data, &currentGroupByRealValue, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &groupByRealAttrMaxValue[currentGroupByRealValue], UNSIGNED_SIZE);
//                }
//                break;
//            case MIN:
//                if (groupAttr.type == TypeVarChar) {
//                    memcpy(data, &currentGroupByVarCharValueLength, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &currentGroupByVarCharValue, currentGroupByVarCharValueLength);
//                    pos += currentGroupByVarCharValueLength;
//                    memcpy((char*)data + pos, &groupByVarCharAttrMinValue[currentGroupByVarCharValue], UNSIGNED_SIZE);
//                } else if (groupAttr.type == TypeInt) {
//                    memcpy(data, &currentGroupByIntValue, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &groupByIntAttrMinValue[currentGroupByIntValue], UNSIGNED_SIZE);
//                } else {
//                    memcpy(data, &currentGroupByRealValue, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &groupByRealAttrMinValue[currentGroupByRealValue], UNSIGNED_SIZE);
//                }
//                break;
//            case COUNT:
//                if (groupAttr.type == TypeVarChar) {
//                    memcpy(data, &currentGroupByVarCharValueLength, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &currentGroupByVarCharValue, currentGroupByVarCharValueLength);
//                    pos += currentGroupByVarCharValueLength;
//                    memcpy((char*)data + pos, &groupByVarCharAttrTotalCount[currentGroupByVarCharValue], UNSIGNED_SIZE);
//                } else if (groupAttr.type == TypeInt) {
//                    memcpy(data, &currentGroupByIntValue, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &groupByIntAttrTotalCount[currentGroupByIntValue], UNSIGNED_SIZE);
//                } else {
//                    memcpy(data, &currentGroupByRealValue, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &groupByRealAttrTotalCount[currentGroupByRealValue], UNSIGNED_SIZE);
//                }
//                break;
//            case SUM:
//                if (groupAttr.type == TypeVarChar) {
//                    memcpy(data, &currentGroupByVarCharValueLength, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &currentGroupByVarCharValue, currentGroupByVarCharValueLength);
//                    pos += currentGroupByVarCharValueLength;
//                    memcpy((char*)data + pos, &groupByVarCharAttrValueSum[currentGroupByVarCharValue], UNSIGNED_SIZE);
//                } else if (groupAttr.type == TypeInt) {
//                    memcpy(data, &currentGroupByIntValue, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &groupByIntAttrValueSum[currentGroupByIntValue], UNSIGNED_SIZE);
//                } else {
//                    memcpy(data, &currentGroupByRealValue, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &groupByRealAttrValueSum[currentGroupByRealValue], UNSIGNED_SIZE);
//                }
//                break;
//            case AVG:
//                if (groupAttr.type == TypeVarChar) {
//                    memcpy(data, &currentGroupByVarCharValueLength, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &currentGroupByVarCharValue, currentGroupByVarCharValueLength);
//                    pos += currentGroupByVarCharValueLength;
//                    memcpy((char*)data + pos, &groupByVarCharAttrValueAvg[currentGroupByVarCharValue], UNSIGNED_SIZE);
//                } else if (groupAttr.type == TypeInt) {
//                    memcpy(data, &currentGroupByIntValue, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &groupByIntAttrValueAvg[currentGroupByIntValue], UNSIGNED_SIZE);
//                } else {
//                    memcpy(data, &currentGroupByRealValue, UNSIGNED_SIZE);
//                    memcpy((char*)data + pos, &groupByRealAttrValueAvg[currentGroupByRealValue], UNSIGNED_SIZE);
//                }
//                break;
//        }
//    }
//
//    free(currentTuple);
    return 0;
}

void Aggregate::getAttributes(std::vector<Attribute> &attrs) const {

    Attribute targetAttr = this->aggAttr;

    std::string targetAttrName;
    targetAttrName = op + "(" + targetAttr.name + ")";
    targetAttr.name = targetAttrName;

    attrs.push_back(targetAttr);

}

