
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

    //get tableName from relation
    //input->getTableNameFromRelAttr(this->tableName, relAttrs);

    for (const Attribute& attr : relAttrs) {
        if (attr.name == targetAttrName) {
            this->targetAttribute = attr;
            break;
        }
    }
}

RC Filter::getNextTuple(void *data) {
    if (input->getNextTuple(currentTuple) == QE_EOF) {
        return QE_EOF;
    }

    while (!isTupleSatisfied()) {
        getNextTuple(data);
    }

    memcpy(data, currentTuple, Iterator::getTupleLength(relAttrs, currentTuple));

    return 0;
}

bool Filter::isTupleSatisfied() {

    unsigned short size = relAttrs.size();
    unsigned short pos = 0;

    int *attrsExist = new int[size];

    RecordBasedFileManager::getAttrExistArray(pos, attrsExist, currentTuple, size, false);

    // get data from tuple
    void* data = malloc(PAGE_SIZE);
    unsigned short _;
    getLengthAndDataFromTuple(currentTuple, relAttrs, "", targetAttrIndex, _, data);

    bool res = RecordBasedFileManager::compareValue(rhsValue.data, data, op, targetAttribute.type);

    return res;
}

void Filter::getAttributes(std::vector<Attribute> &attrs) const {
    for (auto const & it : relAttrs) {
        auto attr = it;
        attr.name = "filter." + it.name;
        attrs.push_back(attr);
    }
}


Project::Project(Iterator *input, const std::vector<std::string> &attrNames) {
    input->getAttributes(this->relAttrs);

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
    //this->targetAttributesNames = attrNames;
    this->targetAttributesNames.insert(targetAttributesNames.begin(), attrNames.begin(), attrNames.end());

    for (int i = 0; i < attrNames.size(); i++) {
        int tupleIndex = targetIndexToTupleIndexMap[i];
        targetAttributes.push_back(relAttrs[tupleIndex]);
    }
}

RC Project::getNextTuple(void *data) {

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
            offset = tupleIndexToOffsetMap[i];
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
    memcpy(data,nullIndicator, nullIndicatorSize);
    return 0;
}

void Project::getAttributes(std::vector<Attribute> &attrs) const {
    for (auto const & it : targetAttributes) {
        auto attr = it;
        attr.name = "project." + it.name;
        attrs.push_back(attr);
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

    int nullIndicatorSize = (lSize + rSize + 7) / 8;

    // copy left null indicator
    memcpy(data, left, lSize);

    // continue copy indicator
    unsigned char byte;
    int l = lSize;

    for (int r = 0; r < rSize; r++, l++) {
        RecordBasedFileManager::setNullIndicator(data, l, RecordBasedFileManager::getNullIndicator(right, r));
    }

    unsigned pos = nullIndicatorSize;
    unsigned leftLength = getTupleLength(leftAttrs, left) - (lSize + 7) / 8;
    memcpy((char *) data + pos, (char *) left + lSize / 8, leftLength);
    pos += leftLength;
    unsigned rightLength = getTupleLength(rightAttrs, right) - (rSize + 7) / 8;
    memcpy((char *) data + pos, (char *) right + rSize / 8, rightLength);
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
    while (outBuffer.empty() && lrc != QE_EOF && rrc != QE_EOF) {
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

        bool foundRight = false;
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
        auto attr = it;
        attr.name = "join." + it.name;
        attrs.push_back(attr);
    }

    for (auto const & it : rightAttrs) {
        auto attr = it;
        attr.name = "join." + it.name;
        attrs.push_back(attr);
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
    RecordBasedFileManager::getAttrExistArray(length, attrsExist, data, attrs.size(), false);
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
    while (lrc != QE_EOF && rrc != QE_EOF) {
        // if rightIt is over, leftIt get next, restart rightIt scan
        if (rrc == QE_EOF) {
            // leftIt get Next
            unsigned short _;
            lrc = leftIt->getNextTuple(tuple);
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
        auto attr = it;
        attr.name = "join." + it.name;
        attrs.push_back(attr);
    }

    for (auto const & it : rightAttrs) {
        auto attr = it;
        attr.name = "join." + it.name;
        attrs.push_back(attr);
    }
}

Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {
    this->aggAttr =aggAttr;
    this->op = op;
    this->input = input;
    this->maxValue = MIN_FLOAT;
    this->minValue = MAX_FLOAT;
    this->totalCount = 0;
    this->aggAttrVector.push_back(aggAttr.name);
}

RC Aggregate::getNextTuple(void *data) {

    Project(input, aggAttrVector);

    while(proj->getNextTuple(data) != QE_EOF) {
        float dataValue;
        memcpy(&dataValue, data, UNSIGNED_SIZE);

        totalCount++;
        minValue = dataValue < minValue ? dataValue : minValue;
        maxValue = dataValue > maxValue ? dataValue : maxValue;
        valueSum += dataValue;
        valueAvg = valueSum / totalCount;
    }
    return 0;
}

void Aggregate::getAttributes(std::vector<Attribute> &attrs) const {

    Attribute targetAttr = this->aggAttr;

    std::string targetAttrName;
    targetAttrName = op + "(" + targetAttr.name + ")";
    targetAttr.name = targetAttrName;

    attrs.push_back(targetAttr);

}

