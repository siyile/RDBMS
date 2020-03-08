
#include "qe.h"

Filter::Filter(Iterator *input, const Condition &condition) {
}

// ... the rest of your implementations go here

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

int Iterator::getAttrIndex(std::vector<Attribute> attrs, const std::string& attrName) {
    for (int i = 0; i < attrs.size(); i++) {
        if (attrs[i].name == attrName) {
            return i;
        }
    }
    return -1;
}

void Iterator::concatenateTuple(void *data, void *left, void *right, std::vector<Attribute> const &leftAttrs,
                                std::vector<Attribute> const &rightAttrs) {
    int lSize = leftAttrs.size();
    int rSize = rightAttrs.size();

    int nullIndicatorSize = (lSize + rSize) / 8;

    // copy left null indicator
    memcpy(data, left, lSize);

    // continue copy indicator
    unsigned char byte;
    int l = lSize;

    for (int r = 0; r < rSize; r++, l++) {
        RecordBasedFileManager::setNullIndicator(data, l, RecordBasedFileManager::getNullIndicator(right, r));
    }

    unsigned pos = nullIndicatorSize;
    unsigned leftLength = getTupleLength(leftAttrs, left) - lSize / 8;
    memcpy((char *) data + pos, (char *) left + lSize / 8, leftLength);
    pos += leftLength;
    unsigned rightLength = getTupleLength(rightAttrs, right) - rSize / 8;
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
    leftAttrsIndex = getAttrIndex(leftAttrs, condition.lhsAttr);
    rightAttrsIndex = getAttrIndex(rightAttrs, condition.rhsAttr);

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
}

INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {
    leftIt = leftIn;
    rightIt = rightIn;
    this->condition = condition;

    leftIn->getAttributes(leftAttrs);
    rightIn->getAttributes(rightAttrs);

    lrc = 0;
    rrc = QE_EOF;

    leftAttrsIndex = getAttrIndex(leftAttrs, condition.lhsAttr);
    rightAttrsIndex = getAttrIndex(rightAttrs, condition.rhsAttr);

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