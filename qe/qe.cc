
#include "qe.h"

Filter::Filter(Iterator *input, const Condition &condition) {
}

// ... the rest of your implementations go here

/*
 * 1. read tuple from leftIt, till the memory limit
 *      - hash the tuple by key : data
 * 2. start table scan rightIt, scan all the tuple
 *      - if there is data in hash map, connect them
 *
 *
 *
 * */
BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned numPages) {
    memoryLimit = (numPages - 2) * PAGE_SIZE;
    if (!condition.bRhsIsAttr) {
        throw std::logic_error("check right hand Attr");
    }
    this->condition = condition;
    this->leftIt = leftIn;
    this->rightIt = rightIn;


}

RC BNLJoin::getNextTuple(void *data) {

    return 0;
}
