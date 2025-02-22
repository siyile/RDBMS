#include "pfm.h"
#include "test_util.h"

int RBFTest_4(PagedFileManager &pfm) {
    // Functions Tested:
    // 1. Open File
    // 2. Append Page
    // 3. Get Number Of Pages
    // 4. Get Counter Values
    // 5. Close File
    std::cout << std::endl << "***** In RBF Test Case 04 *****" << std::endl;

    RC rc;
    std::string fileName = "test3";

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCount1 = 0;
    unsigned writePageCount1 = 0;
    unsigned appendPageCount1 = 0;

    // Open the file "test3"
    FileHandle fileHandle;
    rc = pfm.openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    std::cout << "Start" << std::endl;

    // Collect before counters
    rc = fileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    if (rc != success) {
        std::cout << "[FAIL] collectCounterValues() failed. Test Case 4 failed." << std::endl;
        pfm.closeFile(fileHandle);
        return -1;
    }

    // Append the first page
    void *data = malloc(PAGE_SIZE);
    for (unsigned i = 0; i < PAGE_SIZE; i++) {
        *((char *) data + i) = i % 96 + 30;
    }
    rc = fileHandle.appendPage(data);
    assert(rc == success && "Appending a page should not fail.");

    // collect after counters
    rc = fileHandle.collectCounterValues(readPageCount1, writePageCount1, appendPageCount1);
    if (rc != success) {
        std::cout << "[FAIL] collectCounterValues() failed. Test Case 4 failed." << std::endl;
        pfm.closeFile(fileHandle);
        return -1;
    }
    std::cout << "before:R W A - " << readPageCount << " " << writePageCount << " " << appendPageCount
              << " after:R W A - "
              << readPageCount1 << " " << writePageCount1 << " " << appendPageCount1 << std::endl;
    assert(appendPageCount1 > appendPageCount && "The appendPageCount should have been increased.");

    // Get the number of pages
    unsigned count = fileHandle.getNumberOfPages();
    assert(count == (unsigned) 1 && "The count should be one at this moment.");

    // Close the file "test3"
    rc = pfm.closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");

    free(data);

    std::cout << "RBF Test Case 04 Finished! The result will be examined." << std::endl << std::endl;

    return 0;
}

int main() {
    // To test the functionality of the paged file manager
    PagedFileManager &pfm = PagedFileManager::instance();

    return RBFTest_4(pfm);
}
