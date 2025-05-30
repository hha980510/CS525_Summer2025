
char *testName;

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "storage_mgr.h"
#include "dberror.h"
#include "test_helper.h"

#define TESTPF "test_pagefile_extra.bin"

static void testAppendAndEnsureCapacity() {
    SM_FileHandle fh;
    SM_PageHandle ph = (SM_PageHandle) malloc(PAGE_SIZE);

    testName = "test appendEmptyBlock and ensureCapacity";

    // Create and open a new page file
    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));

    // Check initial state
    ASSERT_EQUALS_INT(1, fh.totalNumPages, "Initial page count should be 1");

    // Append two empty pages
    TEST_CHECK(appendEmptyBlock(&fh));
    TEST_CHECK(appendEmptyBlock(&fh));
    ASSERT_EQUALS_INT(3, fh.totalNumPages, "Page count should be 3 after appending 2 blocks");

    // Ensure capacity to 5 pages
    TEST_CHECK(ensureCapacity(5, &fh));
    ASSERT_EQUALS_INT(5, fh.totalNumPages, "Page count should be 5 after ensureCapacity");

    // Try ensureCapacity to a smaller value
    TEST_CHECK(ensureCapacity(3, &fh));
    ASSERT_EQUALS_INT(5, fh.totalNumPages, "Page count should remain 5 after ensureCapacity(3)");

    // Cleanup
    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(destroyPageFile(TESTPF));

    free(ph);
    TEST_DONE();
}

int main(void) {
    initStorageManager();
    testAppendAndEnsureCapacity();
    return 0;
}
