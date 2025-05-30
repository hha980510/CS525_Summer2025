#include "storage_mgr.h"
#include "test_helper.h"
char *testName;

#define TESTPF "test_pagefile.bin"

void testCreateOpenClose(void) {
    SM_FileHandle fh;

    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));
    ASSERT_TRUE(strcmp(fh.fileName, TESTPF) == 0, "filename correct");
    ASSERT_TRUE(fh.totalNumPages == 1, "expect 1 page in new file");
    ASSERT_TRUE(fh.curPagePos == 0, "freshly opened file's page position should be 0");

    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(destroyPageFile(TESTPF));
    TEST_DONE();
}

void testSinglePageContent(void) {
    SM_FileHandle fh;
    SM_PageHandle ph;

    ph = (SM_PageHandle) malloc(PAGE_SIZE);

    remove(TESTPF);
    TEST_CHECK(createPageFile(TESTPF));
    TEST_CHECK(openPageFile(TESTPF, &fh));

    for (int i = 0; i < PAGE_SIZE; i++)
        ph[i] = (i % 10) + '0';

    TEST_CHECK(writeBlock(0, &fh, ph));
    TEST_CHECK(readBlock(0, &fh, ph));

    for (int i = 0; i < PAGE_SIZE; i++)
        ASSERT_TRUE(ph[i] == (i % 10) + '0', "character in page read from disk is the one we expected.");

    printf("reading first block\n");
    TEST_CHECK(readFirstBlock(&fh, ph));
    printf("finished test\n");

    TEST_CHECK(closePageFile(&fh));
    TEST_CHECK(destroyPageFile(TESTPF));
    free(ph);
    TEST_DONE();
}

int main(void) {
    initStorageManager();
    testCreateOpenClose();
    testSinglePageContent();
    return 0;
}
