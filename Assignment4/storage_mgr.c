#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "storage_mgr.h"

void initStorageManager(void) {
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {
    RC rc = ensureCapacity(fHandle->totalNumPages + 1, fHandle);
    if (rc != RC_OK)
        return rc;

    SM_PageHandle emptyPage = (SM_PageHandle) calloc(PAGE_SIZE, sizeof(char));
    rc = writeBlock(fHandle->totalNumPages, fHandle, emptyPage);
    free(emptyPage);
    return rc;
}

RC createPageFile(char *fileName) {
    FILE *fp = fopen(fileName, "wb");
    if (fp == NULL)
        return RC_FILE_NOT_FOUND;

    SM_PageHandle emptyPage = (SM_PageHandle)calloc(PAGE_SIZE, sizeof(char));
    fwrite(emptyPage, sizeof(char), PAGE_SIZE, fp);
    fclose(fp);
    free(emptyPage);
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    FILE *fp = fopen(fileName, "rb+");
    if (fp == NULL)
        return RC_FILE_NOT_FOUND;

    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    rewind(fp);

    fHandle->fileName = fileName;
    fHandle->totalNumPages = size / PAGE_SIZE;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = fp;

    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
    if (fHandle->mgmtInfo == NULL)
        return RC_FILE_HANDLE_NOT_INIT;
    fclose((FILE *)fHandle->mgmtInfo);
    return RC_OK;
}

RC destroyPageFile(char *fileName) {
    if (remove(fileName) != 0)
        return RC_FILE_NOT_FOUND;
    return RC_OK;
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (pageNum >= fHandle->totalNumPages || pageNum < 0)
        return RC_READ_NON_EXISTING_PAGE;

    FILE *fp = (FILE *)fHandle->mgmtInfo;
    fseek(fp, pageNum * PAGE_SIZE, SEEK_SET);
    fread(memPage, sizeof(char), PAGE_SIZE, fp);
    fHandle->curPagePos = pageNum;

    return RC_OK;
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    printf("Simulated writeBlock called for page %d\n", pageNum);
    return RC_OK;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    if (fHandle->totalNumPages < numberOfPages) {
        int pagesToAdd = numberOfPages - fHandle->totalNumPages;
        SM_PageHandle emptyPage = (SM_PageHandle) malloc(PAGE_SIZE);
        memset(emptyPage, 0, PAGE_SIZE);
        for (int i = 0; i < pagesToAdd; i++) {
            appendEmptyBlock(fHandle);
        }
        free(emptyPage);
    }
    return RC_OK;
}
