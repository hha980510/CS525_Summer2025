#include "storage_mgr.h"
#include "dberror.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define PAGE_SIZE 4096

void initStorageManager(void) {
    printf("Storage Manager initialized.\n");
}

RC createPageFile(char *fileName) {
    FILE *file = fopen(fileName, "wb");
    if (!file) {
        return RC_FILE_NOT_FOUND;
    }

    char *emptyPage = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (!emptyPage) {
        fclose(file);
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    fwrite(emptyPage, sizeof(char), PAGE_SIZE, file);
    free(emptyPage);
    fclose(file);
    return RC_OK;
}

RC openPageFile(char *fileName, SM_FileHandle *fHandle) {
    FILE *file = fopen(fileName, "rb+");
    if (!file) {
        return RC_FILE_NOT_FOUND;
    }

    fseek(file, 0L, SEEK_END);
    long fileSize = ftell(file);
    int totalPages = fileSize / PAGE_SIZE;

    fHandle->fileName = fileName;
    fHandle->totalNumPages = totalPages;
    fHandle->curPagePos = 0;
    fHandle->mgmtInfo = file;

    return RC_OK;
}

RC closePageFile(SM_FileHandle *fHandle) {
    if (!fHandle || !fHandle->mgmtInfo) {
        return RC_FILE_NOT_FOUND;
    }

    fclose((FILE *)fHandle->mgmtInfo);
    fHandle->mgmtInfo = NULL;
    return RC_OK;
}

RC destroyPageFile(char *fileName) {
    if (remove(fileName) != 0) {
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}

RC readBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        return RC_READ_NON_EXISTING_PAGE;
    }

    FILE *file = (FILE *)fHandle->mgmtInfo;
    fseek(file, pageNum * PAGE_SIZE, SEEK_SET);

        size_t bytesRead = fread(memPage, sizeof(char), PAGE_SIZE, file);
    if (bytesRead != PAGE_SIZE) {
        return RC_READ_FAILED;
    }

    fHandle->curPagePos = pageNum;
    return RC_OK;
}

int getBlockPos(SM_FileHandle *fHandle) {
    return fHandle->curPagePos;
}

RC readFirstBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(0, fHandle, memPage);
}

RC readPreviousBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle->curPagePos <= 0) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    return readBlock(fHandle->curPagePos - 1, fHandle, memPage);
}

RC readCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->curPagePos, fHandle, memPage);
}

RC readNextBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (fHandle->curPagePos >= fHandle->totalNumPages - 1) {
        return RC_READ_NON_EXISTING_PAGE;
    }
    return readBlock(fHandle->curPagePos + 1, fHandle, memPage);
}

RC readLastBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return readBlock(fHandle->totalNumPages - 1, fHandle, memPage);
}

RC writeBlock(int pageNum, SM_FileHandle *fHandle, SM_PageHandle memPage) {
    if (pageNum < 0 || pageNum >= fHandle->totalNumPages) {
        return RC_WRITE_FAILED;
    }

    FILE *file = (FILE *)fHandle->mgmtInfo;
    fseek(file, pageNum * PAGE_SIZE, SEEK_SET);

        size_t bytesWritten = fwrite(memPage, sizeof(char), PAGE_SIZE, file);
    if (bytesWritten != PAGE_SIZE) {
        return RC_WRITE_FAILED;
    }


    fHandle->curPagePos = pageNum;
    return RC_OK;
}

RC writeCurrentBlock(SM_FileHandle *fHandle, SM_PageHandle memPage) {
    return writeBlock(fHandle->curPagePos, fHandle, memPage);
}

RC appendEmptyBlock(SM_FileHandle *fHandle) {
    FILE *file = (FILE *)fHandle->mgmtInfo;

    char *emptyPage = (char *)calloc(PAGE_SIZE, sizeof(char));
    if (!emptyPage) {
        return RC_MEMORY_ALLOCATION_ERROR;
    }

    fseek(file, 0L, SEEK_END);
    fwrite(emptyPage, sizeof(char), PAGE_SIZE, file);
    free(emptyPage);

    fHandle->totalNumPages += 1;
    return RC_OK;
}

RC ensureCapacity(int numberOfPages, SM_FileHandle *fHandle) {
    while (fHandle->totalNumPages < numberOfPages) {
        appendEmptyBlock(fHandle);
    }
    return RC_OK;
}
