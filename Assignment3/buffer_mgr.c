
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <limits.h>
#include "dberror.h"

#define K_VAL 2


RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy, void *stratData) {

    SM_FileHandle *fHandle = malloc(sizeof(SM_FileHandle));
    RC rc = openPageFile((char *)pageFileName, fHandle);
    if (rc != RC_OK) return rc;

    BM_MgmtData *mgmt = malloc(sizeof(BM_MgmtData));
    mgmt->fh = fHandle;  

    mgmt->frames = malloc(sizeof(Frame) * numPages);

    for (int i = 0; i < numPages; i++) {
        mgmt->frames[i].pageNum = NO_PAGE;
        mgmt->frames[i].fixCount = 0;
        mgmt->frames[i].isDirty = false;
        mgmt->frames[i].refCount = 0;
        mgmt->frames[i].lastUsed = 0;
        mgmt->frames[i].histIdx = 0;
        for (int k = 0; k < K_VAL; k++) {
            mgmt->frames[i].history[k] = 0;
        }
        mgmt->frames[i].referenceBit = false;
        mgmt->frames[i].data = malloc(PAGE_SIZE);
        if (mgmt->frames[i].data == NULL) {
            printf("malloc for frame[%d].data failed\n", i);
            return RC_ERROR;
        }

        mgmt->frames[i].next = &mgmt->frames[(i + 1) % numPages];
    }

    mgmt->clockHand = &mgmt->frames[0];
    mgmt->fifoPtr = &mgmt->frames[0];


    mgmt->numPages = numPages;
    mgmt->readIO = 0;
    mgmt->writeIO = 0;
    mgmt->timestamp = 0;

    bm->pageFile = strdup(pageFileName);
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = mgmt;

    return RC_OK;
}


RC shutdownBufferPool(BM_BufferPool *const bm) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;

    forceFlushPool(bm);

    Frame *curr = mgmtData->frames;
    Frame *start = curr;
    do {
        Frame *temp = curr->next;
        if (curr->data != NULL)
            free(curr->data);
        curr = temp;
    } while (curr != start);

    free(mgmtData->frames);  
    closePageFile(mgmtData->fh);  
    free(mgmtData->fh);           
    free(mgmtData);          
    free(bm->pageFile);  
    bm->mgmtData = NULL;
    return RC_OK;
}


RC forceFlushPool(BM_BufferPool *const bm) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
    Frame *curr = mgmtData->frames;
    Frame *start = curr;
    do {
        if (curr->isDirty && curr->fixCount == 0) {
            writeBlock(curr->pageNum, mgmtData->fh, curr->data);
            mgmtData->numWriteIO++;
            curr->isDirty = false;
        }
        curr = curr->next;
    } while (curr != start);
    return RC_OK;
}

RC markDirty(BM_BufferPool *const bm, BM_PageHandle *const page) {
    Frame *curr = ((BM_MgmtData *)bm->mgmtData)->frames;
    Frame *start = curr;
    do {
        if (curr->pageNum == page->pageNum) {
            curr->isDirty = true;
            return RC_OK;
        }
        curr = curr->next;
    } while (curr != start);
    return RC_ERROR;
}

RC unpinPage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    Frame *curr = ((BM_MgmtData *)bm->mgmtData)->frames;
    Frame *start = curr;
    do {
        if (curr->pageNum == page->pageNum) {
            if (curr->fixCount > 0) curr->fixCount--;
            return RC_OK;
        }
        curr = curr->next;
    } while (curr != start);
    return RC_ERROR;
}

RC forcePage(BM_BufferPool *const bm, BM_PageHandle *const page) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
    Frame *curr = mgmtData->frames;
    Frame *start = curr;
    do {
        if (curr->pageNum == page->pageNum) {
            writeBlock(curr->pageNum, mgmtData->fh, curr->data);
            mgmtData->numWriteIO++;
            curr->isDirty = false;
            return RC_OK;
        }
        curr = curr->next;
    } while (curr != start);
    return RC_ERROR;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page, const PageNumber pageNum) 
    {
    printf(">> pinPage() called for pageNum = %d\n", pageNum); fflush(stdout);
    BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
    Frame *curr = mgmtData->frames;
    Frame *start = curr;
    do {
        if (curr->pageNum == pageNum) {
            curr->fixCount++;
            curr->referenceBit = true;
            curr->lastUsed = ++mgmtData->timestamp;
            curr->history[curr->histIdx % K_VAL] = mgmtData->timestamp;
            curr->histIdx++;
            page->pageNum = pageNum;
            page->data = curr->data;
            return RC_OK;
        }
        curr = curr->next;
    } while (curr != start);

    Frame *victim = NULL;
    if (bm->strategy == RS_CLOCK) {
        int passes = 0;
        while (passes < 2 * bm->numPages) {
            if (mgmtData->clockHand->fixCount == 0) {
                if (!mgmtData->clockHand->referenceBit) {
                    victim = mgmtData->clockHand;
                    break;
                } else {
                    mgmtData->clockHand->referenceBit = false;
                }
            }
            mgmtData->clockHand = mgmtData->clockHand->next;
            passes++;
        }
        if (victim != NULL) mgmtData->clockHand = victim->next;
    } else if (bm->strategy == RS_FIFO) {
        Frame *ptr = mgmtData->fifoPtr;
        do {
            if (ptr->fixCount == 0) {
                victim = ptr;
                break;
            }
            ptr = ptr->next;
        } while (ptr != mgmtData->fifoPtr);
    } else if (bm->strategy == RS_LRU) {
        Frame *ptr = mgmtData->frames;
        int minTime = INT_MAX;
        do {
            if (ptr->fixCount == 0 && ptr->lastUsed < minTime) {
                victim = ptr;
                minTime = ptr->lastUsed;
            }
            ptr = ptr->next;
        } while (ptr != mgmtData->frames);
    } else if (bm->strategy == RS_LRU_K) {
        Frame *ptr = mgmtData->frames;
        int minKth = INT_MAX;
        do {
            if (ptr->fixCount == 0 && ptr->histIdx >= K_VAL) {
                int oldest = INT_MAX;
                for (int i = 0; i < K_VAL; i++) {
                    if (ptr->history[i] < oldest) {
                        oldest = ptr->history[i];
                    }
                }
                if (oldest < minKth) {
                    victim = ptr;
                    minKth = oldest;
                }
            }
            ptr = ptr->next;
        } while (ptr != mgmtData->frames);

        if (victim == NULL) {
            ptr = mgmtData->frames;
            int minTime = INT_MAX;
            do {
                if (ptr->fixCount == 0 && ptr->lastUsed < minTime) {
                    victim = ptr;
                    minTime = ptr->lastUsed;
                }
                ptr = ptr->next;
            } while (ptr != mgmtData->frames);
        }
    }

    if (victim == NULL) return RC_BUFFER_POOL_FULL;

    if (victim->isDirty) {
        writeBlock(victim->pageNum, mgmtData->fh, victim->data);
        mgmtData->numWriteIO++;
    }

    if (pageNum >= mgmtData->fh->totalNumPages)
 {
        RC rc = ensureCapacity(pageNum + 1, mgmtData->fh);
        if (rc != RC_OK) return rc;
    }

    if (victim->data == NULL) {
    printf(">>> victim->data is NULL!\n"); fflush(stdout);
    return RC_ERROR;
    }

    RC rc = readBlock(pageNum, mgmtData->fh, victim->data);
    if (rc != RC_OK) return rc;

    victim->pageNum = pageNum;
    victim->fixCount = 1;
    victim->isDirty = false;
    victim->referenceBit = true;
    victim->lastUsed = ++mgmtData->timestamp;
    victim->history[victim->histIdx % K_VAL] = mgmtData->timestamp;
    victim->histIdx++;

    page->pageNum = pageNum;
    page->data = victim->data;
    mgmtData->numReadIO++;

    if (bm->strategy == RS_FIFO) {
        mgmtData->fifoPtr = victim->next;
    }

    return RC_OK;
}

int *getFrameContents(BM_BufferPool *const bm) {
    int *contents = (int *)malloc(sizeof(int) * bm->numPages);
    Frame *curr = ((BM_MgmtData *)bm->mgmtData)->frames;
    for (int i = 0; i < bm->numPages; i++) {
        contents[i] = curr->pageNum;
        curr = curr->next;
    }
    return contents;
}

bool *getDirtyFlags(BM_BufferPool *const bm) {
    bool *flags = (bool *)malloc(sizeof(bool) * bm->numPages);
    Frame *curr = ((BM_MgmtData *)bm->mgmtData)->frames;
    for (int i = 0; i < bm->numPages; i++) {
        flags[i] = curr->isDirty;
        curr = curr->next;
    }
    return flags;
}

int *getFixCounts(BM_BufferPool *const bm) {
    int *counts = (int *)malloc(sizeof(int) * bm->numPages);
    Frame *curr = ((BM_MgmtData *)bm->mgmtData)->frames;
    for (int i = 0; i < bm->numPages; i++) {
        counts[i] = curr->fixCount;
        curr = curr->next;
    }
    return counts;
}

int getNumReadIO(BM_BufferPool *const bm) {
    return ((BM_MgmtData *)bm->mgmtData)->numReadIO;
}

int getNumWriteIO(BM_BufferPool *const bm) {
    return ((BM_MgmtData *)bm->mgmtData)->numWriteIO;
}
