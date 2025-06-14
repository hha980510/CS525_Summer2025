
#include "buffer_mgr.h"
#include "storage_mgr.h"
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <limits.h>

#define K_VAL 2

// Internal frame structure
typedef struct Frame {
    PageNumber pageNum;
    int fixCount;
    bool isDirty;
    bool referenceBit;
    int lastUsed;
    int *history;
    int histIdx;
    char *data;
    struct Frame *next;
} Frame;

// Management metadata for buffer pool
typedef struct BM_MgmtData {
    Frame *frames;
    Frame *clockHand;
    Frame *fifoPtr;
    int numReadIO;
    int numWriteIO;
    int timestamp;
    SM_FileHandle fileHandle;
} BM_MgmtData;

static Frame *createFrame() {
    Frame *frame = (Frame *)malloc(sizeof(Frame));
    frame->pageNum = NO_PAGE;
    frame->fixCount = 0;
    frame->isDirty = false;
    frame->referenceBit = false;
    frame->lastUsed = 0;
    frame->history = (int *)calloc(K_VAL, sizeof(int));
    frame->histIdx = 0;
    frame->data = (char *)calloc(PAGE_SIZE, sizeof(char));
    frame->next = NULL;
    return frame;
}


// LRU-K victim selection
static Frame *selectVictimLRUK(BM_BufferPool *bm, BM_MgmtData *mgmtData) {
    Frame *ptr = mgmtData->frames;
    Frame *victim = NULL;
    int minKthAge = INT_MAX;

    do {
        if (ptr->fixCount == 0 && ptr->histIdx >= K_VAL) {
            int kth = ptr->history[(ptr->histIdx - K_VAL + K_VAL) % K_VAL];
            if (kth < minKthAge || (kth == minKthAge && victim && ptr->lastUsed < victim->lastUsed)) {
                minKthAge = kth;
                victim = ptr;
            }
        }
        ptr = ptr->next;
    } while (ptr != mgmtData->frames);

    if (victim == NULL) {
        ptr = mgmtData->frames;
        int minTime = INT_MAX;
        do {
            if (ptr->fixCount == 0 && ptr->lastUsed < minTime) {
                minTime = ptr->lastUsed;
                victim = ptr;
            }
            ptr = ptr->next;
        } while (ptr != mgmtData->frames);
    }

    return victim;
}



RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName,
                  const int numPages, ReplacementStrategy strategy,
                  void *stratData) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)malloc(sizeof(BM_MgmtData));
    RC rc = openPageFile((char *)pageFileName, &mgmtData->fileHandle);
    if (rc != RC_OK) return rc;

    Frame *head = createFrame();
    Frame *curr = head;
    for (int i = 1; i < numPages; i++) {
        curr->next = createFrame();
        curr = curr->next;
    }
    curr->next = head;

    mgmtData->frames = head;
    mgmtData->clockHand = head;
    mgmtData->fifoPtr = head;
    mgmtData->numReadIO = 0;
    mgmtData->numWriteIO = 0;
    mgmtData->timestamp = 0;

    bm->pageFile = strdup(pageFileName);
    bm->numPages = numPages;
    bm->strategy = strategy;
    bm->mgmtData = mgmtData;

    return RC_OK;
}

RC shutdownBufferPool(BM_BufferPool *const bm) {
    BM_MgmtData *mgmtData = (BM_MgmtData *)bm->mgmtData;
    forceFlushPool(bm);
    Frame *curr = mgmtData->frames;
    Frame *start = curr;
    do {
        Frame *temp = curr->next;
        free(curr->data);
        free(curr->history);
        free(curr);
        curr = temp;
    } while (curr != start);
    closePageFile(&mgmtData->fileHandle);
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
            writeBlock(curr->pageNum, &mgmtData->fileHandle, curr->data);
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
            writeBlock(curr->pageNum, &mgmtData->fileHandle, curr->data);
            mgmtData->numWriteIO++;
            curr->isDirty = false;
            return RC_OK;
        }
        curr = curr->next;
    } while (curr != start);
    return RC_ERROR;
}

RC pinPage(BM_BufferPool *const bm, BM_PageHandle *const page,
           const PageNumber pageNum) {
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
        writeBlock(victim->pageNum, &mgmtData->fileHandle, victim->data);
        mgmtData->numWriteIO++;
    }

    if (pageNum >= mgmtData->fileHandle.totalNumPages) {
        RC rc = ensureCapacity(pageNum + 1, &mgmtData->fileHandle);
        if (rc != RC_OK) return rc;
    }

    RC rc = readBlock(pageNum, &mgmtData->fileHandle, victim->data);
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
