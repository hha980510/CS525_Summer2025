#ifndef BUFFER_MANAGER_H
#define BUFFER_MANAGER_H
#define K_VAL 2


// Include return codes and methods for logging errors
#include "dberror.h"

// Include bool DT
#include "dt.h"

#include "storage_mgr.h"

// Replacement Strategies
typedef enum ReplacementStrategy {
	RS_FIFO = 0,
	RS_LRU = 1,
	RS_CLOCK = 2,
	RS_LFU = 3,
	RS_LRU_K = 4
} ReplacementStrategy;

// Data Types and Structures
typedef int PageNumber;
#define NO_PAGE -1

typedef struct BM_BufferPool {
	char *pageFile;
	int numPages;
	ReplacementStrategy strategy;
	void *mgmtData; // use this one to store the bookkeeping info your buffer
	// manager needs for a buffer pool
} BM_BufferPool;

typedef struct BM_PageHandle {
	PageNumber pageNum;
	char *data;
} BM_PageHandle;

// convenience macros
#define MAKE_POOL()					\
		((BM_BufferPool *) malloc (sizeof(BM_BufferPool)))

#define MAKE_PAGE_HANDLE()				\
		((BM_PageHandle *) malloc (sizeof(BM_PageHandle)))

// Buffer Manager Interface Pool Handling
RC initBufferPool(BM_BufferPool *const bm, const char *const pageFileName, 
		const int numPages, ReplacementStrategy strategy,
		void *stratData);
RC shutdownBufferPool(BM_BufferPool *const bm);
RC forceFlushPool(BM_BufferPool *const bm);

// Buffer Manager Interface Access Pages
RC markDirty (BM_BufferPool *const bm, BM_PageHandle *const page);
RC unpinPage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC forcePage (BM_BufferPool *const bm, BM_PageHandle *const page);
RC pinPage (BM_BufferPool *const bm, BM_PageHandle *const page, 
		const PageNumber pageNum);

// Statistics Interface
PageNumber *getFrameContents (BM_BufferPool *const bm);
bool *getDirtyFlags (BM_BufferPool *const bm);
int *getFixCounts (BM_BufferPool *const bm);
int getNumReadIO (BM_BufferPool *const bm);
int getNumWriteIO (BM_BufferPool *const bm);

typedef struct Frame {
    PageNumber pageNum;
    char *data;
    bool dirty;
    int fixCount;
    int refCount;
    bool isDirty;
	bool referenceBit;    
    int lastUsed;          
    int histIdx;           
    int history[K_VAL]; 
    struct Frame *next;
} Frame;

typedef struct BM_MgmtData {
    SM_FileHandle *fh;
    Frame *frames;
    int numPages;
    int readIO;
    int writeIO;
	Frame *clockHand;
    Frame *fifoPtr;     
	int timestamp;
	int numWriteIO;
	int numReadIO;
} BM_MgmtData;



#endif