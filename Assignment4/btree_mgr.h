#ifndef BTREE_MGR_H
#define BTREE_MGR_H
#define MAX_KEYS 10

#include "buffer_mgr.h"
#include "dberror.h"
#include "tables.h"
#include "storage_mgr.h"


typedef struct BTreeMeta {
    int rootPage;
    int numNodes;
    int numEntries;
    int keyType;
    int fanout;
} BTreeMeta;

typedef struct BTreeMgmtData {
    SM_FileHandle fHandle;
    BTreeMeta meta;
    BM_BufferPool bufferPool; 
    int numNodes;
    int numEntries;
} BTreeMgmtData;


// structure for accessing btrees
typedef struct BTreeHandle {
  DataType keyType;
  char *idxId;
  void *mgmtData;
} BTreeHandle;

typedef struct BT_ScanHandle {
  BTreeHandle *tree;
  void *mgmtData;
} BT_ScanHandle;

typedef struct BTreeNode {
    int isLeaf;
    int numKeys;
    Value *keys[MAX_KEYS];
    RID rids[MAX_KEYS];
    PageNumber children[MAX_KEYS + 1];
} BTreeNode;



// init and shutdown index manager
extern RC initIndexManager (void *mgmtData);
extern RC shutdownIndexManager ();

// create, destroy, open, and close an btree index
extern RC createBtree (char *idxId, DataType keyType, int n);
extern RC openBtree (BTreeHandle **tree, char *idxId);
extern RC closeBtree (BTreeHandle *tree);
extern RC deleteBtree (char *idxId);

// access information about a b-tree
extern RC getNumNodes (BTreeHandle *tree, int *result);
extern RC getNumEntries (BTreeHandle *tree, int *result);
extern RC getKeyType (BTreeHandle *tree, DataType *result);

// index access
extern RC findKey (BTreeHandle *tree, Value *key, RID *result);
extern RC insertKey (BTreeHandle *tree, Value *key, RID rid);
extern RC deleteKey (BTreeHandle *tree, Value *key);
extern RC openTreeScan (BTreeHandle *tree, BT_ScanHandle **handle);
extern RC nextEntry (BT_ScanHandle *handle, RID *result);
extern RC closeTreeScan (BT_ScanHandle *handle);

// debug and test functions
extern char *printTree (BTreeHandle *tree);

#endif // BTREE_MGR_H
