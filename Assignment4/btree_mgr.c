#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer_mgr.h"
#include "btree_mgr.h"
#include "storage_mgr.h"
#include "dberror.h"
#include "tables.h"

#define META_PAGE 0
#define ROOT_PAGE 1
#define PAGE_SIZE 4096

typedef struct BTreeMeta {
    int rootPage;
    int numNodes;
    int numEntries;
    int keyType;
    int fanout;
} BTreeMeta;

void serializeMeta(char *page, BTreeMeta *meta) {
    memcpy(page, meta, sizeof(BTreeMeta));
}

void deserializeMeta(char *page, BTreeMeta *meta) {
    memcpy(meta, page, sizeof(BTreeMeta));
}

RC createBtree(char *idxId, DataType keyType, int n) {
    initStorageManager();
    RC rc;

    rc = createPageFile(idxId);
    if (rc != RC_OK) {
        printf("createPageFile failed with code %d\n", rc);
        return rc;
    }

    SM_FileHandle fh;
    rc = openPageFile(idxId, &fh);
    if (rc != RC_OK) {
        printf("openPageFile failed with code %d\n", rc);
        return rc;
    }

    char *page = (char *)malloc(PAGE_SIZE);
    if (page == NULL) {
        printf("malloc failed\n");
        return RC_NOMEM;
    }

    BTreeMeta meta;
    meta.rootPage = ROOT_PAGE;
    meta.numNodes = 1;
    meta.numEntries = 0;
    meta.keyType = keyType;
    meta.fanout = n;

    memset(page, 0, PAGE_SIZE);
    serializeMeta(page, &meta);
    rc = writeBlock(META_PAGE, &fh, page);
    if (rc != RC_OK) {
        printf("writeBlock (meta) failed with code %d\n", rc);
        free(page);
        return rc;
    }

    memset(page, 0, PAGE_SIZE);
    page[0] = 1;
    page[1] = 0;
    rc = writeBlock(ROOT_PAGE, &fh, page);
    if (rc != RC_OK) {
        printf("writeBlock (root) failed with code %d\n", rc);
        free(page);
        return rc;
    }

    free(page);
    rc = closePageFile(&fh);
    if (rc != RC_OK) {
        printf("closePageFile failed with code %d\n", rc);
        return rc;
    }

    return RC_OK;
}

RC openBtree(BTreeHandle **tree, char *idxId) {
    RC rc;
    SM_FileHandle fh;

    rc = openPageFile(idxId, &fh);
    if (rc != RC_OK) return rc;

    char *page = (char *)malloc(PAGE_SIZE);
    rc = readBlock(META_PAGE, &fh, page);
    if (rc != RC_OK) return rc;

    BTreeMgmtData *mgmt = (BTreeMgmtData *)malloc(sizeof(BTreeMgmtData));
    deserializeMeta(page, &mgmt->meta);
    mgmt->fHandle = fh;

    BTreeHandle *newTree = (BTreeHandle *)malloc(sizeof(BTreeHandle));
    newTree->idxId = idxId;
    newTree->keyType = (DataType)(mgmt->meta.keyType);
    newTree->mgmtData = mgmt;

    *tree = newTree;

    free(page);
    return RC_OK;
}

RC closeBtree(BTreeHandle *tree) {
    if (tree == NULL || tree->mgmtData == NULL) return RC_IM_ERROR;

    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;

    if (mgmt->fHandle.mgmtInfo != NULL)
        fclose((FILE *)mgmt->fHandle.mgmtInfo);

    free(mgmt);
    free(tree);

    return RC_OK;
}


RC deleteBtree(char *idxId) {
    if (remove(idxId) != 0) {
        perror("remove failed");
        return RC_FILE_NOT_FOUND;
    }
    return RC_OK;
}


RC getKeyType(BTreeHandle *tree, DataType *result) {
    if (tree == NULL || result == NULL)
        return RC_IM_ERROR;

    *result = tree->keyType;
    return RC_OK;
}

RC getNumNodes(BTreeHandle *tree, int *result) {
    if (tree == NULL || result == NULL || tree->mgmtData == NULL)
        return RC_IM_ERROR;

    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    *result = mgmt->numNodes;
    return RC_OK;
}

RC getNumEntries(BTreeHandle *tree, int *result) {
    if (tree == NULL || result == NULL || tree->mgmtData == NULL)
        return RC_IM_ERROR;

    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    *result = mgmt->numEntries;
    return RC_OK;
}

RC splitNode(BTreeHandle *tree, BTreeNode *node, BM_PageHandle *ph, Value *key, RID rid, PageNumber currentPage);

RC insertKey(BTreeHandle *tree, Value *key, RID rid) {
    if (!tree || !tree->mgmtData)
        return RC_IM_ERROR;

    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    BM_BufferPool *bp = &mgmt->bufferPool;
    PageNumber currentPage = mgmt->meta.rootPage;
    BM_PageHandle pageData;
    RC rc;

    rc = pinPage(bp, &pageData, currentPage);
    if (rc != RC_OK)
        return rc;

    BTreeNode *node = (BTreeNode *)pageData.data;

    if (node->isLeaf) {
        if (node->numKeys < mgmt->meta.fanout - 1) {
            node->keys[node->numKeys] = key;
            node->rids[node->numKeys] = rid;
            node->numKeys++;

            markDirty(bp, &pageData);
            unpinPage(bp, &pageData);
            return RC_OK;
        } else {
            unpinPage(bp, &pageData);
            return splitNode(tree, node, &pageData, key, rid, currentPage);
        }
    } else {
        unpinPage(bp, &pageData);
        return RC_IM_ERROR;
    }
}

RC findKey(BTreeHandle *tree, Value *key, RID *result) {
    if (tree == NULL || tree->mgmtData == NULL) return RC_IM_ERROR;

    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    SM_FileHandle fh = mgmt->fHandle;
    char *page = (char *)malloc(PAGE_SIZE);

    RC rc = readBlock(mgmt->meta.rootPage, &fh, page);
    if (rc != RC_OK) return rc;

    int numKeys = page[1];
    int found = 0;
    for (int i = 0; i < numKeys; i++) {
        int keyVal;
        memcpy(&keyVal, page + 2 + i * (sizeof(int) + sizeof(RID)), sizeof(int));
        if (keyVal == key->v.intV) {
            memcpy(result, page + 2 + i * (sizeof(int) + sizeof(RID)) + sizeof(int), sizeof(RID));
            found = 1;
            break;
        }
    }

    free(page);
    return found ? RC_OK : RC_IM_KEY_NOT_FOUND;
}


RC deleteKey(BTreeHandle *tree, Value *key) {
    if (tree == NULL || tree->mgmtData == NULL) return RC_IM_ERROR;

    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;
    SM_FileHandle fh = mgmt->fHandle;
    char *page = (char *)malloc(PAGE_SIZE);

    RC rc = readBlock(mgmt->meta.rootPage, &fh, page);
    if (rc != RC_OK) return rc;

    int numKeys = page[1];
    int found = 0;
    for (int i = 0; i < numKeys; i++) {
        int keyVal;
        memcpy(&keyVal, page + 2 + i * (sizeof(int) + sizeof(RID)), sizeof(int));
        if (keyVal == key->v.intV) {
            for (int j = i; j < numKeys - 1; j++) {
                memcpy(page + 2 + j * (sizeof(int) + sizeof(RID)),
                       page + 2 + (j + 1) * (sizeof(int) + sizeof(RID)),
                       sizeof(int) + sizeof(RID));
            }
            page[1] = numKeys - 1;
            found = 1;
            break;
        }
    }

    if (found)
        rc = writeBlock(mgmt->meta.rootPage, &fh, page);

    free(page);
    return found ? rc : RC_IM_KEY_NOT_FOUND;
}

RC splitNode(BTreeHandle *tree, BTreeNode *node, BM_PageHandle *ph, Value *key, RID rid, PageNumber parentPageNum) {
    BTreeMgmtData *mgmt = (BTreeMgmtData *)tree->mgmtData;

    int mid = MAX_KEYS / 2;

    BM_PageHandle newPh;
    RC rc = ensureCapacity(mgmt->meta.numNodes + 1, &mgmt->fHandle);
    if (rc != RC_OK) return rc;

    int newPageNum = mgmt->meta.numNodes;
    mgmt->meta.numNodes++;

    rc = pinPage(&mgmt->bufferPool, &newPh, newPageNum);
    if (rc != RC_OK) return rc;

    BTreeNode *newNode = (BTreeNode *)newPh.data;
    newNode->isLeaf = node->isLeaf;
    newNode->numKeys = 0;

    for (int i = mid, j = 0; i < node->numKeys; i++, j++) {
        newNode->keys[j] = node->keys[i];
        newNode->rids[j] = node->rids[i];
        if (!node->isLeaf)
            newNode->children[j] = node->children[i];
        newNode->numKeys++;
    }
    if (!node->isLeaf)
        newNode->children[newNode->numKeys] = node->children[node->numKeys];

    node->numKeys = mid;

    BM_PageHandle rootPh;
    rc = pinPage(&mgmt->bufferPool, &rootPh, mgmt->meta.numNodes);
    if (rc != RC_OK) return rc;

    BTreeNode *newRoot = (BTreeNode *)rootPh.data;
    newRoot->isLeaf = 0;
    newRoot->numKeys = 1;
    newRoot->keys[0] = node->keys[mid];
    newRoot->children[0] = parentPageNum;
    newRoot->children[1] = newPageNum;

    mgmt->meta.rootPage = mgmt->meta.numNodes;
    mgmt->meta.numNodes++;

    markDirty(&mgmt->bufferPool, &newPh);
    markDirty(&mgmt->bufferPool, &rootPh);
    unpinPage(&mgmt->bufferPool, &newPh);
    unpinPage(&mgmt->bufferPool, &rootPh);
    forcePage(&mgmt->bufferPool, &newPh);
    forcePage(&mgmt->bufferPool, &rootPh);

    return RC_OK;
}

RC initIndexManager(void *mgmtData) {
    printf("initIndexManager called\n");
    return RC_OK;
}

RC shutdownIndexManager() {
    printf("shutdownIndexManager called\n");
    return RC_OK;
}

RC openTreeScan(BTreeHandle *tree, BT_ScanHandle **handle) {
    if (!tree || !handle) return RC_IM_ERROR;

    BT_ScanHandle *sc = malloc(sizeof(BT_ScanHandle));
    if (!sc) return RC_NOMEM;

    sc->tree = tree;
    sc->mgmtData = NULL;

    *handle = sc;
    printf("openTreeScan called\n");
    return RC_OK;
}

RC nextEntry(BT_ScanHandle *handle, RID *result) {
    if (!handle || !result) return RC_IM_ERROR;

    return RC_IM_NO_MORE_ENTRIES;
}

RC closeTreeScan(BT_ScanHandle *handle) {
    if (!handle) return RC_IM_ERROR;

    free(handle);
    printf("closeTreeScan called\n");
    return RC_OK;
}
