#include <stdio.h>
#include <stdlib.h>
#include "btree_mgr.h"
#include "dberror.h"

int main() {
    BTreeHandle *tree = NULL;
    char *fname = "test_btree.bin";
    RC rc;

    // 1. Create B+ Tree
    rc = createBtree(fname, DT_INT, 4);
    if (rc != RC_OK) { printf("❌ createBtree failed\n"); return rc; }

    // 2. Open
    rc = openBtree(&tree, fname);
    if (rc != RC_OK) { printf("❌ openBtree failed\n"); return rc; }

    // 3. Insert keys
    for (int i = 1; i <= 5; i++) {
        Value *v = (Value *)malloc(sizeof(Value));
        v->dt = DT_INT;
        v->v.intV = i * 10;
        RID rid = { .page = 0, .slot = i };
        rc = insertKey(tree, v, rid);
        if (rc != RC_OK) { printf("❌ insertKey failed for key %d\n", v->v.intV); return rc; }
        printf("✅ Inserted key %d\n", v->v.intV);
        free(v);
    }

    // 4. Find keys
    for (int i = 1; i <= 5; i++) {
        Value *v = (Value *)malloc(sizeof(Value));
        v->dt = DT_INT;
        v->v.intV = i * 10;
        RID resultRid;
        rc = findKey(tree, v, &resultRid);
        if (rc == RC_OK)
            printf("🔍 Found key %d -> RID (slot=%d)\n", v->v.intV, resultRid.slot);
        else
            printf("❌ Could not find key %d\n", v->v.intV);
        free(v);
    }

    // 5. Delete keys
    for (int i = 1; i <= 5; i++) {
        Value *v = (Value *)malloc(sizeof(Value));
        v->dt = DT_INT;
        v->v.intV = i * 10;
        rc = deleteKey(tree, v);
        if (rc == RC_OK)
            printf("🗑️ Deleted key %d\n", v->v.intV);
        else
            printf("❌ Failed to delete key %d\n", v->v.intV);
        free(v);
    }

    // 6. Close and delete tree
    rc = closeBtree(tree);
    if (rc != RC_OK) { printf("❌ closeBtree failed\n"); return rc; }

    rc = deleteBtree(fname);
    if (rc != RC_OK) { printf("❌ deleteBtree failed\n"); return rc; }

    printf("🎉 All tests passed!\n");
    return 0;
}
