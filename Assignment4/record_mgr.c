#include "record_mgr.h"
#include "storage_mgr.h"
#include "string.h"
#include "stdlib.h"
#include "stdio.h"
#include "buffer_mgr.h"

RC attrOffset(Schema *schema, int attrNum, int *result);

char *serializeSchemaBinary(Schema *schema, int *outSize);

int getRecordSize(Schema *schema) {
    int size = 0;
    for (int i = 0; i < schema->numAttr; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT:
                size += sizeof(int);
                break;
            case DT_FLOAT:
                size += sizeof(float);
                break;
            case DT_BOOL:
                size += sizeof(bool);
                break;
            case DT_STRING:
                size += schema->typeLength[i];
                break;
        }
    }
    return size;
}

RC createRecord(Record **record, Schema *schema) {
    *record = (Record *) malloc(sizeof(Record));
    if (*record == NULL) return RC_WRITE_FAILED;

    (*record)->data = (char *) malloc(getRecordSize(schema));
    if ((*record)->data == NULL) return RC_WRITE_FAILED;

    // 0으로 초기화
    memset((*record)->data, 0, getRecordSize(schema));

    // RID 초기화
    (*record)->id.page = -1;
    (*record)->id.slot = -1;

    return RC_OK;
}

RC freeRecord(Record *record) {
    if (record != NULL) {
        if (record->data != NULL)
            free(record->data);
        free(record);
    }
    return RC_OK;
}

RC getAttr(Record *record, Schema *schema, int attrNum, Value **value) {
    int offset;
    attrOffset(schema, attrNum, &offset);

    *value = (Value *) malloc(sizeof(Value));
    (*value)->dt = schema->dataTypes[attrNum];
    char *start = record->data + offset;

    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            memcpy(&(*value)->v.intV, start, sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(&(*value)->v.floatV, start, sizeof(float));
            break;
        case DT_BOOL:
            memcpy(&(*value)->v.boolV, start, sizeof(bool));
            break;
        case DT_STRING:
            (*value)->v.stringV = (char *) malloc(schema->typeLength[attrNum] + 1);
            strncpy((*value)->v.stringV, start, schema->typeLength[attrNum]);
            (*value)->v.stringV[schema->typeLength[attrNum]] = '\0';
            break;
    }
    return RC_OK;
}

RC setAttr(Record *record, Schema *schema, int attrNum, Value *value) {
    int offset;
    attrOffset(schema, attrNum, &offset);
    char *start = record->data + offset;

    switch (schema->dataTypes[attrNum]) {
        case DT_INT:
            memcpy(start, &value->v.intV, sizeof(int));
            break;
        case DT_FLOAT:
            memcpy(start, &value->v.floatV, sizeof(float));
            break;
        case DT_BOOL:
            memcpy(start, &value->v.boolV, sizeof(bool));
            break;
        case DT_STRING:
            strncpy(start, value->v.stringV, schema->typeLength[attrNum]);
            break;
    }
    return RC_OK;
}

typedef struct RM_MetaData {
    BM_BufferPool bufferPool;
    int numTuples;
    int firstFreePage;
} RM_MetaData;

RC createDummyPage(BM_BufferPool *bm, int pageNum, char *data) {
    BM_PageHandle ph;
    RC rc = pinPage(bm, &ph, pageNum);
    if (rc != RC_OK) return rc;

    memcpy(ph.data, data, PAGE_SIZE);
    markDirty(bm, &ph);
    rc = unpinPage(bm, &ph);
    return rc;
}

RC insertRecord(RM_TableData *rel, Record *record) {
    printf(">> insertRecord() called\n"); fflush(stdout);   
    printf(">> insertRecord() record = %p, record->data = %p\n", record, record ? record->data : NULL); fflush(stdout);
    RM_MetaData *meta = (RM_MetaData *)rel->mgmtData;
    BM_BufferPool *bm = &meta->bufferPool;
    printf(">> insertRecord: bm = %p\n", bm); fflush(stdout);
    printf(">> insertRecord: bm->pageFile = %s\n", bm->pageFile); fflush(stdout);
    BM_PageHandle page;
    RC rc;

    int recordSize = getRecordSize(rel->schema);
    int slotSize = recordSize;
    int slotsPerPage = PAGE_SIZE / slotSize;
    int pageNum = 1;
    bool inserted = false;

while (!inserted) {
    printf("Trying page %d...\n", pageNum); fflush(stdout);
    rc = pinPage(bm, &page, pageNum);
    printf(">>> pinPage result = %d\n", rc); fflush(stdout);
    if (rc != RC_OK) {
        printf("Page %d does not exist. Creating new page...\n", pageNum); fflush(stdout);
        printf("Page %d not available. Expanding file...\n", pageNum); fflush(stdout);

        SM_FileHandle fh;
        RC capRC = openPageFile(bm->pageFile, &fh);
        if (capRC != RC_OK) return capRC;

        capRC = ensureCapacity(pageNum + 1, &fh);
        if (capRC != RC_OK) return capRC;

        closePageFile(&fh);

        rc = pinPage(bm, &page, pageNum);
        if (rc != RC_OK) return rc;
        if (page.data == NULL) {
            printf("ERROR: pinPage returned NULL data\n");
            return RC_ERROR;
    }

        memset(page.data, 0, PAGE_SIZE);
    }


        for (int slot = 0; slot < slotsPerPage; slot++) {
            int offset = slot * slotSize;
            if (page.data[offset] == 0) {
                printf("Inserting at page %d, slot %d\n", pageNum, slot); fflush(stdout);
                page.data[offset] = 1;
                if (recordSize < 1) return RC_ERROR;
                memcpy(page.data + offset + 1, record->data, recordSize - 1);
                record->id.page = pageNum;
                record->id.slot = slot;

                rc = markDirty(bm, &page);
                if (rc != RC_OK) return rc;
                rc = unpinPage(bm, &page);
                if (rc != RC_OK) return rc;

                meta->numTuples++;
                inserted = true;
                break;
            }
        }

        if (!inserted) {
            rc = unpinPage(bm, &page);
            if (rc != RC_OK) return rc;
            pageNum++;
        }
    }

    return RC_OK;
}

RC getRecord(RM_TableData *rel, RID id, Record *record) {
    RM_MetaData *meta = (RM_MetaData *)rel->mgmtData;
    BM_BufferPool *bm = &meta->bufferPool;
    BM_PageHandle page;
    RC rc;

    int recordSize = getRecordSize(rel->schema);
    int offset = id.slot * recordSize;

    rc = pinPage(bm, &page, id.page);
    if (rc != RC_OK) return rc;

    if (page.data[offset] == 0) {
        unpinPage(bm, &page);
        return RC_READ_NON_EXISTING_PAGE;
    }

    memcpy(record->data, page.data + offset + 1, recordSize - 1);

    record->id.page = id.page;
    record->id.slot = id.slot;

    rc = unpinPage(bm, &page);
    if (rc != RC_OK) return rc;

    return RC_OK;
}

RC updateRecord(RM_TableData *rel, Record *record) {
    RM_MetaData *meta = (RM_MetaData *)rel->mgmtData;
    BM_BufferPool *bm = &meta->bufferPool;
    BM_PageHandle page;
    RC rc;

    int recordSize = getRecordSize(rel->schema);
    int offset = record->id.slot * recordSize;

    rc = pinPage(bm, &page, record->id.page);
    if (rc != RC_OK) return rc;

    if (page.data[offset] == 0) {
        unpinPage(bm, &page);
        return RC_READ_NON_EXISTING_PAGE;
    }

    memcpy(page.data + offset + 1, record->data, recordSize - 1);

    rc = markDirty(bm, &page);
    if (rc != RC_OK) return rc;

    rc = unpinPage(bm, &page);
    if (rc != RC_OK) return rc;

    return RC_OK;
}

RC deleteRecord(RM_TableData *rel, RID id) {
    RM_MetaData *meta = (RM_MetaData *)rel->mgmtData;
    BM_BufferPool *bm = &meta->bufferPool;
    BM_PageHandle page;
    RC rc;

    int recordSize = getRecordSize(rel->schema);
    int offset = id.slot * recordSize;

    rc = pinPage(bm, &page, id.page);
    if (rc != RC_OK) return rc;

    if (page.data[offset] == 0) {
        unpinPage(bm, &page);
        return RC_READ_NON_EXISTING_PAGE;
    }

    page.data[offset] = 0;

    rc = markDirty(bm, &page);
    if (rc != RC_OK) return rc;

    rc = unpinPage(bm, &page);
    if (rc != RC_OK) return rc;

    meta->numTuples--;

    return RC_OK;
}

typedef struct RM_ScanHandle_Mgmt {
    int page;
    int slot;
    Expr *cond;
} RM_ScanHandle_Mgmt;

RC initRecordManager(void *mgmtData) {
    initStorageManager();
    return RC_OK;
}

RC shutdownRecordManager() {
    return RC_OK;
}

RC createTable(char *name, Schema *schema) {
    SM_FileHandle fh;
    RC rc = createPageFile(name);
    if (rc != RC_OK) return rc;
    
    rc = openPageFile(name, &fh);
    if (rc != RC_OK) return rc;

    int schemaSize;
    char *schemaData = serializeSchemaBinary(schema, &schemaSize);

    char pageData[PAGE_SIZE] = {0};
    if (schemaSize > PAGE_SIZE) {
        free(schemaData);
        closePageFile(&fh);
        return RC_WRITE_FAILED;
    }

    memcpy(pageData, schemaData, schemaSize);
    free(schemaData);

    rc = writeBlock(0, &fh, pageData);
    closePageFile(&fh);
    return rc;
}


RC openTable(RM_TableData *rel, char *name) {
    RM_MetaData *meta = malloc(sizeof(RM_MetaData));
    initBufferPool(&meta->bufferPool, name, 100, RS_FIFO, NULL);

    printf(">> openTable() name = %s\n", name); fflush(stdout);
    printf(">> bm->pageFile = %s\n", meta->bufferPool.pageFile); fflush(stdout);

    meta->numTuples = 0;

    rel->mgmtData = meta;
    rel->name = name;

    return RC_OK;
}


RC closeTable(RM_TableData *rel) {
    RM_MetaData *meta = rel->mgmtData;
    shutdownBufferPool(&meta->bufferPool);
    free(meta);
    return RC_OK;
}

RC deleteTable(char *name) {
    return destroyPageFile(name);
}

int getNumTuples(RM_TableData *rel) {
    RM_MetaData *meta = rel->mgmtData;
    return meta->numTuples;
}

RC startScan(RM_TableData *rel, RM_ScanHandle *scan, Expr *cond) {
    RM_ScanHandle_Mgmt *mgmt = malloc(sizeof(RM_ScanHandle_Mgmt));
    mgmt->page = 1;
    mgmt->slot = 0;
    mgmt->cond = cond;

    scan->rel = rel;
    scan->mgmtData = mgmt;
    return RC_OK;
}

RC next(RM_ScanHandle *scan, Record *record) {
    RM_ScanHandle_Mgmt *mgmt = scan->mgmtData;
    RM_TableData *rel = scan->rel;
    RM_MetaData *meta = rel->mgmtData;
    BM_BufferPool *bm = &meta->bufferPool;
    BM_PageHandle page;
    int size = getRecordSize(rel->schema);
    int slots = PAGE_SIZE / size;

    while (mgmt->page < 1000) {
        pinPage(bm, &page, mgmt->page);
        while (mgmt->slot < slots) {
            int offset = mgmt->slot * size;
            if (page.data[offset] == 1) {
                memcpy(record->data, page.data + offset + 1, size - 1);
                record->id.page = mgmt->page;
                record->id.slot = mgmt->slot;

                Value *result;
                evalExpr(record, rel->schema, mgmt->cond, &result);

                mgmt->slot++;
                if (result->v.boolV) {
                    unpinPage(bm, &page);
                    RC rc = RC_OK;
                    free(result);
                    return rc;
                }
                free(result);

            }
            mgmt->slot++;
        }
        mgmt->page++;
        mgmt->slot = 0;
        unpinPage(bm, &page);
    }
    return RC_RM_NO_MORE_TUPLES;
}

RC closeScan(RM_ScanHandle *scan) {
    free(scan->mgmtData);
    return RC_OK;
}

Schema *createSchema(int numAttr, char **attrNames, DataType *dataTypes,
                     int *typeLength, int keySize, int *keys) {
    Schema *schema = malloc(sizeof(Schema));
    schema->numAttr = numAttr;
    schema->attrNames = attrNames;
    schema->dataTypes = dataTypes;
    schema->typeLength = typeLength;
    schema->keySize = keySize;
    schema->keyAttrs = keys;
    return schema;
}

RC freeSchema(Schema *schema) {
    free(schema);
    return RC_OK;
}

RC attrOffset(Schema *schema, int attrNum, int *result) {
    int offset = 0;
    for (int i = 0; i < attrNum; i++) {
        switch (schema->dataTypes[i]) {
            case DT_INT:
                offset += sizeof(int);
                break;
            case DT_FLOAT:
                offset += sizeof(float);
                break;
            case DT_BOOL:
                offset += sizeof(bool);
                break;
            case DT_STRING:
                offset += schema->typeLength[i];
                break;
        }
    }
    *result = offset;
    return RC_OK;
}

char *serializeSchemaBinary(Schema *schema, int *outSize) {
    int totalSize = sizeof(int);
    totalSize += schema->numAttr * (sizeof(DataType) + sizeof(int));

    char *buffer = malloc(totalSize);
    char *ptr = buffer;

    memcpy(ptr, &schema->numAttr, sizeof(int));
    ptr += sizeof(int);

    for (int i = 0; i < schema->numAttr; i++) {
        memcpy(ptr, &schema->dataTypes[i], sizeof(DataType));
        ptr += sizeof(DataType);
        memcpy(ptr, &schema->typeLength[i], sizeof(int));
        ptr += sizeof(int);
    }

    *outSize = totalSize;
    return buffer;
}
