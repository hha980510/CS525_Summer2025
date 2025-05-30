README.txt

This implementation of the Storage Manager supports the required functions from the CS525 Assignment 1 specification.

The file handle structure is used to track current page and total number of pages. The implementation uses FILE* pointers stored in mgmtInfo for actual file operations.

All blocks are managed in PAGE_SIZE units. The storage manager supports reading, writing, appending, and capacity enforcement for fixed-size page files.

Key files:
- store_mgr.c: Main implementation
- dberror.c/h: Error handling
- storage_mgr.h: Interface definition
- test_assign1_1.c: Provided tests
