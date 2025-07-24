B+ Tree Index Manager

Hyunsung Ha A20557555

Overview
This project implements a B+ Tree index manager that supports creating, managing, and querying B+ Tree indexes. It integrates with buffer and storage managers for efficient page caching and disk I/O.

Features
Index Lifecycle: Create, open, close, and delete B+ Tree indexes.

Key Operations: Insert, find, and delete keys with associated record identifiers (RIDs).

Scanning: Open, iterate over, and close index scans in sorted order.

Metadata Access: Retrieve the number of nodes, entries, and key data type.

Testing: Automated tests covering insertion, searching, deletion, and scanning scenarios.

Buffer & Storage Integration: Utilizes buffer manager for page pinning/unpinning and storage manager for persistent disk operations.

File Structure
btree_mgr.c/h: B+ Tree core implementation.

buffer_mgr.c/h: Buffer management for page caching.

storage_mgr.c/h: Disk page management.

expr.c/h: Value and expression utilities.

record_mgr.c/h: Record management utilities.

rm_serializer.c: Record serialization.

test_assign4_1.c: Automated tests using provided macros.

test_helper.h: Testing macros and assertions.

Building and Running Tests
Compile all sources:

gcc -o test_assign4_1 test_assign4_1.c btree_mgr.c dberror.c storage_mgr.c expr.c record_mgr.c rm_serializer.c buffer_mgr.c
Run tests:

./test_assign4_1

Notes
The buffer and storage managers must be correctly implemented and integrated.

Test suite performs randomized insertions and deletions to validate correctness.

Debug logs (e.g., pinPage calls) assist in tracing execution.

Test macros in test_helper.h control error checking behavior and output verbosity.