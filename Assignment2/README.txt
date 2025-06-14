# CS525 Assignment 2 – Buffer Manager

## Overview

This project was part of the CS525 course and involved implementing a **buffer manager** that interacts with a lower-level storage manager. The main task was to manage a buffer pool that supports pinning/unpinning pages, tracking dirty pages, and applying different page replacement strategies when the buffer gets full.

The replacement strategies I implemented include:

- **FIFO (First-In First-Out)**
- **CLOCK** (Second-Chance, for extra credit)
- **LRU-K** (Least Recently Used-K, for extra credit)

All the logic was implemented in C without using any external libraries.

---

## My Work

The key parts of the buffer manager I wrote include:

- Initializing and shutting down the buffer pool
- Pinning/unpinning pages to/from memory
- Marking pages as dirty and writing them back to disk
- Keeping track of page I/O operations (reads and writes)
- Implementing three replacement policies (FIFO, CLOCK, LRU-K)

The code lives mostly in `buffer_mgr.c`, where I also added support for tracking K-page history for LRU-K, using circular buffer logic and timestamps.

---

## How to Compile & Run

To compile and run the tests:

```bash
gcc -o test1 test_assign2_1.c buffer_mgr.c storage_mgr.c buffer_mgr_stat.c dberror.c -lm
gcc -o test2 test_assign2_2.c buffer_mgr.c storage_mgr.c buffer_mgr_stat.c dberror.c -lm

./test1
./test2
```

Make sure you're using a C99-compatible compiler like `gcc`.

---

## ✅ Test Status

- `test1`: All FIFO logic passes
- `test2`: CLOCK and LRU-K tests mostly pass (1 edge case under review in LRU-K)
- Confirmed that all `markDirty`, `forcePage`, and `flushPool` functionality behaves correctly

---

## 🔍 Notes

- CLOCK uses a circular pointer (clock hand) and gives unpinned pages a second chance
- LRU-K uses a configurable K value (default 2) to select the page whose K-th latest access is oldest
- Fallback to LRU is applied when K-history is insufficient

---

## 👨‍💻 Author

Hyunsung Ha\
Spring 2025 – Illinois Institute of Technology\
Course: CS525 – Advanced Database Organization
