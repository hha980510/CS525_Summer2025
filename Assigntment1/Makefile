
CC = gcc
CFLAGS = -Wall -g

SRC = store_mgr.c dberror.c
HDR = storage_mgr.h dberror.h test_helper.h

TEST1 = test_assign1_1.c
TEST2 = test_assign1_extra.c

.PHONY: all clean test test-extra

all: test1 test2

test1: $(SRC) $(TEST1) $(HDR)
	$(CC) $(CFLAGS) -o test1 $(TEST1) $(SRC)

test2: $(SRC) $(TEST2) $(HDR)
	$(CC) $(CFLAGS) -o test2 $(TEST2) $(SRC)

test: test1
	./test1

test-extra: test2
	./test2

clean:
	rm -f test1 test2 *.bin
