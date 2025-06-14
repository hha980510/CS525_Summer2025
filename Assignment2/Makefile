# Compiler
CC = gcc
CFLAGS = -Wall -Wextra -std=c99

# Source files
SRCS = buffer_mgr.c buffer_mgr_stat.c storage_mgr.c dberror.c
HDRS = buffer_mgr.h buffer_mgr_stat.h storage_mgr.h dberror.h test_helper.h

# Targets
all: test1 test2

test1: test_assign2_1.c $(SRCS) $(HDRS)
	$(CC) $(CFLAGS) -o test1 test_assign2_1.c $(SRCS) -lm

test2: test_assign2_2.c $(SRCS) $(HDRS)
	$(CC) $(CFLAGS) -o test2 test_assign2_2.c $(SRCS) -lm

run1:
	./test1

run2:
	./test2

clean:
	rm -f test1 test2 *.o