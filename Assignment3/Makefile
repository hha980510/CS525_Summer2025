CC = gcc
CFLAGS = -std=c99 -Wall

OBJS = \
    test_assign3_1.c \
    record_mgr.c \
    buffer_mgr.c \
    buffer_mgr_stat.c \
    dberror.c \
    expr.c \
    rm_serializer.c \
    storage_mgr.c

test: $(OBJS)
	$(CC) $(CFLAGS) -o test_exec $(OBJS)

clean:
	rm -f test_exec
