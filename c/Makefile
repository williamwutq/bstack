CC      = cc
CFLAGS  = -std=c11 -Wall -Wextra -Wpedantic -O2

# Pass extra defines via DEFINES, e.g.:
#   make DEFINES=-DBSTACK_FEATURE_SET
DEFINES =

LIB      = libbstack.a
OBJ      = bstack.o
TEST_BIN = test_bstack
TEST_OBJ = test_bstack.o

.PHONY: all test leaks clean

all: $(LIB)

$(LIB): $(OBJ)
	ar rcs $@ $^

$(OBJ): bstack.c bstack.h
	$(CC) $(CFLAGS) $(DEFINES) -c -o $@ $<

$(TEST_OBJ): test_bstack.c bstack.h
	$(CC) $(CFLAGS) $(DEFINES) -c -o $@ $<

$(TEST_BIN): $(TEST_OBJ) $(LIB)
	$(CC) $(CFLAGS) $(DEFINES) -o $@ $< -L. -lbstack -lpthread

test: $(TEST_BIN)
	./$(TEST_BIN)

# macOS leak detection — requires Xcode developer tools.
# Usage: make leaks  (or: make leaks DEFINES=-DBSTACK_FEATURE_SET)
leaks: $(TEST_BIN)
	MallocStackLogging=1 leaks --atExit -- ./$(TEST_BIN)

clean:
	rm -f $(OBJ) $(TEST_OBJ) $(LIB) $(TEST_BIN)
