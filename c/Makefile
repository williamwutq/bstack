CC      = cc
CFLAGS  = -std=c11 -Wall -Wextra -Wpedantic -O2

# Pass extra defines via DEFINES, e.g.:
#   make DEFINES=-DBSTACK_FEATURE_SET
DEFINES =

# Windows (MinGW / MSYS2): executables need .exe, run without ./
# Unix: no extension, run with ./
ifeq ($(OS),Windows_NT)
    EXE_EXT =.exe
    RUN     =
else
    EXE_EXT =
    RUN     = ./
endif

LIB      = libbstack.a
OBJ      = bstack.o
TEST_BIN = test_bstack$(EXE_EXT)
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
	$(RUN)$(TEST_BIN)

# Leak detection — macOS requires Xcode developer tools; Linux uses Valgrind.
# Usage: make leaks  (or: make leaks DEFINES=-DBSTACK_FEATURE_SET)
ifneq ($(OS),Windows_NT)
leaks: $(TEST_BIN)
	@if [ "$(shell uname -s)" = "Darwin" ]; then \
		MallocStackLogging=1 leaks --atExit -- $(RUN)$(TEST_BIN); \
	elif [ "$(shell uname -s)" = "Linux" ]; then \
		valgrind --leak-check=full --show-leak-kinds=all $(RUN)$(TEST_BIN); \
	else \
		echo "Leak detection not supported on this platform"; \
		exit 1; \
	fi
endif

clean:
	rm -f $(OBJ) $(TEST_OBJ) $(LIB) $(TEST_BIN)
