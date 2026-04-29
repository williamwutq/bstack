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

# Set-enabled library — required by the hashmap example.
LIB_SET  = libbstack-set.a
OBJ_SET  = bstack-set.o

# Atomic-enabled library.
LIB_ATOMIC  = libbstack-atomic.a
OBJ_ATOMIC  = bstack-atomic.o

# Set + Atomic library.
LIB_SET_ATOMIC  = libbstack-set-atomic.a
OBJ_SET_ATOMIC  = bstack-set-atomic.o

# Alloc layer (bstack_alloc.h / bstack_alloc.c).
LIB_ALLOC     = libbstack-alloc.a
OBJ_ALLOC     = bstack_alloc.o
LIB_ALLOC_SET = libbstack-alloc-set.a
OBJ_ALLOC_SET = bstack_alloc-set.o

# First-fit allocator test (requires BSTACK_FEATURE_SET).
TEST_FF_BIN = test_first_fit$(EXE_EXT)
TEST_FF_OBJ = test_first_fit.o

EXAMPLE_NAMES = basic buffer_reuse journal reading vec_store
EXAMPLE_BINS  = $(addprefix ../examples/,$(addsuffix $(EXE_EXT),$(EXAMPLE_NAMES)))
HASHMAP_BIN   = ../examples/hashmap$(EXE_EXT)
ATOMIC_BIN    = ../examples/atomic_ops$(EXE_EXT)

.PHONY: all test test-set test-atomic test-set-atomic test-first-fit leaks clean alloc \
        example example-basic example-buffer_reuse example-journal \
        example-reading example-vec_store example-hashmap example-atomic_ops

all: $(LIB)


$(LIB): $(OBJ)
	ar rcs $@ $^

$(OBJ): bstack.c bstack.h
	$(CC) $(CFLAGS) $(DEFINES) -c -o $@ $<

$(OBJ_SET): bstack.c bstack.h
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_SET -c -o $@ $<

$(LIB_SET): $(OBJ_SET)
	ar rcs $@ $^

$(OBJ_ATOMIC): bstack.c bstack.h
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_ATOMIC -c -o $@ $<

$(LIB_ATOMIC): $(OBJ_ATOMIC)
	ar rcs $@ $^

$(OBJ_SET_ATOMIC): bstack.c bstack.h
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_SET -DBSTACK_FEATURE_ATOMIC -c -o $@ $<

$(LIB_SET_ATOMIC): $(OBJ_SET_ATOMIC)
	ar rcs $@ $^

$(OBJ_ALLOC): bstack_alloc.c bstack_alloc.h bstack.h
	$(CC) $(CFLAGS) $(DEFINES) -c -o $@ $<

$(OBJ_ALLOC_SET): bstack_alloc.c bstack_alloc.h bstack.h
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_SET -c -o $@ $<

$(LIB_ALLOC): $(OBJ) $(OBJ_ALLOC)
	ar rcs $@ $^

$(LIB_ALLOC_SET): $(OBJ_SET) $(OBJ_ALLOC_SET)
	ar rcs $@ $^

alloc: $(LIB_ALLOC)

$(TEST_OBJ): test_bstack.c bstack.h
	$(CC) $(CFLAGS) $(DEFINES) -c -o $@ $<

$(TEST_BIN): $(TEST_OBJ) $(LIB)
	$(CC) $(CFLAGS) $(DEFINES) -o $@ $< -L. -lbstack -lpthread

test: $(TEST_BIN)
	$(RUN)$(TEST_BIN)

$(TEST_FF_OBJ): test_first_fit.c bstack_alloc.h bstack.h
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_SET -c -o $@ $<

$(TEST_FF_BIN): $(TEST_FF_OBJ) $(LIB_ALLOC_SET)
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_SET -o $@ $< -L. -lbstack-alloc-set -lpthread

test-first-fit: $(TEST_FF_BIN)
	$(RUN)$(TEST_FF_BIN)

# Run tests with the set feature enabled.
test-set: $(LIB_SET)
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_SET -c -o $(TEST_OBJ) test_bstack.c
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_SET -o $(TEST_BIN) $(TEST_OBJ) -L. -lbstack-set -lpthread
	$(RUN)$(TEST_BIN)

# Run tests with the atomic feature enabled.
test-atomic: $(LIB_ATOMIC)
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_ATOMIC -c -o $(TEST_OBJ) test_bstack.c
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_ATOMIC -o $(TEST_BIN) $(TEST_OBJ) -L. -lbstack-atomic -lpthread
	$(RUN)$(TEST_BIN)

# Run tests with both set and atomic features enabled.
test-set-atomic: $(LIB_SET_ATOMIC)
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_SET -DBSTACK_FEATURE_ATOMIC -c -o $(TEST_OBJ) test_bstack.c
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_SET -DBSTACK_FEATURE_ATOMIC -o $(TEST_BIN) $(TEST_OBJ) -L. -lbstack-set-atomic -lpthread
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

# ── Examples ──────────────────────────────────────────────────────────────────
# Examples write .bstack files into examples/ next to the source.

../examples/basic$(EXE_EXT): ../examples/basic.c $(LIB)
	$(CC) $(CFLAGS) -I. -o $@ $< -L. -lbstack -lpthread

../examples/buffer_reuse$(EXE_EXT): ../examples/buffer_reuse.c $(LIB)
	$(CC) $(CFLAGS) -I. -o $@ $< -L. -lbstack -lpthread

../examples/journal$(EXE_EXT): ../examples/journal.c $(LIB)
	$(CC) $(CFLAGS) -I. -o $@ $< -L. -lbstack -lpthread

../examples/reading$(EXE_EXT): ../examples/reading.c $(LIB)
	$(CC) $(CFLAGS) -I. -o $@ $< -L. -lbstack -lpthread

../examples/vec_store$(EXE_EXT): ../examples/vec_store.c $(LIB)
	$(CC) $(CFLAGS) -I. -o $@ $< -L. -lbstack -lpthread

../examples/hashmap$(EXE_EXT): ../examples/hashmap.c $(LIB_SET)
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_SET -I. -o $@ $< -L. -lbstack-set -lpthread

../examples/atomic_ops$(EXE_EXT): ../examples/atomic_ops.c $(LIB_SET_ATOMIC)
	$(CC) $(CFLAGS) -DBSTACK_FEATURE_SET -DBSTACK_FEATURE_ATOMIC -I. -o $@ $< -L. -lbstack-set-atomic -lpthread

example-basic: ../examples/basic$(EXE_EXT)
	(cd ../examples && $(RUN)basic$(EXE_EXT))

example-buffer_reuse: ../examples/buffer_reuse$(EXE_EXT)
	(cd ../examples && $(RUN)buffer_reuse$(EXE_EXT))

example-journal: ../examples/journal$(EXE_EXT)
	(cd ../examples && $(RUN)journal$(EXE_EXT))

example-reading: ../examples/reading$(EXE_EXT)
	(cd ../examples && $(RUN)reading$(EXE_EXT))

example-vec_store: ../examples/vec_store$(EXE_EXT)
	(cd ../examples && $(RUN)vec_store$(EXE_EXT))

example-hashmap: ../examples/hashmap$(EXE_EXT)
	(cd ../examples && $(RUN)hashmap$(EXE_EXT))

example-atomic_ops: ../examples/atomic_ops$(EXE_EXT)
	(cd ../examples && $(RUN)atomic_ops$(EXE_EXT))

example: $(EXAMPLE_BINS) $(HASHMAP_BIN) $(ATOMIC_BIN)
	@echo "==> basic"        && (cd ../examples && $(RUN)basic$(EXE_EXT))
	@echo "==> buffer_reuse" && (cd ../examples && $(RUN)buffer_reuse$(EXE_EXT))
	@echo "==> journal"      && (cd ../examples && $(RUN)journal$(EXE_EXT))
	@echo "==> reading"      && (cd ../examples && $(RUN)reading$(EXE_EXT))
	@echo "==> vec_store"    && (cd ../examples && $(RUN)vec_store$(EXE_EXT))
	@echo "==> hashmap"      && (cd ../examples && $(RUN)hashmap$(EXE_EXT))
	@echo "==> atomic_ops"   && (cd ../examples && $(RUN)atomic_ops$(EXE_EXT))

clean:
	rm -f $(OBJ) $(TEST_OBJ) $(OBJ_SET) $(LIB) $(LIB_SET) $(TEST_BIN) \
	      $(OBJ_ATOMIC) $(LIB_ATOMIC) $(OBJ_SET_ATOMIC) $(LIB_SET_ATOMIC) \
	      $(OBJ_ALLOC) $(OBJ_ALLOC_SET) $(LIB_ALLOC) $(LIB_ALLOC_SET) \
	      $(TEST_FF_OBJ) $(TEST_FF_BIN) \
	      $(EXAMPLE_BINS) $(HASHMAP_BIN) $(ATOMIC_BIN)
