CC      = cc
CFLAGS  = -std=c11 -Wall -Wextra -Wpedantic -O2

# Pass extra defines via DEFINES, e.g.:
#   make DEFINES=-DBSTACK_FEATURE_SET
DEFINES =

LIB  = libbstack.a
OBJ  = bstack.o

.PHONY: all clean

all: $(LIB)

$(LIB): $(OBJ)
	ar rcs $@ $^

$(OBJ): bstack.c bstack.h
	$(CC) $(CFLAGS) $(DEFINES) -c -o $@ $<

clean:
	rm -f $(OBJ) $(LIB)
