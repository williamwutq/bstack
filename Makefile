# bstack cross-platform release builder
#
# Requires:
#   cargo-zigbuild   — cargo install cargo-zigbuild
#   C toolchain (CC, default clang):
#     clang  — needs llvm-ar;  cross-targets via --target=<triple>
#     zig cc — needs zig;      cross-targets via -target <zig-triple>
#     gcc    — needs cross-compiler suite; cross-targets via <prefix>-gcc
#
# Rust targets must be added beforehand:
#   rustup target add x86_64-apple-darwin aarch64-apple-darwin \
#       x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu \
#       x86_64-unknown-linux-musl aarch64-unknown-linux-musl
#
# Usage:
#   make release                           default (CC=clang)
#   make release CC="zig cc"              use zig cc for C targets
#   make release CC=gcc                   use gcc cross-compiler suite
#   make rust                             Rust targets only
#   make c                                C targets only
#   make rust-aarch64-apple-darwin        single Rust target
#   make c-x86_64-unknown-linux-musl      single C target
#   make test                             run local test suites
#   make clean                            remove build/ and target/

BUILD   := build
C_SRC   := c/bstack.c
C_INC   := c
C_FLAGS := -std=c11 -O2

# ── Compiler selection ────────────────────────────────────────────────────────
# Override from the command line: make CC="zig cc"  or  make CC=gcc
CC ?= clang

# Detect compiler family from the CC value.
ifneq ($(findstring zig,$(CC)),)
  _CC_FAMILY := zig
else ifneq ($(findstring clang,$(CC)),)
  _CC_FAMILY := clang
else ifneq ($(findstring gcc,$(CC)),)
  _CC_FAMILY := gcc
else
  _CC_FAMILY := clang
endif

# Default archiver per family (user can override: make AR=my-ar)
ifeq ($(_CC_FAMILY),zig)
  AR = zig ar
else ifeq ($(_CC_FAMILY),gcc)
  AR = ar
else
  AR = llvm-ar
endif

# ── Target triple tables ──────────────────────────────────────────────────────
# Variable names use _ instead of - so Make can index them by target name.

# clang: --target=<triple>
CLANG_x86_64_apple_darwin        := x86_64-apple-darwin
CLANG_aarch64_apple_darwin       := aarch64-apple-darwin
CLANG_x86_64_unknown_linux_gnu   := x86_64-linux-gnu
CLANG_aarch64_unknown_linux_gnu  := aarch64-linux-gnu
CLANG_x86_64_unknown_linux_musl  := x86_64-linux-musl
CLANG_aarch64_unknown_linux_musl := aarch64-linux-musl
clang_triple = $(CLANG_$(subst -,_,$(1)))

# zig cc: -target <triple>
ZIG_x86_64_apple_darwin        := x86_64-macos-none
ZIG_aarch64_apple_darwin       := aarch64-macos-none
ZIG_x86_64_unknown_linux_gnu   := x86_64-linux-gnu
ZIG_aarch64_unknown_linux_gnu  := aarch64-linux-gnu
ZIG_x86_64_unknown_linux_musl  := x86_64-linux-musl
ZIG_aarch64_unknown_linux_musl := aarch64-linux-musl
zig_triple = $(ZIG_$(subst -,_,$(1)))

# gcc: cross-compiler prefix (e.g. x86_64-linux-gnu → x86_64-linux-gnu-gcc)
# macOS targets have no standard GCC cross-compiler prefix.
GCCPFX_x86_64_apple_darwin        :=
GCCPFX_aarch64_apple_darwin       :=
GCCPFX_x86_64_unknown_linux_gnu   := x86_64-linux-gnu
GCCPFX_aarch64_unknown_linux_gnu  := aarch64-linux-gnu
GCCPFX_x86_64_unknown_linux_musl  := x86_64-linux-musl
GCCPFX_aarch64_unknown_linux_musl := aarch64-linux-musl
gcc_prefix = $(GCCPFX_$(subst -,_,$(1)))

# ── Per-target compiler and archiver commands ─────────────────────────────────
# cc_for(target)  — full compiler invocation including target flags
# ar_for(target)  — archiver (gcc cross needs a prefixed ar)

cc_for = $(if $(filter zig,$(_CC_FAMILY)),\
           $(CC) -target $(call zig_triple,$(1)),\
           $(if $(filter gcc,$(_CC_FAMILY)),\
             $(if $(call gcc_prefix,$(1)),$(call gcc_prefix,$(1))-gcc,gcc),\
             $(CC) --target=$(call clang_triple,$(1))))

ar_for = $(if $(filter gcc,$(_CC_FAMILY)),\
           $(if $(call gcc_prefix,$(1)),$(call gcc_prefix,$(1))-ar,$(AR)),\
           $(AR))

# ── Phony target lists ────────────────────────────────────────────────────────
RUST_TARGETS := \
    x86_64-apple-darwin \
    aarch64-apple-darwin \
    x86_64-unknown-linux-gnu \
    aarch64-unknown-linux-gnu \
    x86_64-unknown-linux-musl \
    aarch64-unknown-linux-musl

RUST_PHONY := $(addprefix rust-,$(RUST_TARGETS))
C_PHONY    := $(addprefix c-,$(RUST_TARGETS))

.PHONY: all release rust c test clean $(RUST_PHONY) $(C_PHONY)

all: release

release: rust c

rust: $(RUST_PHONY)

c: $(C_PHONY)

# ── Rust — cargo zigbuild ─────────────────────────────────────────────────────
# Output: build/rust/<target>/libbstack.rlib
#         build/rust/<target>/libbstack-set.rlib
define rust_rule
rust-$(1):
	@echo "==> rust $(1)"
	@mkdir -p $(BUILD)/rust/$(1)
	cargo zigbuild --target $(1) --release
	cp target/$(1)/release/libbstack.rlib $(BUILD)/rust/$(1)/libbstack.rlib
	cargo zigbuild --target $(1) --release --features set
	cp target/$(1)/release/libbstack.rlib $(BUILD)/rust/$(1)/libbstack-set.rlib
endef

$(foreach t,$(RUST_TARGETS),$(eval $(call rust_rule,$(t))))

# ── C — cross-compilation ─────────────────────────────────────────────────────
# Output: build/c/<target>/libbstack.a
#         build/c/<target>/libbstack-set.a
#         build/c/<target>/bstack.h
define c_rule
c-$(1):
	@echo "==> c $(1)  [$(_CC_FAMILY): $(call cc_for,$(1))]"
	@mkdir -p $(BUILD)/c/$(1)
	cp $(C_INC)/bstack.h $(BUILD)/c/$(1)/bstack.h
	$(call cc_for,$(1)) $(C_FLAGS) \
	    -I $(C_INC) -c -o $(BUILD)/c/$(1)/bstack.o $(C_SRC)
	$(call ar_for,$(1)) rcs $(BUILD)/c/$(1)/libbstack.a $(BUILD)/c/$(1)/bstack.o
	$(call cc_for,$(1)) $(C_FLAGS) -DBSTACK_FEATURE_SET \
	    -I $(C_INC) -c -o $(BUILD)/c/$(1)/bstack-set.o $(C_SRC)
	$(call ar_for,$(1)) rcs $(BUILD)/c/$(1)/libbstack-set.a $(BUILD)/c/$(1)/bstack-set.o
endef

$(foreach t,$(RUST_TARGETS),$(eval $(call c_rule,$(t))))

# ── Local tests (native only) ─────────────────────────────────────────────────
test:
	cargo test
	$(MAKE) -C c clean
	$(MAKE) -C c test
	$(MAKE) -C c clean
	$(MAKE) -C c test DEFINES=-DBSTACK_FEATURE_SET
	$(MAKE) -C c clean

# ── Clean ─────────────────────────────────────────────────────────────────────
clean:
	rm -rf $(BUILD) target
	$(MAKE) -C c clean
