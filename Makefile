# bstack cross-platform release builder
#
# Requires:
#   cargo-zigbuild   — cargo install cargo-zigbuild
#   C toolchain (CC, default gcc on Unix):
#     clang  — needs llvm-ar;  cross-targets via --target=<triple>
#     zig cc — needs zig;      cross-targets via -target <zig-triple>
#     gcc    — needs cross-compiler suite; cross-targets via <prefix>-gcc
#
# Rust targets must be added beforehand:
#   rustup target add x86_64-apple-darwin aarch64-apple-darwin \
#       x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu \
#       x86_64-unknown-linux-musl aarch64-unknown-linux-musl \
#       x86_64-pc-windows-gnu aarch64-pc-windows-gnu \
#       x86_64-pc-windows-gnu aarch64-pc-windows-gnu
#
# Usage:
#   make release                           default (CC=gcc on Unix)
#   make release CC="zig cc"              use zig cc for C targets
#   make release CC=clang                 use clang for C targets
#   make rust                             Rust targets only
#   make c                                C targets only
#   make rust-aarch64-apple-darwin        single Rust target
#   make c-x86_64-unknown-linux-musl      single C target
#   make test                             run local test suites
#   make clean                            remove build/ and target/

BUILD        := build
C_SRC        := c/bstack.c
C_ALLOC_SRC  := c/bstack_alloc.c
C_INC        := c
C_FLAGS      := -std=c11 -O2

# ── Platform detection ───────────────────────────────────────────────────────
HOST_OS := $(shell uname -s)

# ── Compiler selection ────────────────────────────────────────────────────────
# Override from the command line: make CC="zig cc"  or  make CC=clang
# Default to gcc on Unix-like systems
ifneq ($(findstring MINGW,$(HOST_OS)),)
  CC ?= gcc
else ifneq ($(findstring CYGWIN,$(HOST_OS)),)
  CC ?= gcc
else ifneq ($(findstring Windows,$(HOST_OS)),)
  CC ?= gcc
else
  CC ?= gcc
endif

# Detect compiler family from the CC value.
ifneq ($(findstring zig,$(CC)),)
  _CC_FAMILY := zig
else ifneq ($(findstring clang,$(CC)),)
  _CC_FAMILY := clang
else ifneq ($(findstring gcc,$(CC)),)
  _CC_FAMILY := gcc
else
  _CC_FAMILY := gcc
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
CLANG_x86_64_pc_windows_gnu      := x86_64-w64-windows-gnu
CLANG_aarch64_pc_windows_gnullvm := aarch64-w64-windows-gnullvm
clang_triple = $(CLANG_$(subst -,_,$(1)))

# zig cc: -target <triple>
ZIG_x86_64_apple_darwin        := x86_64-macos-none
ZIG_aarch64_apple_darwin       := aarch64-macos-none
ZIG_x86_64_unknown_linux_gnu   := x86_64-linux-gnu
ZIG_aarch64_unknown_linux_gnu  := aarch64-linux-gnu
ZIG_x86_64_unknown_linux_musl  := x86_64-linux-musl
ZIG_aarch64_unknown_linux_musl := aarch64-linux-musl
ZIG_x86_64_pc_windows_gnu      := x86_64-windows-gnu
ZIG_aarch64_pc_windows_gnullmv := aarch64-windows-gnullvm
zig_triple = $(ZIG_$(subst -,_,$(1)))

# gcc: cross-compiler prefix (e.g. x86_64-linux-gnu → x86_64-linux-gnu-gcc)
# macOS targets have no standard GCC cross-compiler prefix.
GCCPFX_x86_64_apple_darwin        :=
GCCPFX_aarch64_apple_darwin       :=
GCCPFX_x86_64_unknown_linux_gnu   := x86_64-linux-gnu
GCCPFX_aarch64_unknown_linux_gnu  := aarch64-linux-gnu
GCCPFX_x86_64_unknown_linux_musl  := x86_64-linux-musl
GCCPFX_aarch64_unknown_linux_musl := aarch64-linux-musl
GCCPFX_x86_64_pc_windows_gnu      := x86_64-w64-mingw32
GCCPFX_aarch64_pc_windows_gnullvv := aarch64-w64-mingw32
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
    aarch64-unknown-linux-musl \
	x86_64-pc-windows-gnu \
	aarch64-pc-windows-gnullvm

RUST_PHONY := $(addprefix rust-,$(RUST_TARGETS))
C_PHONY    := $(addprefix c-,$(RUST_TARGETS))

.PHONY: all release rust c test clean zip help $(RUST_PHONY) $(C_PHONY)

help:
	@echo 'Usage:'
	@echo '  make release                           build all Rust + C targets (default)'
	@echo '  make release CC="zig cc"               use zig cc for C cross-compilation'
	@echo '  make release CC=clang                  use clang for C cross-compilation'
	@echo ''
	@echo 'Selective build:'
	@echo '  make rust                              all Rust targets'
	@echo '  make c                                 all C targets'
	@echo '  make rust-<target>                     single Rust target, e.g.:'
	@echo '                                           rust-aarch64-apple-darwin'
	@echo '                                           rust-x86_64-unknown-linux-musl'
	@echo '  make c-<target>                        single C target (same triples as above)'
	@echo ''
	@echo 'Testing (native only):'
	@echo '  make test                              Rust + all C feature variants'
	@echo ''
	@echo 'Packaging:'
	@echo '  make zip                               archive build/ outputs'
	@echo ''
	@echo 'Cleanup:'
	@echo '  make clean                             remove build/ and Rust target/'
	@echo '  make clean-zip                         remove archives only'
	@echo '  make clean-data                        remove *.bstack data files'
	@echo ''
	@echo 'C feature variants built per target:'
	@echo '  libbstack.a            base'
	@echo '  libbstack-set.a        -DBSTACK_FEATURE_SET'
	@echo '  libbstack-atomic.a     -DBSTACK_FEATURE_ATOMIC'
	@echo '  libbstack-set-atomic.a -DBSTACK_FEATURE_SET -DBSTACK_FEATURE_ATOMIC'
	@echo '  libbstack-alloc.a      base + alloc layer'
	@echo '  libbstack-alloc-set.a  -DBSTACK_FEATURE_SET + alloc layer'

all: release zip

release: rust c

rust: $(RUST_PHONY)

c: $(C_PHONY)

# ── Rust — cargo zigbuild ─────────────────────────────────────────────────────
# Output: $(BUILD)/<target>/rust/libbstack.rlib
#         $(BUILD)/<target>/rust/libbstack-set.rlib
#         $(BUILD)/<target>/rust/libbstack-alloc.rlib
#         $(BUILD)/<target>/rust/libbstack-alloc-set.rlib
#         $(BUILD)/<target>/rust/libbstack-atomic.rlib
#         $(BUILD)/<target>/rust/libbstack-set-atomic.rlib
define rust_rule
rust-$(1):
	@echo "==> rust $(1)"
	@mkdir -p $(BUILD)/$(1)/rust
	cargo zigbuild --target $(1) --release
	cp target/$(1)/release/libbstack.rlib $(BUILD)/$(1)/rust/libbstack.rlib
	cargo zigbuild --target $(1) --release --features set
	cp target/$(1)/release/libbstack.rlib $(BUILD)/$(1)/rust/libbstack-set.rlib
	cargo zigbuild --target $(1) --release --features alloc
	cp target/$(1)/release/libbstack.rlib $(BUILD)/$(1)/rust/libbstack-alloc.rlib
	cargo zigbuild --target $(1) --release --features "alloc,set"
	cp target/$(1)/release/libbstack.rlib $(BUILD)/$(1)/rust/libbstack-alloc-set.rlib
	cargo zigbuild --target $(1) --release --features atomic
	cp target/$(1)/release/libbstack.rlib $(BUILD)/$(1)/rust/libbstack-atomic.rlib
	cargo zigbuild --target $(1) --release --features "set,atomic"
	cp target/$(1)/release/libbstack.rlib $(BUILD)/$(1)/rust/libbstack-set-atomic.rlib
endef

$(foreach t,$(RUST_TARGETS),$(eval $(call rust_rule,$(t))))

# ── C — cross-compilation ─────────────────────────────────────────────────────
# Output: $(BUILD)/<target>/c/libbstack.a
#         $(BUILD)/<target>/c/libbstack-set.a
#         $(BUILD)/<target>/c/libbstack-atomic.a
#         $(BUILD)/<target>/c/libbstack-set-atomic.a
#         $(BUILD)/<target>/c/libbstack-alloc.a
#         $(BUILD)/<target>/c/libbstack-alloc-set.a
#         $(BUILD)/<target>/c/bstack.h
#         $(BUILD)/<target>/c/bstack_alloc.h
define c_rule
c-$(1):
	@echo "==> c $(1)  [$(_CC_FAMILY): $(call cc_for,$(1))]"
	@mkdir -p $(BUILD)/$(1)/c
	cp $(C_INC)/bstack.h       $(BUILD)/$(1)/c/bstack.h
	cp $(C_INC)/bstack_alloc.h $(BUILD)/$(1)/c/bstack_alloc.h
	$(call cc_for,$(1)) $(C_FLAGS) \
	    -I $(C_INC) -c -o $(BUILD)/$(1)/c/bstack.o $(C_SRC)
	$(call ar_for,$(1)) rcs $(BUILD)/$(1)/c/libbstack.a \
	    $(BUILD)/$(1)/c/bstack.o
	$(call cc_for,$(1)) $(C_FLAGS) -DBSTACK_FEATURE_SET \
	    -I $(C_INC) -c -o $(BUILD)/$(1)/c/bstack-set.o $(C_SRC)
	$(call ar_for,$(1)) rcs $(BUILD)/$(1)/c/libbstack-set.a \
	    $(BUILD)/$(1)/c/bstack-set.o
	$(call cc_for,$(1)) $(C_FLAGS) -DBSTACK_FEATURE_ATOMIC \
	    -I $(C_INC) -c -o $(BUILD)/$(1)/c/bstack-atomic.o $(C_SRC)
	$(call ar_for,$(1)) rcs $(BUILD)/$(1)/c/libbstack-atomic.a \
	    $(BUILD)/$(1)/c/bstack-atomic.o
	$(call cc_for,$(1)) $(C_FLAGS) -DBSTACK_FEATURE_SET -DBSTACK_FEATURE_ATOMIC \
	    -I $(C_INC) -c -o $(BUILD)/$(1)/c/bstack-set-atomic.o $(C_SRC)
	$(call ar_for,$(1)) rcs $(BUILD)/$(1)/c/libbstack-set-atomic.a \
	    $(BUILD)/$(1)/c/bstack-set-atomic.o
	$(call cc_for,$(1)) $(C_FLAGS) \
	    -I $(C_INC) -c -o $(BUILD)/$(1)/c/bstack_alloc.o $(C_ALLOC_SRC)
	$(call ar_for,$(1)) rcs $(BUILD)/$(1)/c/libbstack-alloc.a \
	    $(BUILD)/$(1)/c/bstack.o $(BUILD)/$(1)/c/bstack_alloc.o
	$(call cc_for,$(1)) $(C_FLAGS) -DBSTACK_FEATURE_SET \
	    -I $(C_INC) -c -o $(BUILD)/$(1)/c/bstack_alloc-set.o $(C_ALLOC_SRC)
	$(call ar_for,$(1)) rcs $(BUILD)/$(1)/c/libbstack-alloc-set.a \
	    $(BUILD)/$(1)/c/bstack-set.o $(BUILD)/$(1)/c/bstack_alloc-set.o
endef

$(foreach t,$(RUST_TARGETS),$(eval $(call c_rule,$(t))))

# ── Local tests (native only) ─────────────────────────────────────────────────
test:
	cargo test
	cargo test --features set
	cargo test --features "alloc,set"
	$(MAKE) -C c clean
	$(MAKE) -C c test
	$(MAKE) -C c test-set
	$(MAKE) -C c test-atomic
	$(MAKE) -C c test-set-atomic
	$(MAKE) -C c test-first-fit
	$(MAKE) -C c clean

# ── Clean ─────────────────────────────────────────────────────────────────────
clean:
	rm -rf $(BUILD) target $(BUILD)/*.tar.gz $(BUILD)/*.zip
	$(MAKE) -C c clean

clean-zip:
	rm -rf $(BUILD)/*.tar.gz $(BUILD)/*.zip

clean-data:
	rm -rf **/*.bstack

# ── Zip ───────────────────────────────────────────────────────────────────────
zip: $(BUILD)
	@for target in $(RUST_TARGETS); do \
		if [ -d $(BUILD)/$$target ]; then \
			echo "==> zipping $$target"; \
			if [[ $$target == *"windows"* ]]; then \
				zip -r $(BUILD)/$$target.zip $(BUILD)/$$target; \
			else \
				tar -czf $(BUILD)/$$target.tar.gz -C $(BUILD) $$target; \
			fi; \
		fi; \
	done
