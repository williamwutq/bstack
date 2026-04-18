# bstack cross-platform release builder
#
# Requires:
#   cargo-zigbuild  — cargo install cargo-zigbuild
#   clang / llvm-ar — https://llvm.org (clang for C cross-compilation)
#
# Rust targets must be added beforehand:
#   rustup target add x86_64-apple-darwin aarch64-apple-darwin \
#       x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu \
#       x86_64-unknown-linux-musl aarch64-unknown-linux-musl
#
# Usage:
#   make release                         build all Rust + C targets
#   make rust                            Rust targets only
#   make c                               C targets only
#   make rust-aarch64-apple-darwin       single Rust target
#   make c-x86_64-unknown-linux-musl     single C target
#   make test                            run local test suites
#   make clean                           remove build/ and target/

BUILD   := build
C_SRC   := c/bstack.c
C_INC   := c
C_FLAGS := -std=c11 -O2

RUST_TARGETS := \
    x86_64-apple-darwin \
    aarch64-apple-darwin \
    x86_64-unknown-linux-gnu \
    aarch64-unknown-linux-gnu \
    x86_64-unknown-linux-musl \
    aarch64-unknown-linux-musl

# Map each Rust target triple to its clang --target triple.
# Variable names use _ instead of - so Make can look them up.
CLANG_x86_64_apple_darwin        := x86_64-apple-darwin
CLANG_aarch64_apple_darwin       := aarch64-apple-darwin
CLANG_x86_64_unknown_linux_gnu   := x86_64-linux-gnu
CLANG_aarch64_unknown_linux_gnu  := aarch64-linux-gnu
CLANG_x86_64_unknown_linux_musl  := x86_64-linux-musl
CLANG_aarch64_unknown_linux_musl := aarch64-linux-musl

# Resolve Rust triple → clang triple by substituting - → _ then doing a variable lookup.
clang_triple = $(CLANG_$(subst -,_,$(1)))

RUST_PHONY := $(addprefix rust-,$(RUST_TARGETS))
C_PHONY    := $(addprefix c-,$(RUST_TARGETS))

.PHONY: all release rust c test clean $(RUST_PHONY) $(C_PHONY)

all: release

release: rust c

rust: $(RUST_PHONY)

c: $(C_PHONY)

# ------------------------------------------------------------------------------
# Rust — cargo zigbuild, feature-off and feature-on variants
# Output: build/rust/<target>/libbstack.a
#         build/rust/<target>/libbstack-set.a
# ------------------------------------------------------------------------------
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

# ------------------------------------------------------------------------------
# C — clang cross-compilation, feature-off and feature-on variants
# Output: build/c/<target>/libbstack.a
#         build/c/<target>/libbstack-set.a
#         build/c/<target>/bstack.h
# ------------------------------------------------------------------------------
define c_rule
c-$(1):
	@echo "==> c $(1)  [clang target: $(call clang_triple,$(1))]"
	@mkdir -p $(BUILD)/c/$(1)
	cp $(C_INC)/bstack.h $(BUILD)/c/$(1)/bstack.h
	clang --target=$(call clang_triple,$(1)) $(C_FLAGS) \
	    -I $(C_INC) -c -o $(BUILD)/c/$(1)/bstack.o $(C_SRC)
	llvm-ar rcs $(BUILD)/c/$(1)/libbstack.a $(BUILD)/c/$(1)/bstack.o
	clang --target=$(call clang_triple,$(1)) $(C_FLAGS) -DBSTACK_FEATURE_SET \
	    -I $(C_INC) -c -o $(BUILD)/c/$(1)/bstack-set.o $(C_SRC)
	llvm-ar rcs $(BUILD)/c/$(1)/libbstack-set.a $(BUILD)/c/$(1)/bstack-set.o
endef

$(foreach t,$(RUST_TARGETS),$(eval $(call c_rule,$(t))))

# ------------------------------------------------------------------------------
# Local tests (native only)
# ------------------------------------------------------------------------------
test:
	cargo test
	$(MAKE) -C c test
	$(MAKE) -C c clean
	$(MAKE) -C c test DEFINES=-DBSTACK_FEATURE_SET
	$(MAKE) -C c clean

# ------------------------------------------------------------------------------
clean:
	rm -rf $(BUILD) target
	$(MAKE) -C c clean
