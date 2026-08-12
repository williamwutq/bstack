//! # A persistent expression tree (`bstack_raii`)
//!
//! This example exercises the object model end to end within a single file: a
//! recursive `#[bstack_enum]`, owned children, a deep clone (`TryCloneIn`), and
//! a structural move-out (`bstack_move!`) — all backed by the on-disk allocator.
//!
//! We build the arithmetic expression `(3 + 4) * 2`, evaluate it by walking the
//! persisted tree, deep-clone the whole thing into independent storage, mutate
//! the clone, and confirm the original is untouched.
//!
//! Run with: `cargo run --example expr`

use std::io;

use bstack::FirstFitBStackAllocator;
use bstack_raii::{
    BStack, BStackAllocator, BStackDrop, BStackOwned, TryCloneIn, bstack_block, bstack_enum,
    bstack_move,
};

/// A binary operation node: an operator byte and two owned operand sub-trees.
/// Being a named block lets `Expr` recurse through it (a block is referenced by
/// offset, so the layout stays fixed-size).
#[bstack_block]
struct BinOp {
    op: u8, // b'+' or b'*'
    #[bstack_owned]
    lhs: Expr,
    #[bstack_owned]
    rhs: Expr,
}

/// An expression is either a literal or an owned binary operation.
#[bstack_enum]
enum Expr {
    Lit(i64),
    #[bstack_owned]
    Op(BinOp),
}

/// Evaluate an expression by walking the on-disk tree.
fn eval(expr: &Expr, alloc: &FirstFitBStackAllocator) -> io::Result<i64> {
    Ok(match expr.read(alloc)? {
        ExprView::Lit(v) => v,
        ExprView::Op(node) => {
            let lhs = eval(&node.get_lhs(alloc.stack())?, alloc)?;
            let rhs = eval(&node.get_rhs(alloc.stack())?, alloc)?;
            match node.get_op(alloc.stack())? {
                b'+' => lhs + rhs,
                b'*' => lhs * rhs,
                other => unreachable!("unknown operator {other}"),
            }
        }
    })
}

/// Render an expression back to source text.
fn render(expr: &Expr, alloc: &FirstFitBStackAllocator) -> io::Result<String> {
    Ok(match expr.read(alloc)? {
        ExprView::Lit(v) => v.to_string(),
        ExprView::Op(node) => {
            let lhs = render(&node.get_lhs(alloc.stack())?, alloc)?;
            let rhs = render(&node.get_rhs(alloc.stack())?, alloc)?;
            let op = node.get_op(alloc.stack())? as char;
            format!("({lhs} {op} {rhs})")
        }
    })
}

/// Build a literal expression node.
fn lit(alloc: &FirstFitBStackAllocator, v: i64) -> io::Result<BStackOwned<Expr>> {
    Expr::new(alloc, ExprData::Lit(v))
}

/// Build a binary-operation expression node from two owned operands.
fn op(
    alloc: &FirstFitBStackAllocator,
    operator: u8,
    lhs: BStackOwned<Expr>,
    rhs: BStackOwned<Expr>,
) -> io::Result<BStackOwned<Expr>> {
    let node = BinOp::new(alloc, operator, lhs, rhs)?;
    Expr::new(alloc, ExprData::Op(node))
}

fn main() -> io::Result<()> {
    let path = std::env::temp_dir().join("bstack_raii_expr.bstack");
    let _ = std::fs::remove_file(&path);
    let alloc = FirstFitBStackAllocator::new(BStack::open(&path)?)?;

    // Build `(3 + 4) * 2` bottom-up. Each `new` consumes the owned children it
    // takes, wiring the tree together on disk.
    let sum = op(&alloc, b'+', lit(&alloc, 3)?, lit(&alloc, 4)?)?;
    let root = op(&alloc, b'*', sum, lit(&alloc, 2)?)?;

    println!("expression: {}", render(&root, &alloc)?);
    println!("evaluates to: {}", eval(&root, &alloc)?);
    assert_eq!(eval(&root, &alloc)?, 14);

    // Deep-clone the whole tree into fresh, independent storage. Every owned
    // child is recursively duplicated, so the copy shares nothing with `root`.
    let clone = root.try_clone_in(&alloc)?;
    assert_eq!(eval(&clone, &alloc)?, 14);
    println!(
        "deep clone evaluates to: {} (independent copy)",
        eval(&clone, &alloc)?
    );

    // Destructure the clone with `bstack_move!`: the enum shell is freed and the
    // active variant is handed back through `ExprData`. The top node was `Op`, so
    // we get the owning `BinOp` back — its children are still live on disk.
    match bstack_move!(clone, &alloc)? {
        ExprData::Op(binop) => {
            // The moved-out `BinOp` still owns the multiplication's operands.
            let left = binop.handle().get_lhs(alloc.stack())?;
            println!(
                "moved out the root `Op`; its left operand is `{}`",
                render(&left, &alloc)?
            );
            // We now own this subtree explicitly; free it (and its children).
            binop.bstack_drop(&alloc)?;
        }
        ExprData::Lit(_) => unreachable!("root was an Op"),
    }

    // The original is entirely untouched by the clone's move + teardown.
    assert_eq!(eval(&root, &alloc)?, 14);
    println!("original still evaluates to: {}", eval(&root, &alloc)?);

    // A uniquely-owned root frees nothing on scope exit — reclaim it explicitly.
    root.bstack_drop(&alloc)?;

    let _ = std::fs::remove_file(&path);
    Ok(())
}
