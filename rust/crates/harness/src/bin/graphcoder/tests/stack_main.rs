//! Stack budgets run on a process main thread, outside libtest worker threads.
#[path = "../args.rs"]
mod args;
#[path = "../entry.rs"]
mod entry;
#[path = "../stack_budget.rs"]
mod stack_budget;
#[path = "../view.rs"]
mod view;
fn main() {
    stack_budget::qualify();
}
