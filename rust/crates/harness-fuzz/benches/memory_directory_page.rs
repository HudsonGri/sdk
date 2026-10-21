//! Run the production directory selector's tests and paired allocation profiles.
#[cfg(test)]
acyclic_test_support::counting_allocator!();

#[path = "../../harness-core/src/memory_store/directory/page.rs"]
mod page;
