use super::*;
use crate::Fs;

fn single_disclosure_boundary(source: &str) -> bool {
    source.matches(".finish(").count() + source.matches("::finish(").count() == 1
}

#[test]
fn read_results_have_one_checked_output_boundary() {
    assert!(single_disclosure_boundary(include_str!("../read_core.rs")));
}

#[test]
fn direct_output_bypass_negative_control() {
    let old_exit = format!(
        "{}\nwork.finish(result, accounting)",
        include_str!("../read_core.rs")
    );
    assert!(!single_disclosure_boundary(&old_exit));
}

async fn comparison_peak(
    workspace: &Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>,
    selected: GenerationId,
    original: GenerationId,
    tracked: bool,
    ceiling: u64,
) -> Result<u64, WorkspaceError> {
    let cancellation = CancellationToken::new();
    let mut accounting = KernelAccounting::standalone(
        WorkBudget {
            peak_allocation_bytes: ceiling,
            ..WorkBudget::UNBOUNDED
        },
        &cancellation,
    );
    let (mut selected, path) = workspace
        .open_read_path(
            GenerationSelector::Exact(selected),
            "/entry",
            &mut accounting,
            &cancellation,
        )
        .await?;
    selected
        .value
        .lookup_no_follow_with_accounting(&path.value, &mut accounting, &cancellation, true)
        .await?;
    if tracked {
        let mut original = workspace
            .open_checkout(
                GenerationSelector::Exact(original),
                CheckoutMode::read_only_pinned(),
                &mut accounting,
                &cancellation,
            )
            .await?;
        original
            .value
            .lookup_no_follow_with_accounting(&path.value, &mut accounting, &cancellation, false)
            .await?;
        original.retire(&mut accounting)?;
    } else {
        // Negative control: the old path retained a checkout without reserving it.
        let mut original = workspace
            .checkout_accounted(
                GenerationSelector::Exact(original),
                CheckoutMode::read_only_pinned(),
                &mut accounting,
                &cancellation,
            )
            .await?;
        original
            .lookup_no_follow_with_accounting(&path.value, &mut accounting, &cancellation, false)
            .await?;
    }
    Ok(accounting
        .standalone_parts()
        .ok_or(crate::WorkError::Overflow)?
        .1
        .peak_allocation_bytes)
}

async fn minimum_ceiling(
    workspace: &Workspace<crate::MemoryAuthorityBackend, crate::MemoryObjectBackend>,
    selected: GenerationId,
    original: GenerationId,
    tracked: bool,
    upper: u64,
) -> Result<u64, WorkspaceError> {
    let (mut low, mut high) = (0, upper);
    while low < high {
        let candidate = low + (high - low) / 2;
        if comparison_peak(workspace, selected, original, tracked, candidate)
            .await
            .is_ok()
        {
            high = candidate;
        } else {
            low = candidate + 1;
        }
    }
    comparison_peak(workspace, selected, original, tracked, low).await?;
    Ok(low)
}

#[tokio::test]
async fn cold_original_checkout_counts_against_the_live_memory_ceiling()
-> Result<(), Box<dyn std::error::Error>> {
    let fs = Fs::memory();
    let cancellation = CancellationToken::new();
    let workspace = fs
        .create_workspace(
            "original-checkout-budget",
            WorkBudget::UNBOUNDED,
            &cancellation,
        )
        .await?
        .value;
    workspace.write_text("/entry", "payload").await?;
    let original = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    workspace.write_text("/unrelated", "new generation").await?;
    let selected = workspace
        .head(WorkBudget::UNBOUNDED, &cancellation)
        .await?
        .value;
    let upper = comparison_peak(&workspace, selected.id(), original.id(), true, u64::MAX).await?;
    // Probe/cache capacities adapt to the allowance. Find actual admitted floors,
    // rather than assuming the unbounded peak is a lower bound at smaller budgets.
    let broken = minimum_ceiling(&workspace, selected.id(), original.id(), false, upper).await?;
    let owned = minimum_ceiling(&workspace, selected.id(), original.id(), true, upper).await?;
    assert!(owned > broken, "{owned} <= {broken}");
    let denied = selected
        .read_range_if_unchanged(
            &original,
            "/entry",
            crate::ByteRange {
                offset: 0,
                length: 0,
            },
            WorkBudget {
                peak_allocation_bytes: broken,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .await;
    assert!(
        matches!(&denied, Err(failure) if matches!(&failure.error,
            WorkspaceError::Work(crate::WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", ..
            }) | WorkspaceError::Filesystem(FsError::Work(crate::WorkError::BudgetExceeded {
                counter: "peak_allocation_bytes", ..
            }))
        )),
        "original checkout escaped the live allocation ceiling ({broken} < {owned}): {denied:?}"
    );
    let admitted = selected
        .read_range_if_unchanged(
            &original,
            "/entry",
            crate::ByteRange {
                offset: 0,
                length: 0,
            },
            WorkBudget {
                peak_allocation_bytes: owned,
                ..WorkBudget::UNBOUNDED
            },
            &cancellation,
        )
        .await?;
    assert!(admitted.value.bytes.is_empty());
    assert!(admitted.work.peak_allocation_bytes <= owned);
    Ok(())
}
