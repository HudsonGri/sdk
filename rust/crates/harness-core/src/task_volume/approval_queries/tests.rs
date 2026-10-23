use super::*;
use proptest::prelude::*;
fn component(text: &str) -> Component {
    Component::new(text).unwrap()
}
fn key(task: &Component, file: TaskFile) -> FileKey {
    FileKey::Task {
        task: task.clone(),
        file,
    }
}
fn publish(index: &mut Index, task: &Component, file: TaskFile, bytes: &[u8]) {
    let position = index.next + 1;
    apply(
        index,
        FileValue::new(key(task, file), bytes.to_vec()),
        position,
    );
    index.next = position;
}

#[test]
fn approval_history_cannot_fill_the_session_snapshot() {
    let task = component("task");
    let mut index = Index::default();
    for i in 0..5000 {
        let request = component(&format!("request-{i}"));
        publish(
            &mut index,
            &task,
            TaskFile::ApprovalRequest(request.clone()),
            &[1; 1024],
        );
        publish(
            &mut index,
            &task,
            TaskFile::ApprovalReview {
                request: request.clone(),
                principal: component("human"),
            },
            b"waiting",
        );
        publish(
            &mut index,
            &task,
            TaskFile::ApprovalDecision {
                request,
                principal: component("human"),
            },
            b"allow",
        );
    }
    let pending = component("pending");
    publish(
        &mut index,
        &task,
        TaskFile::ApprovalRequest(pending.clone()),
        b"subject",
    );
    publish(
        &mut index,
        &task,
        TaskFile::ApprovalReview {
            request: pending.clone(),
            principal: component("human"),
        },
        b"waiting",
    );
    let query = Query::TaskSession {
        task: task.clone(),
        session: component("session"),
    };
    let files = recent::snapshot(&index, &query, 8).unwrap();
    assert_eq!(files.len(), 2);
    assert_eq!(files.iter().map(|file| file.bytes.len()).sum::<usize>(), 14);
    // Restoring the historical-copy loop violates both shipping view bounds.
    let archived: Vec<_> = index
        .files
        .range(Query::Approvals(task.clone()).ranges().remove(0))
        .collect();
    assert!(archived.len() > 4096);
    assert!(
        archived
            .iter()
            .map(|(_, file)| file.file.bytes.len())
            .sum::<usize>()
            > 4 * 1024 * 1024
    );
    // Settled history remains addressable through the same canonical keys.
    let exact = Query::Approval {
        task: task.clone(),
        request: component("request-0"),
    };
    assert_eq!(
        Query::folder(".tasks/task/approvals/request-0").unwrap(),
        exact
    );
    assert_eq!(
        exact
            .ranges()
            .into_iter()
            .flat_map(|range| index.files.range(range))
            .filter(|(key, _)| exact.matches(key))
            .count(),
        3
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(16))]
    #[test]
    fn approval_pending_index_retires_only_the_decided_binding(events in prop::collection::vec((0u8..4, any::<bool>()), 1..100)) {
        let task = component("task");
        let other = component("other");
        let mut index = Index::default();
        let mut decided = BTreeSet::new();
        let mut admitted = BTreeSet::new();
        for (id, settle) in events {
            let request = component(&format!("request-{id}"));
            publish(&mut index, &task, TaskFile::ApprovalRequest(request.clone()), b"subject");
            admitted.insert(request.clone());
            if settle {
                publish(&mut index, &task, TaskFile::ApprovalDecision { request: request.clone(), principal: component("human") }, b"deny");
                decided.insert(request.clone());
            }
            // A late/replayed review cannot resurrect a decided request.
            publish(&mut index, &task, TaskFile::ApprovalReview { request: request.clone(), principal: component("human") }, b"waiting");
            publish(&mut index, &other, TaskFile::ApprovalRequest(request), b"other task");
            let actual: BTreeSet<_> = index.approvals.files(&index, &task).into_iter().filter_map(|file| match file.key {
                FileKey::Task { file: TaskFile::ApprovalRequest(id), .. } => Some(id), _ => None,
            }).collect();
            let expected: BTreeSet<_> = admitted.difference(&decided).cloned().collect();
            prop_assert_eq!(&actual, &expected);
            // Negative control: retaining every request differs as soon as a
            // decision occurs; another task's same ID must remain pending.
            prop_assert_eq!(actual == admitted, decided.is_empty());
            prop_assert!(!index.approvals.files(&index, &other).is_empty());
        }
    }
}
