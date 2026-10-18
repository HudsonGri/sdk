use super::*;

fn block_on<F: std::future::Future>(future: F) -> F::Output {
    loom::future::block_on(Box::pin(future))
}

fn publication_model<const OMIT_LOOKUP: bool>() {
    loom::model(|| {
        let client = Arc::new(MemoryStream::default());
        let coordinator = block_on(open(&client)).unwrap();
        // The host's production gate orders refresh, retry lookup and mutation.
        let shared = loom::sync::Arc::new(crate::sync::Mutex::new(coordinator));
        let task = OperationId::from_bytes([41; 16]);
        let declaration = spec(task, 0).unwrap();
        let writer = {
            let shared = loom::sync::Arc::clone(&shared);
            let declaration = declaration.clone();
            loom::thread::spawn(move || {
                block_on(async {
                    let mut coordinator = shared.lock().await;
                    declare(&mut coordinator, declaration, "task")
                        .await
                        .unwrap();
                });
            })
        };
        let reader = {
            let shared = loom::sync::Arc::clone(&shared);
            let declaration = declaration.clone();
            loom::thread::spawn(move || {
                block_on(async {
                    let (owner, issuer) = owner_issuer();
                    let scope = grant(&issuer, "observe", &["operation:observe"]).unwrap();
                    let mut coordinator = shared.lock().await;
                    let observed = coordinator
                        .observe_declaration(
                            &owner,
                            &scope,
                            &issuer.verifier(&mut None).unwrap(),
                            task,
                            &IdempotencyKey::new("task").unwrap(),
                            &mut None,
                        )
                        .await
                        .unwrap();
                    if let Some(observed) = observed {
                        assert_eq!(observed, declaration);
                    }
                });
            })
        };
        writer.join().unwrap();
        reader.join().unwrap();
        struct Restore;
        impl Drop for Restore {
            fn drop(&mut self) {
                intent_index::set_omit_index_lookup(false);
            }
        }
        let _restore = Restore;
        intent_index::set_omit_index_lookup(OMIT_LOOKUP);
        let (owner, issuer) = owner_issuer();
        let scope = grant(&issuer, "observe", &["operation:observe"]).unwrap();
        let observed = block_on(async {
            let mut coordinator = shared.lock().await;
            coordinator
                .observe_declaration(
                    &owner,
                    &scope,
                    &issuer.verifier(&mut None).unwrap(),
                    task,
                    &IdempotencyKey::new("task").unwrap(),
                    &mut None,
                )
                .await
        })
        .unwrap();
        assert_eq!(
            observed,
            Some(declaration),
            "committed declaration disappeared"
        );
    });
}

#[test]
fn retry_lookup_and_publication_share_the_host_gate() {
    publication_model::<false>();
}

#[test]
#[should_panic(expected = "committed declaration disappeared")]
fn omitted_production_lookup_fails_the_same_publication_model() {
    publication_model::<true>();
}
