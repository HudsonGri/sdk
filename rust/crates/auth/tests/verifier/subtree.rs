use super::*;
use acyclic_auth::authority::{Deny, Target};

fn capability(prefix: bool) -> VerifiedCapability {
    let bytes = root(|block| {
        if let Some(certificates) = block.certificates.as_mut() {
            for certificate in &mut certificates.keys {
                certificate.key.grants =
                    signed::claims::grants("streams:/team", prefix, "streams/read");
            }
        }
    });
    verify(&bytes, &Snapshot::default(), NOW).unwrap()
}

#[test]
fn subtree_requires_covering_prefix_in_every_authenticated_grant_set() {
    let trust = Snapshot::default();
    let attributes = RecordAttributes::default();
    let usage = limits(None);
    let mut request = request("streams:/team", &attributes, &usage);
    request.target = Target::Resource {
        action: "streams/read",
        resource: "streams:/team",
    };
    let exact = capability(false);
    assert!(exact.authorize(&trust, &request, PolicyDecision::Allow).is_ok());
    assert_eq!(
        exact.authorize_subtree(&trust, &request, PolicyDecision::Allow),
        Err(VerifyError::Denied(Deny::Capability))
    );
    let covering = capability(true);
    assert!(
        covering
            .authorize_subtree(&trust, &request, PolicyDecision::Allow)
            .is_ok()
    );
    let narrowed = covering
        .attenuate(
            vec![Caveat {
                kind: CaveatKind::Grants(signed::claims::grants(
                    "streams:/team",
                    false,
                    "streams/read",
                )),
            }],
            &[8; 32],
        )
        .unwrap();
    assert!(
        narrowed
            .authorize(&trust, &request, PolicyDecision::Allow)
            .is_ok()
    );
    assert_eq!(
        narrowed.authorize_subtree(&trust, &request, PolicyDecision::Allow),
        Err(VerifyError::Denied(Deny::Capability))
    );
    request.target = Target::Resource {
        action: "streams/read",
        resource: "streams:/team-other",
    };
    assert!(
        covering
            .authorize_subtree(&trust, &request, PolicyDecision::Allow)
            .is_err()
    );
}

#[test]
fn subtree_retains_owner_trust_expiry_and_record_predicates() {
    let mut trust = Snapshot::default();
    let attributes = RecordAttributes::default();
    let usage = limits(None);
    let mut request = request("streams:/team", &attributes, &usage);
    request.target = Target::Resource {
        action: "streams/read",
        resource: "streams:/team",
    };
    let covering = capability(true);
    assert_eq!(
        covering.authorize_subtree(&trust, &request, PolicyDecision::Deny),
        Err(VerifyError::Denied(Deny::OwnerPolicy))
    );
    let empty_filter = covering
        .attenuate(
            vec![Caveat {
                kind: CaveatKind::Filter(Filter {
                    keys: vec![],
                    attributes: vec![],
                }),
            }],
            &[8; 32],
        )
        .unwrap();
    assert!(
        empty_filter
            .authorize_subtree(&trust, &request, PolicyDecision::Allow)
            .is_ok()
    );
    let pinned = covering
        .attenuate(
            vec![Caveat {
                kind: CaveatKind::Pin(Pin {
                    name: "owner".into(),
                    value: PinValue::Literal(b"account".to_vec()),
                }),
            }],
            &[9; 32],
        )
        .unwrap();
    assert!(
        pinned
            .authorize_subtree(&trust, &request, PolicyDecision::Allow)
            .is_err()
    );
    trust.revoked_keys.push("key".into());
    assert_eq!(
        covering.authorize_subtree(&trust, &request, PolicyDecision::Allow),
        Err(VerifyError::Revoked)
    );
    trust.revoked_keys.clear();
    request.now_unix_seconds = 1000;
    assert_eq!(
        covering.authorize_subtree(&trust, &request, PolicyDecision::Allow),
        Err(VerifyError::Time)
    );
}
