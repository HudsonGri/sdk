use super::compilation::{
    MAXIMUM_STATE_MEMBERS, MAXIMUM_STATES, MAXIMUM_TEXT_BYTES, MAXIMUM_TRANSITIONS, MAXIMUM_WORK,
    spend,
};
use super::glob::{Nfa, Symbol};
use super::{
    Action, Capability, Context, Decision, Denial, InvalidPolicy, Policy, Principal, Resource,
};
use std::collections::{BTreeMap, BTreeSet};

mod path;
mod reachability;
pub use path::{PinnedPath, PreparedPath};

#[derive(Clone, Copy)]
struct Outcome {
    rule: Option<usize>,
    denial: Option<Denial>,
}

struct State {
    transitions: Vec<usize>,
    decisions: [Outcome; 12],
    actions: u16,
}

/// Determinized glob trie for one principal, volume, generation, and context.
/// Prepare paths once during namespace lookup, then check the resulting handle
/// without scanning names, allocating, or depending on policy size.
pub struct PathTrie {
    alphabet: Alphabet,
    states: Vec<State>,
    pub(crate) rules: std::sync::Arc<[String]>,
    version: u64,
    head: u64,
    pub(crate) generation: u64,
    valid_from: u64,
    valid_until: Option<u64>,
    config: crate::VolumeConfig,
    volume: String,
    subject: (super::PrincipalKind, String),
    capability_binding: [u8; 32],
    principal_binding: [u8; 32],
    pub(crate) policy_revision: Option<(
        crate::GenerationId,
        crate::kernel::MetadataField<crate::ObjectId>,
    )>,
    data_generation: Option<crate::GenerationId>,
    generation_sensitive: bool,
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) native_custody: Option<crate::NativeAllocation>,
    // Last: the shared audit identity funds rule strings until their final owner retires.
    pub(crate) audit: Option<std::sync::Arc<super::audit::Binding>>,
}

struct PathWriter<'a> {
    trie: &'a PathTrie,
    state: usize,
}

impl std::fmt::Write for PathWriter<'_> {
    fn write_str(&mut self, text: &str) -> std::fmt::Result {
        for ch in text.chars() {
            self.state = self
                .trie
                .step(self.state, ch)
                .map_err(|_| std::fmt::Error)?;
        }
        Ok(())
    }
}

// Two-level direct Unicode table: two indexed loads per scalar, no searches.
// The scalar space and the NFA literal limit bound its storage.
struct Alphabet {
    pages: Vec<[u16; 256]>,
    page_ids: Box<[u16; 4352]>,
}

impl Alphabet {
    fn new(symbols: &[char]) -> Result<Self, InvalidPolicy> {
        let other =
            u16::try_from(symbols.len()).map_err(|_| InvalidPolicy("alphabet exceeds limit"))?;
        let mut result = Self {
            pages: vec![[other; 256]],
            page_ids: Box::new([0; 4352]),
        };
        for (symbol, ch) in symbols.iter().enumerate() {
            let code = *ch as usize;
            if let Some(page) = result.page_ids.get_mut(code >> 8) {
                if *page == 0 {
                    *page = u16::try_from(result.pages.len())
                        .map_err(|_| InvalidPolicy("alphabet pages exceed limit"))?;
                    result.pages.push([other; 256]);
                }
                if let Some(slot) = result
                    .pages
                    .get_mut(usize::from(*page))
                    .and_then(|p| p.get_mut(code & 255))
                {
                    *slot = u16::try_from(symbol)
                        .map_err(|_| InvalidPolicy("alphabet exceeds limit"))?;
                }
            }
        }
        Ok(result)
    }

    fn symbol(&self, ch: char) -> Option<usize> {
        let code = ch as usize;
        self.pages
            .get(usize::from(*self.page_ids.get(code >> 8)?))?
            .get(code & 255)
            .copied()
            .map(usize::from)
    }
}

impl PathTrie {
    /// Bind the immutable storage revision authenticated by the policy owner.
    #[must_use]
    pub fn with_policy_revision(
        mut self,
        generation: crate::GenerationId,
        revision: crate::kernel::MetadataField<crate::ObjectId>,
    ) -> Self {
        self.policy_revision = Some((generation, revision));
        self
    }

    /// Bind an explicitly selected immutable data generation. Its authorization
    /// uses current policy, whose storage fence remains independent.
    #[must_use]
    pub fn with_data_generation(mut self, generation: crate::GenerationId) -> Self {
        self.data_generation = Some(generation);
        self
    }

    /// Explicit immutable selection, absent for a tracking view.
    #[must_use]
    pub const fn data_generation(&self) -> Option<crate::GenerationId> {
        self.data_generation
    }

    /// Whether this view still represents the authenticated persisted policy.
    pub fn matches_policy_revision(
        &self,
        generation: crate::GenerationId,
        revision: crate::kernel::MetadataField<crate::ObjectId>,
    ) -> bool {
        self.policy_revision.is_some_and(|(original, policy)| {
            policy == revision && (!self.generation_sensitive || original == generation)
        })
    }

    /// Heap bytes retained by the compiled view, excluding its inline header.
    #[must_use]
    pub fn logical_owned_bytes(&self) -> u64 {
        let rules = self.rules_owned_bytes();
        #[cfg(not(target_arch = "wasm32"))]
        let rules = if self
            .audit
            .as_ref()
            .is_some_and(|audit| audit.identity._custody.is_some())
        {
            0
        } else {
            rules
        };
        (self.alphabet.pages.capacity() * size_of::<[u16; 256]>()
            + size_of::<[u16; 4352]>()
            + self.states.capacity() * size_of::<State>()
            + self
                .states
                .iter()
                .map(|state| state.transitions.capacity() * size_of::<usize>())
                .sum::<usize>()
            + rules
            + self.volume.capacity()
            + self.subject.1.capacity()) as u64
    }

    pub(crate) fn rules_owned_bytes(&self) -> usize {
        self.rules.len() * size_of::<String>()
            + 2 * size_of::<usize>()
            + self.rules.iter().map(String::capacity).sum::<usize>()
    }

    /// Authoritative name semantics pinned by this view.
    #[must_use]
    pub const fn config(&self) -> crate::VolumeConfig {
        self.config
    }

    /// Authenticated volume identity pinned by compilation.
    #[must_use]
    pub fn volume(&self) -> &str {
        &self.volume
    }

    /// Authenticated principal identity pinned by compilation.
    #[must_use]
    pub fn subject(&self) -> (super::PrincipalKind, &str) {
        (self.subject.0, &self.subject.1)
    }

    pub(in crate::access) fn audit_binding(
        &self,
        principal: &Principal,
        capability: &Capability,
    ) -> Result<bool, InvalidPolicy> {
        Ok(
            self.principal_binding == super::capability::binding(principal)?
                && self.capability_binding == super::capability::binding(capability)?,
        )
    }

    /// Whether two views belong to the same authenticated volume, principal,
    /// capability and native name semantics. Policy revisions may differ.
    #[must_use]
    pub fn same_binding(&self, other: &Self) -> bool {
        self.config == other.config
            && self.volume == other.volume
            && self.subject == other.subject
            && self.capability_binding == other.capability_binding
            && self.data_generation == other.data_generation
    }

    /// Policy revision used for every decision in this view.
    #[must_use]
    pub const fn policy_version(&self) -> u64 {
        self.version
    }

    /// Latest authenticated volume position used to compile this view.
    #[must_use]
    pub const fn head_position(&self) -> u64 {
        self.head
    }

    /// Compile the same decision reducer used by `decide` into terminal states.
    /// `resource.path` is ignored; every other input is pinned for this view.
    /// Rejects policies whose glob automaton exceeds bounded state, storage or
    /// compilation work, including short patterns with exponential state growth.
    pub fn compile(
        policy: &Policy,
        principal: &Principal,
        resource: Resource<'_>,
        context: Context,
        capability: &Capability,
    ) -> Result<Self, InvalidPolicy> {
        let mut work = MAXIMUM_WORK;
        Self::compile_with_work(
            policy,
            principal,
            resource,
            context,
            capability,
            &mut work,
            |_| Ok(()),
        )
    }

    /// Compile with caller work and preallocation admission. The two storage
    /// reservations cover NFA preparation and additional DFA storage. They must
    /// remain funded until the returned trie is dropped. Failed work is charged
    /// through `remaining_work`; the global compilation limits still apply.
    pub fn compile_with_work(
        policy: &Policy,
        principal: &Principal,
        resource: Resource<'_>,
        context: Context,
        capability: &Capability,
        remaining_work: &mut usize,
        mut reserve: impl FnMut(usize) -> Result<(), InvalidPolicy>,
    ) -> Result<Self, InvalidPolicy> {
        let mut available = (*remaining_work).min(MAXIMUM_WORK);
        let before = available;
        let result = (|| {
            let (bytes, validation_work) = super::compilation::preparation(
                policy,
                capability,
                super::names::folds_unicode(resource.config),
            )?;
            spend(&mut available, validation_work)?;
            let bindings = resource
                .volume
                .len()
                .checked_add(principal.id.len())
                .and_then(|bytes| bytes.checked_mul(2))
                .and_then(|bindings| bindings.checked_add(bytes))
                .ok_or(InvalidPolicy("glob compilation storage overflow"))?;
            reserve(bindings)?;
            policy.validate()?;
            capability.validate()?;
            if resource.config.validate().is_err() || resource.generation > resource.head {
                return Err(InvalidPolicy("generation exceeds head"));
            }
            let mut nfa = Nfa::default();
            let mut starts = Vec::new();
            let mut rules = BTreeMap::new();
            let mut grants = BTreeMap::new();
            let mut text_left = MAXIMUM_TEXT_BYTES;
            spend(&mut text_left, resource.volume.len())?;
            spend(&mut text_left, principal.id.len())?;
            let mut valid_until = capability.expires_at.filter(|expiry| *expiry > context.now);
            for rule in policy.layers.iter().flatten() {
                if let Some(expiry) = rule
                    .conditions
                    .expires_at
                    .filter(|expiry| *expiry > context.now)
                {
                    valid_until = Some(valid_until.map_or(expiry, |old| old.min(expiry)));
                }
                if super::decision::rule_matches(rule, principal, resource, context) {
                    spend(&mut text_left, rule.resource.path.len())?;
                    let key = super::names::fold(&rule.resource.path, resource.config);
                    let (start, end) =
                        nfa.pattern(&key, super::names::folds_unicode(resource.config))?;
                    starts.push(start);
                    rules.insert(rule.id.as_str(), end);
                }
            }
            for grant in capability.layers.iter().flatten() {
                if grant.resource.matches_view(resource) {
                    spend(&mut text_left, grant.resource.path.len())?;
                    let key = super::names::fold(&grant.resource.path, resource.config);
                    let (start, end) =
                        nfa.pattern(&key, super::names::folds_unicode(resource.config))?;
                    starts.push(start);
                    grants.insert(std::ptr::from_ref(grant), end);
                }
            }
            let (symbols, other) = alphabet(&nfa)?;
            let rule_ids: Vec<_> = rules
                .keys()
                .map(|id| {
                    text_left = text_left
                        .checked_sub(id.len())
                        .ok_or(InvalidPolicy("compiled rule ids exceed text limit"))?;
                    Ok((*id).to_owned())
                })
                .collect::<Result<_, InvalidPolicy>>()?;
            let rules = rules
                .into_iter()
                .enumerate()
                .map(|(index, (id, end))| (id, (end, index)))
                .collect();
            reserve(super::compilation::determinization(
                nfa.nodes.len(),
                symbols.len(),
                std::mem::size_of::<State>(),
            )?)?;
            let uniform = policy
                .layers
                .iter()
                .flatten()
                .all(|rule| rule.resource.path == "**")
                && capability
                    .layers
                    .iter()
                    .flatten()
                    .all(|grant| grant.resource.path == "**");
            let states = if uniform {
                // Every active pattern accepts every canonical path, including
                // root. One terminal state replaces subset construction.
                spend(&mut available, nfa.nodes.len())?;
                spend(
                    &mut available,
                    (policy.layers.len()
                        + capability.layers.len()
                        + policy.layers.iter().map(Vec::len).sum::<usize>()
                        + capability.layers.iter().map(Vec::len).sum::<usize>())
                        * 24,
                )?;
                let subset: Vec<_> = (0..nfa.nodes.len()).collect();
                vec![terminal(
                    vec![0; symbols.len() + 1],
                    outcomes(
                        policy,
                        capability,
                        context,
                        resource.generation != resource.head,
                        &rules,
                        &grants,
                        &subset,
                    ),
                )]
            } else {
                determinize(
                    &nfa,
                    starts,
                    &symbols,
                    other,
                    |subset| {
                        outcomes(
                            policy,
                            capability,
                            context,
                            resource.generation != resource.head,
                            &rules,
                            &grants,
                            subset,
                        )
                    },
                    policy.layers.len()
                        + capability.layers.len()
                        + policy.layers.iter().map(Vec::len).sum::<usize>()
                        + capability.layers.iter().map(Vec::len).sum::<usize>(),
                    &mut available,
                )?
            };
            // Moving head turns a pinned current read/list into history, even
            // when every selector says All. Preserve the same ReadHistory gate.
            let history_sensitive = states.iter().any(|state| {
                state.actions & ((1 << Action::Read as u16) | (1 << Action::List as u16)) != 0
                    && state.actions & (1 << Action::ReadHistory as u16) == 0
            });
            Ok(Self {
                audit: None,
                alphabet: Alphabet::new(&symbols)?,
                states,
                rules: rule_ids.into(),
                version: policy.version,
                head: resource.head,
                generation: resource.generation,
                valid_from: context.now,
                valid_until,
                config: resource.config,
                volume: resource.volume.to_owned(),
                subject: (principal.kind, principal.id.clone()),
                capability_binding: super::capability::binding(capability)?,
                principal_binding: super::capability::binding(principal)?,
                policy_revision: None,
                data_generation: None,
                generation_sensitive: history_sensitive
                    || policy.layers.iter().flatten().any(|rule| {
                        rule.resource.generations != super::Generations::All
                            || rule.conditions.generations.is_some()
                    })
                    || capability
                        .layers
                        .iter()
                        .flatten()
                        .any(|grant| grant.resource.generations != super::Generations::All),
                #[cfg(not(target_arch = "wasm32"))]
                native_custody: None,
            })
        })();
        *remaining_work -= before - available;
        result
    }

    /// Earliest time at which the caller must rebuild this immutable view.
    ///
    /// ```compile_fail
    /// use acyclic_fs::access::PathTrie;
    /// fn disable_expiry(trie: &mut PathTrie) {
    ///     trie.valid_until = None;
    /// }
    /// ```
    #[must_use]
    pub const fn valid_until(&self) -> Option<u64> {
        self.valid_until
    }

    /// Conservative proof for an adapter that cannot enforce individual paths.
    /// Every reachable automaton state must permit every requested action.
    /// This uses the compiled decisions, including capability intersections.
    #[must_use]
    pub fn allows_every_path(&self, actions: &[Action], now: u64) -> bool {
        self.states.iter().enumerate().all(|(state, _)| {
            actions
                .iter()
                .all(|action| self.decision(state, *action, now).allowed)
        })
    }

    /// Whether moving between generations can change this view's decisions.
    #[must_use]
    pub const fn generation_sensitive(&self) -> bool {
        self.generation_sensitive
    }

    /// Prepare an entire path, streaming native name keys without temporary strings.
    /// Native lookups can retain parents and use `PreparedPath::child` to scan
    /// only new components; hot checks use `PreparedPath::decide`.
    pub fn prepare(&self, path: &str) -> Result<PreparedPath<'_>, InvalidPolicy> {
        let mut prepared = PreparedPath {
            trie: self,
            state: 0,
            audit_path: None,
            #[cfg(not(target_arch = "wasm32"))]
            work: None,
            bytes: 0,
            depth: 0,
            capture: true,
        };
        if !path.is_empty() {
            for name in path.split('/') {
                prepared = prepared.child(name)?;
            }
        }
        Ok(prepared)
    }

    /// Convenience decision for an unprepared UTF-8 path. Preparation is linear
    /// in path bytes; the bound handle's decision itself is constant time.
    #[must_use]
    pub fn evaluate(&self, action: Action, path: &str, now: u64) -> Decision<'_> {
        self.prepare_for_preview(path).map_or_else(
            |_| Decision::denied(self.version, Denial::Invalid),
            |path| path.evaluate(action, now),
        )
    }

    /// Runtime authorization: exactly one queued record for either policy outcome.
    /// Audit failure is an error, never a usable authorization decision.
    pub fn decide(
        &self,
        action: Action,
        path: &str,
        now: u64,
    ) -> Result<Decision<'_>, super::audit::AuditError> {
        self.prepare(path)
            .map_err(|_| super::audit::AuditError)?
            .decide(action, now)
    }

    fn authorize_state(
        &self,
        state: usize,
        path: &Option<std::sync::Arc<super::audit::Path>>,
        action: Action,
        now: u64,
    ) -> Result<Decision<'_>, super::audit::AuditError> {
        let decision = self.decision(state, action, now);
        let binding = self.audit.as_ref().ok_or(super::audit::AuditError)?;
        let rule = self
            .states
            .get(state)
            .and_then(|node| node.decisions.get(action as usize))
            .and_then(|outcome| outcome.rule)
            .filter(|_| decision.rule_id.is_some());
        binding.record(self, path.as_ref(), action, now, decision, rule, false)?;
        Ok(decision)
    }

    /// Authorize a coarse adapter over its whole exposed path set. The first
    /// refusing terminal state attributes a denial; the resource is the glob `**`.
    pub fn decide_all(
        &self,
        action: Action,
        now: u64,
    ) -> Result<Decision<'_>, super::audit::AuditError> {
        let state = (0..self.states.len())
            .find(|state| !self.decision(*state, action, now).allowed)
            .unwrap_or(0);
        let decision = self.decision(state, action, now);
        let binding = self.audit.as_ref().ok_or(super::audit::AuditError)?;
        let rule = self
            .states
            .get(state)
            .and_then(|node| node.decisions.get(action as usize))
            .and_then(|outcome| outcome.rule)
            .filter(|_| decision.rule_id.is_some());
        binding.record(self, None, action, now, decision, rule, true)?;
        Ok(decision)
    }

    fn step(&self, state: usize, ch: char) -> Result<usize, InvalidPolicy> {
        let symbol = self
            .alphabet
            .symbol(ch)
            .ok_or(InvalidPolicy("invalid alphabet"))?;
        self.states
            .get(state)
            .and_then(|node| node.transitions.get(symbol))
            .copied()
            .ok_or(InvalidPolicy("invalid compiled path"))
    }

    pub(super) fn decision(&self, state: usize, action: Action, now: u64) -> Decision<'_> {
        match self
            .state_at(state, now)
            .and_then(|node| node.decisions.get(action as usize))
        {
            Some(outcome) => Decision {
                allowed: outcome.denial.is_none(),
                rule_id: outcome
                    .rule
                    .and_then(|id| self.rules.get(id).map(String::as_str)),
                denial: outcome.denial,
                policy_version: self.version,
            },
            None => Decision::denied(self.version, Denial::Invalid),
        }
    }

    fn state_at(&self, state: usize, now: u64) -> Option<&State> {
        if now < self.valid_from || self.valid_until.is_some_and(|expiry| now >= expiry) {
            None
        } else {
            self.states.get(state)
        }
    }
}

fn outcomes(
    policy: &Policy,
    capability: &Capability,
    context: Context,
    historical: bool,
    rules: &BTreeMap<&str, (usize, usize)>,
    grants: &BTreeMap<*const super::Grant, usize>,
    subset: &[usize],
) -> [Outcome; 12] {
    Action::ALL.map(|action| {
        let decision = super::decision::evaluate(
            policy,
            capability,
            context,
            action,
            historical,
            |rule| {
                rules
                    .get(rule.id.as_str())
                    .is_some_and(|(end, _)| subset.binary_search(end).is_ok())
            },
            |grant| {
                grants
                    .get(&std::ptr::from_ref(grant))
                    .is_some_and(|end| subset.binary_search(end).is_ok())
            },
        );
        Outcome {
            rule: decision
                .rule_id
                .and_then(|id| rules.get(id).map(|(_, index)| *index)),
            denial: decision.denial,
        }
    })
}

fn alphabet(nfa: &Nfa) -> Result<(Vec<char>, char), InvalidPolicy> {
    let mut alphabet: BTreeSet<char> = nfa
        .nodes
        .iter()
        .flat_map(|node| node.edges.iter())
        .filter_map(|(symbol, _)| match symbol {
            Symbol::Literal(c) => Some(*c),
            _ => None,
        })
        .collect();
    alphabet.insert('/');
    // One representative for every character not named literally.
    let other = (1..=0x10ffff)
        .filter_map(char::from_u32)
        .find(|ch| !alphabet.contains(ch))
        .ok_or(InvalidPolicy("exhausted glob alphabet"))?;
    let alphabet: Vec<_> = alphabet.into_iter().collect();
    Ok((alphabet, other))
}

fn determinize(
    nfa: &Nfa,
    mut starts: Vec<usize>,
    alphabet: &[char],
    other: char,
    decide: impl Fn(&[usize]) -> [Outcome; 12],
    rules: usize,
    work: &mut usize,
) -> Result<Vec<State>, InvalidPolicy> {
    nfa.close(&mut starts, work)?;
    let mut members = MAXIMUM_STATE_MEMBERS;
    spend(&mut members, starts.len())?;
    let mut transitions_left = MAXIMUM_TRANSITIONS;
    let mut ids = BTreeMap::from([(starts.clone(), 0)]);
    let mut subsets = vec![starts];
    let mut states = Vec::new();
    while states.len() < subsets.len() {
        spend(
            work,
            rules
                .checked_mul(24)
                .ok_or(InvalidPolicy("glob compilation exceeds work limit"))?,
        )?;
        let subset = subsets
            .get(states.len())
            .ok_or(InvalidPolicy("invalid glob state"))?
            .clone();
        let decisions = decide(&subset);
        spend(&mut transitions_left, alphabet.len() + 1)?;
        let mut transitions = Vec::with_capacity(alphabet.len() + 1);
        for ch in alphabet.iter().copied().chain(std::iter::once(other)) {
            let next = nfa.step(&subset, ch, work)?;
            let id = if let Some(id) = ids.get(&next) {
                *id
            } else {
                if subsets.len() == MAXIMUM_STATES {
                    return Err(InvalidPolicy("glob compilation exceeds DFA state limit"));
                }
                spend(&mut members, next.len())?;
                let id = subsets.len();
                ids.insert(next.clone(), id);
                subsets.push(next);
                id
            };
            transitions.push(id);
        }
        states.push(terminal(transitions, decisions));
    }

    Ok(states)
}

fn terminal(transitions: Vec<usize>, decisions: [Outcome; 12]) -> State {
    State {
        transitions,
        actions: decisions
            .iter()
            .enumerate()
            .fold(0, |mask, (bit, outcome)| {
                mask | if outcome.denial.is_none() {
                    1 << bit
                } else {
                    0
                }
            }),
        decisions,
    }
}
