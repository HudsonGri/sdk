//! Heap layout of typed records, described by JSON position.
//!
//! Admission charges each JSON container for the allocations its typed decode
//! performs at that position: a collection buffer, a B-tree, or a box, plus
//! the serde `Content` buffers that tagged enums build before choosing a
//! variant. A container at a position without a description, or of the other
//! JSON kind, is charged as a B-tree of the largest logical record slot and as
//! buffered content, so no undescribed list is cheaper than any admissible
//! type.
use serde::__private229::de::Content;
use std::cell::RefCell;

/// Allocations one JSON container causes when decoded at a described position.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Alloc {
    vec: u64,
    tree: u64,
    once: u64,
}
impl Alloc {
    /// A struct, tuple or fixed array: stored in its parent's slot.
    pub(crate) const INLINE: Self = Self {
        vec: 0,
        tree: 0,
        once: 0,
    };
    /// `Vec<T>` or `VecDeque<T>`, grown one element at a time.
    pub(crate) const fn vec<T>() -> Self {
        Self {
            vec: size_of::<T>() as u64,
            ..Self::INLINE
        }
    }
    /// `BTreeMap<K, V>`, or `BTreeSet<K>` with `V = ()`.
    pub(crate) const fn tree<K, V>() -> Self {
        Self {
            tree: (size_of::<K>() + size_of::<V>()) as u64,
            ..Self::INLINE
        }
    }
    /// `BTreeMap<K, V>` decoded through an intermediate `Vec<(K, V)>`, which
    /// a stable sort orders with at most one scratch slot per entry.
    pub(crate) const fn entries<K, V>() -> Self {
        Self {
            vec: size_of::<(K, V)>() as u64,
            ..Self::tree::<K, V>()
        }
    }
    /// `Box<T>` holding the record at this position.
    pub(crate) const fn boxed<T>() -> Self {
        Self {
            once: size_of::<T>() as u64,
            ..Self::INLINE
        }
    }
    /// Element slot bytes of each allocation kind, for coverage checks.
    #[cfg(test)]
    pub(crate) const fn slots(self) -> [u64; 3] {
        [self.vec, self.tree, self.once]
    }
    /// Bytes the decoded container keeps, and the largest buffer it holds
    /// only while it is built: the vector's previous buffer while it moves,
    /// or the sort scratch of an entry map.
    fn bytes(self, entries: u64) -> (u64, u64) {
        let (kept, moving) = vec_growth(self.vec, entries);
        let sort = if self.tree == 0 { 0 } else { self.vec };
        let kept = kept
            .saturating_add(tree_bytes(self.tree, entries))
            .saturating_add(self.once);
        (kept, moving.max(sort.saturating_mul(entries)))
    }
}

/// Typed heap layout at one JSON position.
#[derive(Debug)]
pub(crate) struct JsonLayout {
    seq: Option<Alloc>,
    map: Option<Alloc>,
    tagged: bool,
    fields: &'static [(&'static str, &'static JsonLayout)],
    elements: &'static [&'static JsonLayout],
}
/// Any position without a description.
pub(crate) static UNDESCRIBED: JsonLayout = JsonLayout {
    seq: None,
    map: None,
    tagged: true,
    fields: &[],
    elements: &[],
};
impl JsonLayout {
    /// A struct object; listed fields hold containers, other fields scalars.
    pub(crate) const fn record(fields: &'static [(&'static str, &'static JsonLayout)]) -> Self {
        Self {
            seq: None,
            map: Some(Alloc::INLINE),
            tagged: false,
            fields,
            elements: &[],
        }
    }
    /// An internally or adjacently tagged enum, buffered before its variant.
    pub(crate) const fn tagged(fields: &'static [(&'static str, &'static JsonLayout)]) -> Self {
        Self {
            tagged: true,
            ..Self::record(fields)
        }
    }
    /// A record held in its own box.
    pub(crate) const fn boxed(
        alloc: Alloc,
        fields: &'static [(&'static str, &'static JsonLayout)],
    ) -> Self {
        Self {
            map: Some(alloc),
            ..Self::record(fields)
        }
    }
    /// A JSON array decoded as a collection; one element layout applies to all.
    pub(crate) const fn list(alloc: Alloc, element: &'static [&'static JsonLayout]) -> Self {
        Self {
            seq: Some(alloc),
            map: None,
            tagged: false,
            fields: &[],
            elements: element,
        }
    }
    /// A tuple or fixed array: positional elements stored inline.
    pub(crate) const fn tuple(elements: &'static [&'static JsonLayout]) -> Self {
        Self::list(Alloc::INLINE, elements)
    }
    /// A JSON object decoded as a map; `values` names its `"*"` entry layout.
    pub(crate) const fn map(
        alloc: Alloc,
        values: &'static [(&'static str, &'static JsonLayout)],
    ) -> Self {
        Self {
            map: Some(alloc),
            ..Self::record(values)
        }
    }
    /// The same position also admits a JSON array decoded as `alloc`.
    pub(crate) const fn or_list(
        self,
        alloc: Alloc,
        element: &'static [&'static JsonLayout],
    ) -> Self {
        Self {
            seq: Some(alloc),
            elements: element,
            ..self
        }
    }
    /// Allocation for one JSON kind, or `None` when that kind is undescribed.
    pub(crate) const fn alloc(&self, map: bool) -> Option<Alloc> {
        if map { self.map } else { self.seq }
    }
    fn element(&self, index: usize) -> &'static JsonLayout {
        match self.elements {
            [only] => only,
            elements => elements.get(index).copied().unwrap_or(&UNDESCRIBED),
        }
    }
    fn field(&self, key: &[u8]) -> &'static JsonLayout {
        self.fields
            .iter()
            .find(|(name, _)| name.as_bytes() == key || *name == "*")
            .map_or(&UNDESCRIBED, |(_, child)| *child)
    }
    fn buffers(&self, map: bool) -> bool {
        self.tagged || self.alloc(map).is_none()
    }
}

/// Bytes admitted for a layout walk.
///
/// The decode keeps every collection, box and string it builds. Everything
/// else is held only while one container is built, and the decoder builds one
/// container at a time: a vector's previous buffer while it moves, an entry
/// map's sort scratch, and the `Content` that a tagged enum buffers until its
/// variant is chosen. A tagged subtree's buffers are released before its next
/// sibling's are made, so the peak holds the kept bytes, one tagged subtree's
/// buffers, and one moving buffer.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct LayoutCost {
    /// Collections, boxes and trees the decoded value keeps.
    kept: u64,
    /// Decoded string and owned map key bytes.
    strings: u64,
    /// Longest decoded string, the bound on the decoder's escape scratch.
    longest: u64,
    /// `Content` buffers of the tagged subtree being walked.
    buffering: u64,
    /// Largest `Content` buffers of one tagged subtree.
    buffered: u64,
    /// Largest buffer held while one container is built.
    moving: u64,
    /// Containers charged at the undescribed fallback.
    pub(crate) undescribed: u64,
}
impl LayoutCost {
    /// Cost of a document of `input` bytes and `items` values whose walk did
    /// not complete, every value an undescribed, buffered container. A
    /// container of `n` entries costs at most what three entries cost, per
    /// entry, and every container is kept.
    pub(crate) fn undescribed(items: u64, input: u64) -> Self {
        let (kept, buffered) = fallback_bytes(true, 3, true);
        Self {
            kept: kept.saturating_add(buffered).saturating_mul(items.max(1)),
            strings: input,
            longest: input,
            undescribed: items,
            ..Self::default()
        }
    }
    /// Maximum heap bytes of the typed decode.
    pub(crate) fn bytes(self) -> u64 {
        // Escaped strings decode through one scratch vector that grows by
        // doubling and is reused for every later string.
        self.kept
            .saturating_add(self.strings)
            .saturating_add(vec_bytes(1, self.longest))
            .saturating_add(self.buffered)
            .saturating_add(self.moving)
    }
    /// Longest decoded string or key.
    pub(crate) const fn longest(self) -> u64 {
        self.longest
    }
    fn string(&mut self, bytes: u64, owned: bool) {
        if owned {
            self.strings = self.strings.saturating_add(bytes);
        }
        self.longest = self.longest.max(bytes);
    }
    fn add(&mut self, node: &JsonLayout, map: bool, entries: u64, buffered: bool) {
        let (kept, content) = if let Some(alloc) = node.alloc(map) {
            let (kept, moving) = alloc.bytes(entries);
            self.moving = self.moving.max(moving);
            (kept, content_bytes(map, entries, buffered, node.tagged))
        } else {
            self.undescribed += 1;
            fallback_bytes(map, entries, buffered)
        };
        self.kept = self.kept.saturating_add(kept);
        self.buffering = self.buffering.saturating_add(content);
        // This container's visit ends after its subtree's: a buffered
        // subtree's root closes the subtree.
        if !buffered && node.buffers(map) {
            self.buffered = self.buffered.max(self.buffering);
            self.buffering = 0;
        }
    }
}
/// Largest logical record slot: the charge for an undescribed element.
pub(crate) const FALLBACK_SLOT: u64 = crate::core::Snapshot::LOGICAL_SCHEMA_SLOT_BYTES;
/// Kept and buffered bytes of an undescribed container: a B-tree of the
/// largest slot, buffered as a tagged enum.
fn fallback_bytes(map: bool, entries: u64, buffered: bool) -> (u64, u64) {
    (
        tree_bytes(FALLBACK_SLOT, entries),
        content_bytes(map, entries, buffered, true),
    )
}
/// Final and previous capacity, in bytes, of a vector pushed one element at
/// a time from no capacity: it starts at the allocator's minimum capacity and
/// doubles.
fn vec_growth(slot: u64, entries: u64) -> (u64, u64) {
    if slot == 0 || entries == 0 {
        return (0, 0);
    }
    let mut capacity = match slot {
        1 => 8,
        2..=1024 => 4,
        _ => 1,
    };
    let mut previous = 0;
    while capacity < entries {
        previous = capacity;
        capacity = capacity.saturating_mul(2);
    }
    (slot.saturating_mul(capacity), slot.saturating_mul(previous))
}
/// Peak bytes of a growing vector: its last two buffers together.
fn vec_bytes(slot: u64, entries: u64) -> u64 {
    let (last, previous) = vec_growth(slot, entries);
    last.saturating_add(previous)
}
/// Bytes of a B-tree of `entries`. A leaf holds eleven slots and a header; an
/// internal node adds twelve edges. Up to eleven entries fit the root leaf.
/// Beyond that every leaf holds at least five entries and every internal node
/// at least two children, and building the tree frees no node.
fn tree_bytes(slot: u64, entries: u64) -> u64 {
    if slot == 0 || entries == 0 {
        return 0;
    }
    let leaf = slot.saturating_mul(11).saturating_add(32);
    if entries <= 11 {
        return leaf;
    }
    let leaves = entries / 5;
    leaf.saturating_mul(leaves)
        .saturating_add(leaf.saturating_add(96).saturating_mul(leaves - 1))
}
/// A tagged enum buffers its whole subtree as `Content`, one vector per
/// container, and collects its own entries again when an enclosing buffer
/// replays it. Values move out of buffers without copying.
fn content_bytes(map: bool, entries: u64, buffered: bool, tagged: bool) -> u64 {
    let copies = u64::from(buffered) + u64::from(tagged);
    if copies == 0 {
        return 0;
    }
    let slot = if map {
        size_of::<(Content<'static>, Content<'static>)>()
    } else {
        size_of::<Content<'static>>()
    } as u64;
    vec_bytes(slot, entries).saturating_mul(copies)
}

/// One position in an admission walk.
#[derive(Clone, Copy)]
pub(super) struct Placement<'a> {
    node: &'static JsonLayout,
    buffered: bool,
    cost: &'a RefCell<LayoutCost>,
}
impl<'a> Placement<'a> {
    pub(super) fn root(node: &'static JsonLayout, cost: &'a RefCell<LayoutCost>) -> Self {
        Self {
            node,
            buffered: false,
            cost,
        }
    }
    /// A string value. Enum labels and identifiers own no bytes, but a string
    /// is charged at every position as though it decodes to a `String`.
    pub(super) fn string(self, bytes: usize) {
        self.cost.borrow_mut().string(bytes as u64, true);
    }
}
/// Counts one container's entries and charges it however its visit ends.
pub(super) struct Entries<'a> {
    placement: Option<Placement<'a>>,
    map: bool,
    pub(super) count: u64,
}
impl<'a> Entries<'a> {
    pub(super) fn new(placement: Option<Placement<'a>>, map: bool) -> Self {
        Self {
            placement,
            map,
            count: 0,
        }
    }
    /// An object key. Map keys and keys buffered as content own their bytes;
    /// struct field names only select a field.
    pub(super) fn key(&self, bytes: usize) {
        if let Some(placement) = self.placement {
            let owned = placement.buffered
                || placement.node.buffers(true)
                || placement.node.alloc(true) != Some(Alloc::INLINE);
            placement.cost.borrow_mut().string(bytes as u64, owned);
        }
    }
    pub(super) fn child(&self, key: Option<&[u8]>, index: usize) -> Option<Placement<'a>> {
        self.placement.map(|placement| Placement {
            node: key.map_or_else(
                || placement.node.element(index),
                |key| placement.node.field(key),
            ),
            buffered: placement.buffered || placement.node.buffers(self.map),
            cost: placement.cost,
        })
    }
}
impl Drop for Entries<'_> {
    fn drop(&mut self) {
        if let Some(placement) = self.placement {
            placement.cost.borrow_mut().add(
                placement.node,
                self.map,
                self.count,
                placement.buffered,
            );
        }
    }
}
