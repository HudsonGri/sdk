//! Balanced ownership-preserving indices. Rotations move existing allocations.

pub(crate) type Tree<N> = Option<<N as Node>::Pointer>;

pub(crate) trait Node: Sized {
    type Key: Ord;
    type Pointer: std::ops::Deref<Target = Self> + std::ops::DerefMut;
    fn key(&self) -> &Self::Key;
    fn left(&self) -> &Tree<Self>;
    fn left_mut(&mut self) -> &mut Tree<Self>;
    fn right(&self) -> &Tree<Self>;
    fn right_mut(&mut self) -> &mut Tree<Self>;
    fn height(&self) -> usize;
    /// Refresh height and adapter-specific summaries after moving children.
    fn refresh(&mut self);
}

pub(crate) fn height<N: Node>(tree: &Tree<N>) -> usize {
    tree.as_ref().map_or(0, |node| node.height())
}

pub(crate) fn find_with<'a, N: Node, E>(
    tree: &'a Tree<N>,
    key: &N::Key,
    mut examine: impl FnMut() -> Result<(), E>,
) -> Result<Option<&'a N>, E> {
    let mut current = tree;
    while let Some(node) = current {
        examine()?;
        current = match key.cmp(node.key()) {
            std::cmp::Ordering::Less => node.left(),
            std::cmp::Ordering::Greater => node.right(),
            std::cmp::Ordering::Equal => return Ok(Some(&**node)),
        };
    }
    Ok(None)
}

fn left<N: Node>(mut node: N::Pointer) -> N::Pointer {
    let Some(mut pivot) = node.right_mut().take() else {
        return node;
    };
    *node.right_mut() = pivot.left_mut().take();
    node.refresh();
    *pivot.left_mut() = Some(node);
    pivot.refresh();
    pivot
}

fn right<N: Node>(mut node: N::Pointer) -> N::Pointer {
    let Some(mut pivot) = node.left_mut().take() else {
        return node;
    };
    *node.left_mut() = pivot.right_mut().take();
    node.refresh();
    *pivot.right_mut() = Some(node);
    pivot.refresh();
    pivot
}

fn balance<N: Node>(mut node: N::Pointer) -> N::Pointer {
    node.refresh();
    if height::<N>(node.left()) > height::<N>(node.right()) + 1 {
        if node
            .left()
            .as_ref()
            .is_some_and(|child| height::<N>(child.right()) > height::<N>(child.left()))
        {
            *node.left_mut() = node.left_mut().take().map(left::<N>);
        }
        return right::<N>(node);
    }
    if height::<N>(node.right()) > height::<N>(node.left()) + 1 {
        if node
            .right()
            .as_ref()
            .is_some_and(|child| height::<N>(child.left()) > height::<N>(child.right()))
        {
            *node.right_mut() = node.right_mut().take().map(right::<N>);
        }
        return left::<N>(node);
    }
    node
}

/// Inserts an already funded allocation, returning a detached replacement.
/// No allocation or destruction occurs in balancing or replacement.
pub(crate) fn insert<N: Node>(tree: Tree<N>, mut inserted: N::Pointer) -> (N::Pointer, Tree<N>) {
    let Some(mut node) = tree else {
        return (inserted, None);
    };
    let replaced = match inserted.key().cmp(node.key()) {
        std::cmp::Ordering::Less => {
            let (next, replaced) = insert::<N>(node.left_mut().take(), inserted);
            *node.left_mut() = Some(next);
            replaced
        }
        std::cmp::Ordering::Greater => {
            let (next, replaced) = insert::<N>(node.right_mut().take(), inserted);
            *node.right_mut() = Some(next);
            replaced
        }
        std::cmp::Ordering::Equal => {
            *inserted.left_mut() = node.left_mut().take();
            *inserted.right_mut() = node.right_mut().take();
            return (balance::<N>(inserted), Some(node));
        }
    };
    (balance::<N>(node), replaced)
}

fn detach_first<N: Node>(mut node: N::Pointer) -> (Tree<N>, N::Pointer) {
    let Some(first) = node.left_mut().take() else {
        return (node.right_mut().take(), node);
    };
    let (remaining, first) = detach_first::<N>(first);
    *node.left_mut() = remaining;
    (Some(balance::<N>(node)), first)
}

pub(crate) fn remove<N: Node>(tree: Tree<N>, key: &N::Key) -> (Tree<N>, Tree<N>) {
    let Some(mut node) = tree else {
        return (None, None);
    };
    let removed = match key.cmp(node.key()) {
        std::cmp::Ordering::Less => {
            let (remaining, removed) = remove::<N>(node.left_mut().take(), key);
            *node.left_mut() = remaining;
            removed
        }
        std::cmp::Ordering::Greater => {
            let (remaining, removed) = remove::<N>(node.right_mut().take(), key);
            *node.right_mut() = remaining;
            removed
        }
        std::cmp::Ordering::Equal => {
            let Some(right) = node.right_mut().take() else {
                return (node.left_mut().take(), Some(node));
            };
            let (remaining, mut first) = detach_first::<N>(right);
            *first.left_mut() = node.left_mut().take();
            *first.right_mut() = remaining;
            return (Some(balance::<N>(first)), Some(node));
        }
    };
    (Some(balance::<N>(node)), removed)
}
