use super::PathTrie;

impl PathTrie {
    pub(in crate::access) fn subtree_action_mask(&self, prefix: &str, now: u64) -> u16 {
        let Ok(path) = self.prepare(prefix) else {
            return 0;
        };
        let Some(initial) = self.state_at(path.state, now) else {
            return 0;
        };
        let mut actions = initial.actions;
        let start = if prefix.is_empty() {
            path.state
        } else {
            let Ok(state) = self.step(path.state, '/') else {
                return actions;
            };
            state
        };
        let mut seen = vec![false; self.states.len()];
        let mut pending = vec![start];
        let Some(visited) = seen.get_mut(start) else {
            return 0;
        };
        *visited = true;
        while let Some(state) = pending.pop() {
            let Some(node) = self.states.get(state) else {
                return 0;
            };
            actions |= node.actions;
            for &next in &node.transitions {
                let Some(visited) = seen.get_mut(next) else {
                    return 0;
                };
                if !*visited {
                    *visited = true;
                    pending.push(next);
                }
            }
        }
        actions
    }
}
