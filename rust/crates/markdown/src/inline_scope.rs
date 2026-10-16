use super::State;
use crate::inline_scan::Scope;

impl State {
    pub(in crate::inline_scan) fn scope(&mut self, scope: &Scope, output: usize) {
        let frontier = self.earliest(scope.range.start);
        if let Some(eof) = &mut self.eof {
            eof.invalidate();
        }
        self.forest.atom(scope.range.clone(), output);
        self.labels = if scope.label && self.bracket == 0 {
            [Some(scope.range.clone()), None]
        } else {
            [None, None]
        };
        if scope.open_code {
            self.opaque = Some(frontier);
        }
        if scope.disables_links {
            self.link_floor = scope.range.end;
        }
    }
}
