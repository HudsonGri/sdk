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
            [self.remember_closed(scope.range.clone(), false, 0), 0]
        } else {
            [0, 0]
        };
        if scope.open_code {
            self.opaque = Some(frontier);
        }
        if scope.disables_links {
            self.link_floor = scope.range.end;
        }
    }
}
