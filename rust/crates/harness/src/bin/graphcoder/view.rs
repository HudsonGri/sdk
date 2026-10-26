//! Terminal input and painting over the same agent session as headless/WASM.
use acyclic_harness::{
    OperationId, client,
    agent::{Delivered, IntentId, Sender, TurnInput},
    local::LocalHarness,
    tui::{
        self, AgentRenderer, InputEffect, PromptInput, Viewport,
        ratatui::layout::{Position, Rect, Size},
        term::{Environment, FrameTerminal, RegionScroll, ScreenMode, Tty},
    },
};
use futures::StreamExt as _;
use std::{sync::Arc, time::Instant};

#[cfg(test)]
#[path = "view/diff.rs"]
mod diff;

pub async fn run(
    local: Arc<LocalHarness>,
    inline: bool,
    resume: bool,
) -> Result<client::HeadlessOutcome, Box<dyn std::error::Error>> {
    let mut client = client::LocalAgentClient::connect_window(
        local, if resume { 16 } else { 0 }, 4 * 1024 * 1024,
    ).await?;
    let tty = open_tty(inline)?;
    let terminal_size = tty.size_reader();
    let (tty, output) = tty.split();
    let mut terminal = FrameTerminal::new(output, terminal_size()?)?;
    let mut events = client::terminal_events(tty);
    let mut input = PromptInput::default();
    let catalog = serde_json::to_vec(&client.catalog().commands().collect::<Vec<_>>())?;
    input.completion.set_file("commands/catalog.json", Some(&catalog))?;
    let mut viewport = Viewport::default();
    let mut renderer = AgentRenderer::new(client.client().agent().clone());
    let mut shown = None;
    let start = Instant::now();
    let mut batch = client::InputBatch::new();
    let mut effects = Vec::new();
    // Only one locally submitted receipt is retained. The input is refused
    // before staging when occupied; it remains an editable draft.
    let mut pending: Option<(IntentId, u64)> = None;
    renderer.request_frame();
    loop {
        if batch.is_empty() {
            let deadline = renderer.frame_deadline();
            tokio::select! {
                read = events.next() => {
                    batch = read.ok_or("terminal input closed")??;
                },
                event = client.next() => {
                    match event? {
                        client::AgentEvent::Conversation { sequence, entry, .. } => {
                            if let TurnInput::Message { from: Sender::User { principal }, id, inbox, .. } = &entry.input
                                && principal == "local"
                                && pending.as_ref().is_some_and(|(expected, at)| expected == id && at == inbox)
                            {
                                pending = None;
                            }
                            let turn = client.read_turn(sequence, entry).await?;
                            renderer.discard_before(client.client().conversation().first_sequence());
                            renderer.append(sequence, &turn.output.text)?;
                        }
                        client::AgentEvent::Status { status, .. } => renderer.status(&status),
                        client::AgentEvent::Progress => {}
                    }
                },
                _ = renderer.search_progress() => {},
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep_until(deadline).await,
                        None => std::future::pending::<()>().await,
                    }
                } => {
                    input.busy = pending.is_some() || client.client().current_status()?
                        .is_some_and(|status| matches!(status.phase, acyclic_harness::agent::Phase::Running));
                    let size = terminal_size()?;
                    paint(&mut renderer, (&mut input, &mut viewport),
                        (&mut terminal, size, &mut shown), &start)?;
                }
            }
        }
        if batch.is_empty() { continue; }
        let size = terminal_size()?;
        renderer.layout(&input, size);
        let areas = renderer.screen().areas(Rect::from((Position::ORIGIN, size)), &input);
        let page_rows = usize::from(areas.transcript.height);
        let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
        client::input_burst(&mut batch, page_rows, &mut effects, |event| {
            areas.event(&mut input, event, &mut viewport, now, &mut |byte| renderer.anchor_at(byte))
        });
        for effect in effects.drain(..) {
            if let Some(rows) = client::transcript_motion(&effect, page_rows) {
                renderer.scroll(rows.saturating_neg());
                continue;
            }
            match effect {
                InputEffect::Submit { text, edit: None, .. } => {
                    if input.transcript_search() {
                        input.set_transcript_search(Some(&text));
                        renderer.search_step(false);
                        continue;
                    }
                    let resolution = match client.resolve_command(&text).await {
                        Ok(resolution) => resolution,
                        Err(client::LocalAgentError::Harness(error @ (
                            acyclic_harness::Error::Invalid(_) | acyclic_harness::Error::NotFound(_)
                            | acyclic_harness::Error::Unsupported(_)
                        ))) => {
                            input.set_chat_draft(&text);
                            renderer.notice(&error.to_string());
                            continue;
                        }
                        Err(error) => return Err(error.into()),
                    };
                    let prompt = match resolution {
                        client::CommandResolution::Prompt { text, .. } => text,
                        client::CommandResolution::Submit { request: client::CommandRequest::Quit } => {
                            return Ok(client::HeadlessOutcome::Completed);
                        }
                        client::CommandResolution::Select { query: client::CommandQuery::Shortcuts } => {
                            renderer.help(client.catalog(), &mut input);
                            continue;
                        }
                        client::CommandResolution::Select { query: client::CommandQuery::Find } => {
                            renderer.open_search(&mut input);
                            continue;
                        }
                        _ => {
                            input.set_chat_draft(&text);
                            renderer.notice("This command's actor binding is not available yet. Your input remains in the editor.");
                            continue;
                        }
                    };
                    if pending.is_some() {
                        input.set_chat_draft(&text);
                        renderer.notice("The submitted turn is still pending. Your next prompt remains in the editor.");
                        continue;
                    }
                    if prompt.is_empty() || prompt.len() > client::MAXIMUM_AGENT_PROMPT_BYTES {
                        input.set_chat_draft(&text);
                        renderer.notice("Prompt must contain 1..=65536 bytes.");
                        continue;
                    }
                    let id = IntentId::try_from(OperationId::new().to_string())?;
                    match client.prompt(&id, &prompt).await? {
                        Delivered::Committed { inbox } => pending = Some((id, inbox)),
                        Delivered::Closed => {
                            input.set_chat_draft(&text);
                            renderer.notice("The selected agent is closed. Your prompt remains in the editor.");
                        }
                    }
                }
                InputEffect::Quit => return Ok(client::HeadlessOutcome::Completed),
                InputEffect::Host(tui::Action::Escape) if renderer.close_panel(&mut input) => {},
                InputEffect::Host(tui::Action::App(tui::AppAction::Help)) => {
                    renderer.help(client.catalog(), &mut input);
                }
                InputEffect::Host(tui::Action::App(tui::AppAction::Find)) => {
                    renderer.open_search(&mut input);
                }
                InputEffect::Host(tui::Action::App(tui::AppAction::ClosePanel)) => {
                    renderer.close_panel(&mut input);
                }
                InputEffect::Host(tui::Action::App(tui::AppAction::ToggleFollowTail)) => renderer.seek(None),
                InputEffect::Host(tui::Action::App(tui::AppAction::Quit)) => return Ok(client::HeadlessOutcome::Completed),
                InputEffect::FindStep { previous } => renderer.search_step(previous),
                InputEffect::ScrollTo { anchor } => renderer.seek(anchor),
                InputEffect::Interrupt | InputEffect::Host(tui::Action::Escape) => {
                    let id = IntentId::try_from(OperationId::new().to_string())?;
                    client.cancel(&id).await?;
                }
                InputEffect::Submit { text, edit: Some(_), .. } => {
                    input.set_chat_draft(&text);
                    renderer.notice("Message editing is not available yet. Your text remains in the editor.");
                }
                // Port these effects to the actor-native command/file binding.
                // There is no retired TaskVolume workflow fallback.
                _ => renderer.notice("This action is not available yet."),
            }
        }
        input.busy = pending.is_some() || client.client().current_status()?
            .is_some_and(|status| matches!(status.phase, acyclic_harness::agent::Phase::Running));
        renderer.request_frame();
        // Pointer hit-testing sees the preceding frame even inside a single
        // terminal read. Other input is folded until that read's final frame.
        if !batch.is_empty() && client::frame_due(&batch) {
            if let Some(deadline) = renderer.frame_deadline() {
                tokio::time::sleep_until(deadline).await;
            }
            paint(&mut renderer, (&mut input, &mut viewport),
                (&mut terminal, terminal_size()?, &mut shown), &start)?;
        }
    }
}

fn paint<P: tui::term::Present>(
    renderer: &mut AgentRenderer,
    (input, viewport): (&mut PromptInput, &mut Viewport),
    (terminal, size, shown): (&mut FrameTerminal<P>, Size, &mut Option<(Rect, tui::TranscriptAnchor)>),
    start: &Instant,
) -> std::io::Result<()> {
    input.refresh_completions();
    renderer.layout(input, size);
    let screen = renderer.screen();
    let area = Rect::from((Position::ORIGIN, size));
    let scroll = renderer.view_scroll(area, input);
    let now = u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX);
    present(terminal, size, shown, false,
        (screen.areas(area, input).transcript, None, None), |frame| {
            screen.render(frame, input, viewport, scroll, now);
        })?;
    renderer.frame_presented();
    Ok(())
}

fn open_tty(inline: bool) -> std::io::Result<Tty> {
    let mut tty = Tty::open()?;
    tty.negotiate(&Environment::capture())?;
    tty.setup(if inline { ScreenMode::Inline } else { ScreenMode::Alternate })?;
    Ok(tty)
}

/// Shared device boundary. Remember position only after successful output.
pub(super) fn present<P: tui::term::Present>(
    terminal: &mut FrameTerminal<P>,
    size: Size,
    shown: &mut Option<(Rect, tui::TranscriptAnchor)>,
    regions: bool,
    (band, top, moved): (Rect, Option<tui::TranscriptAnchor>, Option<isize>),
    render: impl FnOnce(&mut tui::ratatui::Frame<'_>),
) -> std::io::Result<()> {
    let moved = moved
        .filter(|rows| *rows != 0 && regions && shown.is_some_and(|(before, _)| before == band))
        .map(|rows| RegionScroll { region: band, rows });
    terminal.draw_scrolled(size, moved, render)?;
    *shown = top.map(|top| (band, top));
    Ok(())
}
