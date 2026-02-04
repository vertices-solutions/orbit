// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use anyhow::bail;
use crossterm::{
    cursor,
    event::{self, Event, KeyCode},
    execute, terminal,
};
use ratatui::{
    Terminal, TerminalOptions, Viewport,
    backend::{Backend, ClearType, CrosstermBackend},
    layout::{Constraint, Direction, Layout, Position},
    prelude::Frame,
    style::{Color, Modifier, Style},
    text::{Line, Text},
    widgets::{Clear, List, ListItem, ListState, Paragraph},
};
use std::io::IsTerminal;

struct TerminalGuard;

impl TerminalGuard {
    fn enter() -> anyhow::Result<Self> {
        terminal::enable_raw_mode()?;
        let mut stdout = std::io::stdout();
        if let Err(err) = execute!(stdout, cursor::Hide) {
            let _ = terminal::disable_raw_mode();
            return Err(err.into());
        }
        Ok(Self)
    }
}

impl Drop for TerminalGuard {
    fn drop(&mut self) {
        let mut stdout = std::io::stdout();
        let _ = execute!(stdout, cursor::Show);
        let _ = terminal::disable_raw_mode();
    }
}

type RatatuiTerminal = Terminal<CrosstermBackend<std::io::Stdout>>;

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
enum EnumPickerPhase {
    Selecting,
    Selected(usize),
    Canceled,
}

struct EnumPicker {
    name: String,
    help: String,
    values: Vec<String>,
    list_state: ListState,
    phase: EnumPickerPhase,
    viewport_height: usize,
}

impl EnumPicker {
    fn new(name: &str, help: &str, values: Vec<String>, default: Option<&str>) -> Self {
        let mut list_state = ListState::default();
        let default_index = default
            .and_then(|value| values.iter().position(|item| item == value))
            .unwrap_or(0);
        list_state.select(Some(default_index));
        let viewport_height = values.len().min(8).max(1);
        Self {
            name: name.to_string(),
            help: help.to_string(),
            values,
            list_state,
            phase: EnumPickerPhase::Selecting,
            viewport_height,
        }
    }

    fn header_lines(&self) -> usize {
        if self.help.is_empty() { 1 } else { 2 }
    }

    fn handle_event(&mut self, event: Event) {
        if self.phase != EnumPickerPhase::Selecting {
            return;
        }
        let len = self.values.len();
        if len == 0 {
            self.phase = EnumPickerPhase::Canceled;
            return;
        }
        if let Event::Key(key) = event {
            match key.code {
                KeyCode::Up => self.move_selection(-1),
                KeyCode::Down => self.move_selection(1),
                KeyCode::PageUp => self.move_selection(-(self.viewport_height as isize)),
                KeyCode::PageDown => self.move_selection(self.viewport_height as isize),
                KeyCode::Home => self.select_index(0),
                KeyCode::End => self.select_index(len.saturating_sub(1)),
                KeyCode::Enter => {
                    let selected = self.list_state.selected().unwrap_or(0);
                    self.phase = EnumPickerPhase::Selected(selected);
                }
                KeyCode::Esc | KeyCode::Char('q') | KeyCode::Char('Q') => {
                    self.phase = EnumPickerPhase::Canceled;
                }
                KeyCode::Char('c') if key.modifiers.contains(event::KeyModifiers::CONTROL) => {
                    self.phase = EnumPickerPhase::Canceled;
                }
                _ => {}
            }
        }
    }

    fn select_index(&mut self, index: usize) {
        let index = index.min(self.values.len().saturating_sub(1));
        self.list_state.select(Some(index));
    }

    fn move_selection(&mut self, delta: isize) {
        let len = self.values.len();
        if len == 0 {
            return;
        }
        let current = self.list_state.selected().unwrap_or(0) as isize;
        let next = (current + delta).clamp(0, (len - 1) as isize) as usize;
        self.list_state.select(Some(next));
    }

    fn render(&mut self, frame: &mut Frame) {
        let area = frame.area();
        frame.render_widget(Clear, area);

        let items: Vec<ListItem> = self
            .values
            .iter()
            .map(|value| ListItem::new(value.as_str()))
            .collect();
        let list = List::new(items)
            .highlight_style(Style::default().add_modifier(Modifier::BOLD))
            .highlight_symbol("> ");

        let header_lines = self.header_lines() as u16;
        let footer_lines = 1u16;
        let list_area = if area.height >= header_lines + footer_lines + 1 {
            let layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([
                    Constraint::Length(header_lines),
                    Constraint::Min(1),
                    Constraint::Length(footer_lines),
                ])
                .split(area);

            let mut header_text = vec![Line::from(format!("Select value for {}", self.name))];
            if !self.help.is_empty() {
                header_text.push(
                    Line::from(self.help.as_str()).style(
                        Style::default()
                            .fg(Color::DarkGray)
                            .add_modifier(Modifier::DIM),
                    ),
                );
            }
            let header = Paragraph::new(Text::from(header_text));
            frame.render_widget(header, layout[0]);

            let footer = Paragraph::new("Up/Down to move, Enter to select, Esc to cancel").style(
                Style::default()
                    .fg(Color::DarkGray)
                    .add_modifier(Modifier::DIM),
            );
            frame.render_widget(footer, layout[2]);
            layout[1]
        } else if area.height > header_lines {
            let layout = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(header_lines), Constraint::Min(1)])
                .split(area);
            let header = Paragraph::new(format!("Select value for {}", self.name));
            frame.render_widget(header, layout[0]);
            layout[1]
        } else {
            area
        };

        frame.render_stateful_widget(list, list_area, &mut self.list_state);
        self.viewport_height = list_area.height as usize;
    }

    fn inline_viewport_height(&self) -> u16 {
        let header = self.header_lines() as u16;
        let footer = 1u16;
        let max_list = 8u16;
        let list_height = (self.values.len().max(1) as u16).min(max_list);
        let desired = header + footer + list_height;
        desired.max(1)
    }

    fn clear(&mut self, terminal: &mut RatatuiTerminal, start: Position) -> anyhow::Result<()> {
        terminal.backend_mut().set_cursor_position(start)?;
        terminal
            .backend_mut()
            .clear_region(ClearType::AfterCursor)?;
        Ok(())
    }
}

pub(super) fn pick_enum_value(
    name: &str,
    help: &str,
    values: Vec<String>,
    default: Option<&str>,
) -> anyhow::Result<String> {
    if values.is_empty() {
        bail!("template enum '{name}' has no available values");
    }
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        bail!(
            "enum selection for '{name}' requires a TTY; pass --field {name}=VALUE to skip the prompt"
        );
    }
    let _guard = TerminalGuard::enter()?;
    let (cursor_x, cursor_y) = cursor::position()?;
    let (_, term_height) = terminal::size()?;
    let mut picker = EnumPicker::new(name, help, values, default);
    let desired_height = picker.inline_viewport_height();
    let viewport_height = desired_height.min(term_height.max(1));
    let lines_after_cursor = viewport_height.saturating_sub(1);
    let available_lines = term_height.saturating_sub(cursor_y).saturating_sub(1);
    let scroll_lines = lines_after_cursor.saturating_sub(available_lines);
    let start_pos = Position::new(cursor_x, cursor_y.saturating_sub(scroll_lines));
    let mut terminal = Terminal::with_options(
        CrosstermBackend::new(std::io::stdout()),
        TerminalOptions {
            viewport: Viewport::Inline(viewport_height),
        },
    )?;

    loop {
        terminal.draw(|frame| picker.render(frame))?;
        picker.handle_event(event::read()?);
        match picker.phase {
            EnumPickerPhase::Selecting => {}
            EnumPickerPhase::Selected(index) => {
                picker.clear(&mut terminal, start_pos)?;
                return Ok(picker.values[index].clone());
            }
            EnumPickerPhase::Canceled => {
                picker.clear(&mut terminal, start_pos)?;
                bail!("enum selection canceled");
            }
        }
    }
}
