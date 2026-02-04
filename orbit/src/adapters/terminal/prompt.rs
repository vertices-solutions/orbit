// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use anyhow::bail;
use crossterm::cursor;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::style::{Color, Print, ResetColor, SetForegroundColor};
use crossterm::terminal::{self, ClearType};
use std::io::{IsTerminal, Write};

const HINT_COLOR: Color = Color::DarkGrey;

pub(super) fn confirm_action(prompt: &str, hint: &str) -> anyhow::Result<bool> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        bail!("confirmation requires a TTY; pass --yes to skip the prompt");
    }
    prompt_yes_no(prompt, hint)
}

pub(super) fn prompt_yes_no(prompt: &str, hint: &str) -> anyhow::Result<bool> {
    loop {
        let input = prompt_line(prompt, hint)?;
        let normalized = input.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "y" | "yes" => return Ok(true),
            "n" | "no" | "" => return Ok(false),
            _ => eprintln!("Please answer 'yes' or 'no'."),
        }
    }
}

pub(super) fn prompt_line(prompt: &str, hint: &str) -> anyhow::Result<String> {
    prompt_line_with_default(prompt, hint, None)
}

pub(super) fn prompt_line_with_default(
    prompt: &str,
    hint: &str,
    default: Option<&str>,
) -> anyhow::Result<String> {
    Ok(prompt_line_with_default_result(prompt, hint, default)?.input)
}

struct PromptLineResult {
    input: String,
}

fn prompt_line_with_default_result(
    prompt: &str,
    hint: &str,
    default: Option<&str>,
) -> anyhow::Result<PromptLineResult> {
    let _guard = RawModeGuard::enter()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, cursor::MoveToColumn(0))?;
    let mut editor = LineEditor::new(prompt, 0, hint, default);
    if default.is_some() {
        editor.apply_default_if_empty();
    }
    editor.render(&mut stdout)?;
    loop {
        match event::read()? {
            Event::Key(key) => match key.code {
                KeyCode::Enter => {
                    if editor.apply_default_if_empty() {
                        editor.render(&mut stdout)?;
                    }
                    execute!(stdout, ResetColor, Print("\r\n"))?;
                    break;
                }
                KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                    execute!(stdout, ResetColor, Print("\r\n"))?;
                    bail!("prompt canceled");
                }
                KeyCode::Esc => {
                    execute!(stdout, ResetColor, Print("\r\n"))?;
                    bail!("prompt canceled");
                }
                KeyCode::Backspace => {
                    editor.backspace();
                }
                KeyCode::Delete => {
                    editor.delete();
                }
                KeyCode::Left => {
                    editor.move_left();
                }
                KeyCode::Right => {
                    if !editor.apply_default_if_empty() {
                        editor.move_right();
                    }
                }
                KeyCode::Tab => {
                    editor.apply_default_if_empty();
                }
                KeyCode::Home => {
                    editor.move_home();
                }
                KeyCode::End => {
                    editor.move_end();
                }
                KeyCode::Char(c) => {
                    if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT {
                        editor.insert(c);
                    }
                }
                _ => {}
            },
            _ => {}
        }
        editor.render(&mut stdout)?;
    }

    Ok(PromptLineResult {
        input: editor.into_string(),
    })
}

struct RawModeGuard;

impl RawModeGuard {
    fn enter() -> anyhow::Result<Self> {
        terminal::enable_raw_mode()?;
        Ok(Self)
    }
}

impl Drop for RawModeGuard {
    fn drop(&mut self) {
        let _ = terminal::disable_raw_mode();
    }
}

struct LineEditor {
    prompt: String,
    prompt_len: u16,
    hint: String,
    default_value: Option<String>,
    start_col: u16,
    buffer: Vec<char>,
    cursor: usize,
    has_typed: bool,
}

impl LineEditor {
    fn new(prompt: &str, start_col: u16, hint: &str, default: Option<&str>) -> Self {
        Self {
            prompt: prompt.to_string(),
            prompt_len: prompt.len().min(u16::MAX as usize) as u16,
            hint: hint.to_string(),
            default_value: default.map(|value| value.to_string()),
            start_col,
            buffer: Vec::new(),
            cursor: 0,
            has_typed: false,
        }
    }

    fn insert(&mut self, ch: char) {
        self.buffer.insert(self.cursor, ch);
        self.cursor += 1;
        self.has_typed = true;
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        self.cursor -= 1;
        self.buffer.remove(self.cursor);
        self.has_typed = true;
    }

    fn delete(&mut self) {
        if self.cursor >= self.buffer.len() {
            return;
        }
        self.buffer.remove(self.cursor);
        self.has_typed = true;
    }

    fn move_left(&mut self) {
        if self.cursor > 0 {
            self.cursor -= 1;
        }
    }

    fn move_right(&mut self) {
        if self.cursor < self.buffer.len() {
            self.cursor += 1;
        }
    }

    fn move_home(&mut self) {
        self.cursor = 0;
    }

    fn move_end(&mut self) {
        self.cursor = self.buffer.len();
    }

    fn render(&self, stdout: &mut std::io::Stdout) -> anyhow::Result<()> {
        let buffer_string: String = self.buffer.iter().collect();
        let term_width = terminal::size().map(|(w, _)| w).unwrap_or(80);
        let max_cols = term_width.saturating_sub(self.start_col);
        let available = max_cols.saturating_sub(self.prompt_len) as usize;
        execute!(
            stdout,
            cursor::MoveToColumn(self.start_col),
            terminal::Clear(ClearType::UntilNewLine),
            Print(&self.prompt),
        )?;
        if buffer_string.is_empty() {
            if !self.hint.is_empty() {
                let hint = truncate_display_text(&self.hint, available);
                execute!(
                    stdout,
                    SetForegroundColor(HINT_COLOR),
                    Print(&hint),
                    ResetColor,
                )?;
            }
        } else {
            let (visible, _) = visible_buffer_segment(&self.buffer, self.cursor, available);
            execute!(stdout, Print(visible))?;
        }
        let cursor_offset = if buffer_string.is_empty() {
            0
        } else {
            let (_, offset) = visible_buffer_segment(&self.buffer, self.cursor, available);
            offset
        };
        let cursor_col = self
            .start_col
            .saturating_add(self.prompt_len)
            .saturating_add(cursor_offset.min(u16::MAX as usize) as u16)
            .min(term_width.saturating_sub(1));
        execute!(stdout, cursor::MoveToColumn(cursor_col))?;
        stdout.flush()?;
        Ok(())
    }

    fn apply_default_if_empty(&mut self) -> bool {
        if !self.buffer.is_empty() {
            return false;
        }
        let Some(default_value) = self.default_value.as_ref() else {
            return false;
        };
        self.buffer.extend(default_value.chars());
        self.cursor = self.buffer.len();
        self.has_typed = true;
        true
    }

    fn into_string(self) -> String {
        self.buffer.into_iter().collect()
    }
}

fn truncate_display_text(input: &str, max_len: usize) -> String {
    if max_len == 0 {
        return String::new();
    }
    input.chars().take(max_len).collect()
}

fn visible_buffer_segment(buffer: &[char], cursor: usize, max_len: usize) -> (String, usize) {
    if max_len == 0 {
        return (String::new(), 0);
    }
    let len = buffer.len();
    if len <= max_len {
        let visible: String = buffer.iter().collect();
        return (visible, cursor.min(len));
    }
    let mut start = 0usize;
    if cursor >= max_len {
        start = cursor + 1 - max_len;
    }
    if start + max_len > len {
        start = len - max_len;
    }
    let end = (start + max_len).min(len);
    let visible: String = buffer[start..end].iter().collect();
    let offset = cursor.saturating_sub(start).min(max_len.saturating_sub(1));
    (visible, offset)
}
