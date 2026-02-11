// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use anyhow::Result;
use crossterm::{
    execute,
    style::{Color, Print, ResetColor, SetForegroundColor},
};
use std::io::{IsTerminal, Write};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::time::Duration;

pub(super) fn print_with_green_check_stdout(message: &str) -> Result<()> {
    let mut line = String::from("✓ ");
    line.push_str(message);
    line.push('\n');
    write_stdout_with_green_ticks(line.as_bytes())
}

pub(super) fn print_with_red_cross_stdout(message: &str) -> Result<()> {
    let mut line = String::from("✗ ");
    line.push_str(message);
    line.push('\n');
    write_stdout_with_green_ticks(line.as_bytes())
}

pub(super) fn print_with_green_check_stderr(message: &str) -> Result<()> {
    let mut line = String::from("✓ ");
    line.push_str(message);
    line.push('\n');
    write_stderr_with_colored_symbols(line.as_bytes())
}

pub(super) fn print_with_red_cross_stderr(message: &str) -> Result<()> {
    let mut line = String::from("✗ ");
    line.push_str(message);
    line.push('\n');
    write_stderr_with_colored_symbols(line.as_bytes())
}

fn write_stdout_with_green_ticks(bytes: &[u8]) -> Result<()> {
    let mut stdout = std::io::stdout();
    if !stdout.is_terminal() {
        return write_all(&mut stdout, bytes);
    }
    let text = match std::str::from_utf8(bytes) {
        Ok(v) => v,
        Err(_) => return write_all(&mut stdout, bytes),
    };
    if !text.contains('✓') && !text.contains('✗') {
        return write_all(&mut stdout, bytes);
    }
    for line in text.split_inclusive('\n') {
        if let Some(rest) = line.strip_prefix('✓') {
            execute!(
                stdout,
                SetForegroundColor(Color::Green),
                Print("✓"),
                ResetColor,
                Print(rest)
            )?;
        } else if let Some(rest) = line.strip_prefix('✗') {
            execute!(
                stdout,
                SetForegroundColor(Color::Red),
                Print("✗"),
                ResetColor,
                Print(rest)
            )?;
        } else {
            write_all(&mut stdout, line.as_bytes())?;
        }
    }
    Ok(())
}

fn write_stderr_with_colored_symbols(bytes: &[u8]) -> Result<()> {
    let mut stderr = std::io::stderr();
    if !stderr.is_terminal() {
        return write_all(&mut stderr, bytes);
    }
    let text = match std::str::from_utf8(bytes) {
        Ok(v) => v,
        Err(_) => return write_all(&mut stderr, bytes),
    };
    if !text.contains('✓') && !text.contains('✗') {
        return write_all(&mut stderr, bytes);
    }
    for line in text.split_inclusive('\n') {
        if let Some(rest) = line.strip_prefix('✓') {
            execute!(
                stderr,
                SetForegroundColor(Color::Green),
                Print("✓"),
                ResetColor,
                Print(rest)
            )?;
        } else if let Some(rest) = line.strip_prefix('✗') {
            execute!(
                stderr,
                SetForegroundColor(Color::Red),
                Print("✗"),
                ResetColor,
                Print(rest)
            )?;
        } else {
            write_all(&mut stderr, line.as_bytes())?;
        }
    }
    Ok(())
}

fn write_all<W: Write>(w: &mut W, buf: &[u8]) -> Result<()> {
    w.write_all(buf)?;
    w.flush()?;
    Ok(())
}

#[derive(Clone, Copy)]
pub(super) enum SpinnerTarget {
    Stdout,
    Stderr,
}

pub(super) struct Spinner {
    stop: Arc<AtomicBool>,
    handle: Option<std::thread::JoinHandle<()>>,
}

impl Spinner {
    pub(super) fn start(message: &str, target: SpinnerTarget) -> Option<Self> {
        let is_tty = match target {
            SpinnerTarget::Stdout => std::io::stdout().is_terminal(),
            SpinnerTarget::Stderr => std::io::stderr().is_terminal(),
        };
        if !is_tty {
            return None;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let stop_signal = Arc::clone(&stop);
        let message = message.to_string();
        let handle = std::thread::spawn(move || {
            let frames = ['⠾', '⠷', '⠯', '⠟', '⠻', '⠽'];
            let mut idx = 0usize;
            while !stop_signal.load(Ordering::Relaxed) {
                let frame = frames[idx % frames.len()];
                let line = format!("\r{} {}", frame, message);
                match target {
                    SpinnerTarget::Stdout => {
                        let mut stdout = std::io::stdout();
                        let _ = stdout.write_all(line.as_bytes());
                        let _ = stdout.flush();
                    }
                    SpinnerTarget::Stderr => {
                        let mut stderr = std::io::stderr();
                        let _ = stderr.write_all(line.as_bytes());
                        let _ = stderr.flush();
                    }
                }
                std::thread::sleep(Duration::from_millis(120));
                idx = idx.wrapping_add(1);
            }
            let clear_width = message.len() + 2;
            let clear = format!("\r{}{}\r", " ".repeat(clear_width), " ");
            match target {
                SpinnerTarget::Stdout => {
                    let mut stdout = std::io::stdout();
                    let _ = stdout.write_all(clear.as_bytes());
                    let _ = stdout.flush();
                }
                SpinnerTarget::Stderr => {
                    let mut stderr = std::io::stderr();
                    let _ = stderr.write_all(clear.as_bytes());
                    let _ = stderr.flush();
                }
            }
        });
        Some(Self {
            stop,
            handle: Some(handle),
        })
    }

    pub(super) fn stop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for Spinner {
    fn drop(&mut self) {
        self.stop();
    }
}
