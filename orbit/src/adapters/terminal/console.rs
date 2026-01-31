// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use anyhow::Result;
use crossterm::{
    execute,
    style::{Color, Print, ResetColor, SetForegroundColor},
};
use std::io::{IsTerminal, Write};

pub(super) fn print_with_green_check_stdout(message: &str) -> Result<()> {
    let mut line = String::from("✓ ");
    line.push_str(message);
    line.push('\n');
    write_stdout_with_green_ticks(line.as_bytes())
}

pub(super) fn print_with_green_check_stderr(message: &str) -> Result<()> {
    let mut line = String::from("✓ ");
    line.push_str(message);
    line.push('\n');
    write_stderr_with_green_ticks(line.as_bytes())
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
    if !text.contains('✓') {
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
        } else {
            write_all(&mut stdout, line.as_bytes())?;
        }
    }
    Ok(())
}

fn write_stderr_with_green_ticks(bytes: &[u8]) -> Result<()> {
    let mut stderr = std::io::stderr();
    if !stderr.is_terminal() {
        return write_all(&mut stderr, bytes);
    }
    let text = match std::str::from_utf8(bytes) {
        Ok(v) => v,
        Err(_) => return write_all(&mut stderr, bytes),
    };
    if !text.contains('✓') {
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
