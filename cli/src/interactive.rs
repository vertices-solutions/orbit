use crate::args::AddClusterArgs;
use anyhow::bail;
use crossterm::cursor;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::style::{Color, Print, ResetColor, SetForegroundColor};
use crossterm::terminal::{self, ClearType};
use std::io::{IsTerminal, Write};
use std::net::IpAddr;

const DEFAULT_SSH_PORT: u32 = 22;
const DEFAULT_IDENTITY_PATH: &str = "~/.ssh/id_ed25519";
const INPUT_COLOR: Color = Color::Magenta;

#[derive(Debug)]
pub struct ResolvedAddClusterArgs {
    pub hostname: Option<String>,
    pub ip: Option<String>,
    pub username: String,
    pub hostid: String,
    pub port: u32,
    pub identity_path: String,
    pub default_base_path: Option<String>,
}

pub fn resolve_add_cluster_args(args: AddClusterArgs) -> anyhow::Result<ResolvedAddClusterArgs> {
    let hostname = normalize_option(args.hostname);
    let ip = normalize_option(args.ip);
    let username = normalize_option(args.username);
    let hostid = normalize_option(args.hostid);
    let identity_path = normalize_option(args.identity_path);
    let default_base_path = normalize_option(args.default_base_path);

    if args.headless {
        let mut missing = Vec::new();
        if hostname.is_none() && ip.is_none() {
            missing.push("--hostname/--ip");
        }
        if username.is_none() {
            missing.push("--username");
        }
        if hostid.is_none() {
            missing.push("--hostid");
        }
        if !missing.is_empty() {
            bail!(
                "missing required options in headless mode: {}",
                missing.join(", ")
            );
        }
    }

    let needs_prompt = !args.headless
        && ((hostname.is_none() && ip.is_none())
            || username.is_none()
            || hostid.is_none()
            || args.port.is_none()
            || identity_path.is_none()
            || default_base_path.is_none());
    if needs_prompt {
        ensure_tty_for_prompt()?;
    }

    let (hostname, ip) = match (hostname, ip) {
        (Some(hostname), None) => (Some(hostname), None),
        (None, Some(ip)) => (None, Some(ip)),
        (None, None) => {
            if args.headless {
                bail!("hostname or ip is required in headless mode");
            }
            prompt_host()?
        }
        (Some(_hostname), Some(_ip)) => {
            bail!("hostname and ip cannot be provided at the same time");
        }
    };

    let username = match username {
        Some(value) => value,
        None => {
            if args.headless {
                bail!("--username is required in headless mode");
            }
            prompt_required("Username")?
        }
    };

    let hostid = match hostid {
        Some(value) => value,
        None => {
            if args.headless {
                bail!("--hostid is required in headless mode");
            }
            prompt_required("Host ID")?
        }
    };

    let port = match args.port {
        Some(value) => value,
        None => {
            if args.headless {
                DEFAULT_SSH_PORT
            } else {
                prompt_port(DEFAULT_SSH_PORT)?
            }
        }
    };

    let identity_path = match identity_path {
        Some(value) => value,
        None => {
            if args.headless {
                DEFAULT_IDENTITY_PATH.to_string()
            } else {
                prompt_with_default("Identity path", DEFAULT_IDENTITY_PATH)?
            }
        }
    };

    let default_base_path = match default_base_path {
        Some(value) => Some(value),
        None => {
            if args.headless {
                None
            } else {
                prompt_optional("Default base path")?
            }
        }
    };

    Ok(ResolvedAddClusterArgs {
        hostname,
        ip,
        username,
        hostid,
        port,
        identity_path,
        default_base_path,
    })
}

fn normalize_option(value: Option<String>) -> Option<String> {
    value.and_then(|item| {
        let trimmed = item.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn ensure_tty_for_prompt() -> anyhow::Result<()> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        bail!("interactive prompts require a TTY; pass --headless and specify all options");
    }
    Ok(())
}

fn prompt_host() -> anyhow::Result<(Option<String>, Option<String>)> {
    loop {
        let input = prompt_line("Hostname or IP address: ")?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            eprintln!("Hostname or IP address is required.");
            continue;
        }
        if trimmed.parse::<IpAddr>().is_ok() {
            return Ok((None, Some(trimmed.to_string())));
        }
        return Ok((Some(trimmed.to_string()), None));
    }
}

fn prompt_required(label: &str) -> anyhow::Result<String> {
    loop {
        let input = prompt_line(&format!("{label}: "))?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            eprintln!("{label} is required.");
            continue;
        }
        return Ok(trimmed.to_string());
    }
}

fn prompt_with_default(label: &str, default: &str) -> anyhow::Result<String> {
    let input = prompt_line_with_prefill(&format!("{label}: "), default)?;
    let trimmed = input.trim();
    if trimmed.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

fn prompt_optional(label: &str) -> anyhow::Result<Option<String>> {
    let input = prompt_line_with_prefill(&format!("{label}: "), "")?;
    let trimmed = input.trim();
    if trimmed.is_empty() {
        Ok(None)
    } else {
        Ok(Some(trimmed.to_string()))
    }
}

fn prompt_port(default: u32) -> anyhow::Result<u32> {
    loop {
        let input = prompt_line_with_prefill("Port: ", &default.to_string())?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            return Ok(default);
        }
        match trimmed.parse::<u32>() {
            Ok(value) => return Ok(value),
            Err(_) => eprintln!("Invalid port; expected a number."),
        }
    }
}

fn prompt_line(prompt: &str) -> anyhow::Result<String> {
    prompt_line_with_prefill(prompt, "")
}

fn prompt_line_with_prefill(prompt: &str, initial: &str) -> anyhow::Result<String> {
    let _guard = RawModeGuard::enter()?;
    let mut stdout = std::io::stdout();
    execute!(stdout, cursor::MoveToColumn(0))?;
    let mut editor = LineEditor::new(prompt, 0, initial);
    editor.render(&mut stdout)?;

    loop {
        match event::read()? {
            Event::Key(key) => match key.code {
                KeyCode::Enter => {
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
                    editor.move_right();
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

    Ok(editor.into_string())
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
    start_col: u16,
    buffer: Vec<char>,
    cursor: usize,
}

impl LineEditor {
    fn new(prompt: &str, start_col: u16, initial: &str) -> Self {
        let buffer: Vec<char> = initial.chars().collect();
        let cursor = buffer.len();
        Self {
            prompt: prompt.to_string(),
            prompt_len: prompt.len().min(u16::MAX as usize) as u16,
            start_col,
            buffer,
            cursor,
        }
    }

    fn insert(&mut self, ch: char) {
        self.buffer.insert(self.cursor, ch);
        self.cursor += 1;
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        self.cursor -= 1;
        self.buffer.remove(self.cursor);
    }

    fn delete(&mut self) {
        if self.cursor >= self.buffer.len() {
            return;
        }
        self.buffer.remove(self.cursor);
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
        execute!(
            stdout,
            cursor::MoveToColumn(self.start_col),
            terminal::Clear(ClearType::UntilNewLine),
            Print(&self.prompt),
            SetForegroundColor(INPUT_COLOR),
            Print(buffer_string),
            ResetColor,
        )?;
        let cursor_col = self
            .start_col
            .saturating_add(self.prompt_len)
            .saturating_add(self.cursor.min(u16::MAX as usize) as u16);
        execute!(stdout, cursor::MoveToColumn(cursor_col))?;
        stdout.flush()?;
        Ok(())
    }

    fn into_string(self) -> String {
        self.buffer.into_iter().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_add_cluster_headless_defaults() {
        let args = AddClusterArgs {
            hostname: Some("example.com".into()),
            ip: None,
            username: Some("alex".into()),
            hostid: Some("test".into()),
            port: None,
            identity_path: None,
            default_base_path: None,
            headless: true,
        };

        let resolved = resolve_add_cluster_args(args).unwrap();
        assert_eq!(resolved.port, DEFAULT_SSH_PORT);
        assert_eq!(resolved.identity_path, DEFAULT_IDENTITY_PATH);
        assert!(resolved.default_base_path.is_none());
    }

    #[test]
    fn resolve_add_cluster_headless_requires_hostid() {
        let args = AddClusterArgs {
            hostname: Some("example.com".into()),
            ip: None,
            username: Some("alex".into()),
            hostid: None,
            port: Some(2222),
            identity_path: Some("~/.ssh/id_rsa".into()),
            default_base_path: None,
            headless: true,
        };

        let err = resolve_add_cluster_args(args).unwrap_err();
        assert!(err.to_string().contains("--hostid"));
    }
}
