// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use crate::args::AddClusterArgs;
use anyhow::bail;
use crossterm::cursor;
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::execute;
use crossterm::style::{Color, Print, ResetColor, SetForegroundColor};
use crossterm::terminal::{self, ClearType};
use rand::prelude::IndexedRandom;
use std::io::{IsTerminal, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

const DEFAULT_SSH_PORT: u32 = 22;
const DEFAULT_IDENTITY_PATH: &str = "~/.ssh/id_ed25519";
const DEFAULT_BASE_PATH: &str = "~/runs";
const HINT_COLOR: Color = Color::DarkGrey;
const CONNECT_TIMEOUT_SECS: u64 = 3;

#[derive(Debug)]
pub struct ResolvedAddClusterArgs {
    pub hostname: Option<String>,
    pub ip: Option<String>,
    pub username: String,
    pub name: String,
    pub port: u32,
    pub identity_path: String,
    pub default_base_path: Option<String>,
}

pub fn prompt_default_base_path(home_dir: &str) -> anyhow::Result<String> {
    ensure_tty_for_prompt()?;
    let default_value = std::path::PathBuf::from(home_dir)
        .join("runs")
        .to_string_lossy()
        .into_owned();
    prompt_with_default(
        "Default base path",
        &default_value,
        "Remote base folder for projects.",
    )
}

pub fn resolve_add_cluster_args(args: AddClusterArgs) -> anyhow::Result<ResolvedAddClusterArgs> {
    let destination = normalize_option(args.destination);
    let name = normalize_option(args.name);
    let identity_path = normalize_option(args.identity_path);
    let default_base_path = normalize_option(args.default_base_path);

    let needs_prompt = !args.headless
        && (destination.is_none()
            || name.is_none()
            || identity_path.is_none()
            || default_base_path.is_none());
    if needs_prompt {
        ensure_tty_for_prompt()?;
    }

    let mut destination_prompted = false;
    let parsed_destination = if let Some(value) = destination.as_deref() {
        let parsed = parse_destination(value)?;
        validate_destination_reachability(&parsed)?;
        parsed
    } else {
        if args.headless {
            bail!("destination is required in headless mode");
        }
        destination_prompted = true;
        prompt_destination()?
    };

    if !destination_prompted {
        if let Some(value) = destination.as_deref() {
            print_selected_value("Destination", value);
        }
    }

    let (hostname, ip) = if parsed_destination.host.parse::<IpAddr>().is_ok() {
        (None, Some(parsed_destination.host.clone()))
    } else {
        (Some(parsed_destination.host.clone()), None)
    };

    let username = match parsed_destination.username.clone() {
        Some(value) => value,
        None => bail!("destination username is required"),
    };

    let mut name_prompted = false;
    let name = match name {
        Some(value) => value,
        None => {
            if args.headless {
                default_name_for_host(hostname.as_deref(), ip.as_deref())?
            } else {
                let default_name = generate_random_name();
                name_prompted = true;
                prompt_with_default(
                    "Name",
                    &default_name,
                    "Friendly name used in other commands (e.g. gpu01).",
                )?
            }
        }
    };
    if !name_prompted {
        print_selected_value("Name", &name);
    }

    let port = parsed_destination.port.unwrap_or(DEFAULT_SSH_PORT);

    let mut identity_prompted = false;
    let identity_path = match identity_path {
        Some(value) => value,
        None => {
            if args.headless {
                DEFAULT_IDENTITY_PATH.to_string()
            } else {
                identity_prompted = true;
                prompt_with_default(
                    "Identity path",
                    DEFAULT_IDENTITY_PATH,
                    "SSH private key path used for authentication.",
                )?
            }
        }
    };
    if !identity_prompted {
        print_selected_value("Identity path", &identity_path);
    }

    let default_base_path = match default_base_path {
        Some(value) => Some(value),
        None => {
            if args.headless {
                Some(DEFAULT_BASE_PATH.to_string())
            } else {
                None
            }
        }
    };
    if let Some(ref value) = default_base_path {
        print_selected_value("Default base path", value);
    }

    Ok(ResolvedAddClusterArgs {
        hostname,
        ip,
        username,
        name,
        port,
        identity_path,
        default_base_path,
    })
}

#[derive(Debug)]
struct ParsedDestination {
    username: Option<String>,
    host: String,
    port: Option<u32>,
}

fn print_selected_value(label: &str, value: &str) {
    println!("{label}: {value}");
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

pub fn confirm_action(prompt: &str, hint: &str) -> anyhow::Result<bool> {
    if !std::io::stdin().is_terminal() || !std::io::stdout().is_terminal() {
        bail!("confirmation requires a TTY; pass --yes to skip the prompt");
    }
    loop {
        let input = prompt_line_with_default(prompt, hint, Some("no"))?;
        let normalized = input.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "y" | "yes" => return Ok(true),
            "n" | "no" | "" => return Ok(false),
            _ => eprintln!("Please answer 'yes' or 'no'."),
        }
    }
}

fn prompt_destination() -> anyhow::Result<ParsedDestination> {
    loop {
        let input = prompt_line(
            "Destination: ",
            "SSH destination in user@host[:port] form.",
        )?;
        let trimmed = input.trim();
        if trimmed.is_empty() {
            eprintln!("Please, provide a valid connection string");
            continue;
        }
        match parse_destination(trimmed)
            .and_then(|parsed| {
                validate_destination_reachability(&parsed)?;
                Ok(parsed)
            }) {
            Ok(parsed) => return Ok(parsed),
            Err(_) => eprintln!("Please, provide a valid connection string"),
        }
    }
}

fn parse_destination(input: &str) -> anyhow::Result<ParsedDestination> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("destination is required");
    }

    let (username_part, host_part) = match trimmed.split_once('@') {
        Some((user, host)) => {
            if user.trim().is_empty() {
                bail!("destination username is required");
            }
            (Some(user.trim().to_string()), host.trim())
        }
        None => bail!("destination must be in user@host[:port] format"),
    };

    let host_part = host_part.trim();
    if host_part.is_empty() {
        bail!("destination host is required");
    }

    let (host, port) = if let Some(rest) = host_part.strip_prefix('[') {
        let Some((host, remainder)) = rest.split_once(']') else {
            bail!("destination IPv6 host must end with ']'");
        };
        let remainder = remainder.trim();
        if remainder.is_empty() {
            (host.to_string(), None)
        } else if let Some(port_str) = remainder.strip_prefix(':') {
            (host.to_string(), Some(parse_destination_port(port_str)?))
        } else {
            bail!("destination has unexpected trailing characters after ']'");
        }
    } else if host_part.matches(':').count() > 1 {
        if host_part.parse::<IpAddr>().is_err() {
            bail!("destination host must be IPv6 when using multiple ':' characters");
        }
        (host_part.to_string(), None)
    } else if let Some((host, port_str)) = host_part.rsplit_once(':') {
        if host.trim().is_empty() {
            bail!("destination host is required");
        }
        (
            host.trim().to_string(),
            Some(parse_destination_port(port_str)?),
        )
    } else {
        (host_part.to_string(), None)
    };

    Ok(ParsedDestination {
        username: username_part,
        host,
        port,
    })
}

fn parse_destination_port(value: &str) -> anyhow::Result<u32> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("destination port is required after ':'");
    }
    match trimmed.parse::<u32>() {
        Ok(port) => Ok(port),
        Err(_) => bail!("destination port must be a number"),
    }
}

fn validate_destination_reachability(destination: &ParsedDestination) -> anyhow::Result<()> {
    let port = resolve_destination_port(destination.port)?;
    let host = destination.host.as_str();

    if let Ok(ip) = host.parse::<IpAddr>() {
        if is_socket_reachable(SocketAddr::new(ip, port)) {
            return Ok(());
        }
        bail!("destination host is unreachable");
    }

    let mut addrs = (host, port)
        .to_socket_addrs()
        .map_err(|_| anyhow::anyhow!("destination host could not be resolved"))?;
    let mut resolved = false;
    while let Some(addr) = addrs.next() {
        resolved = true;
        if is_socket_reachable(addr) {
            return Ok(());
        }
    }

    if !resolved {
        bail!("destination host could not be resolved");
    }

    bail!("destination host is unreachable");
}

fn resolve_destination_port(port: Option<u32>) -> anyhow::Result<u16> {
    let port = port.unwrap_or(DEFAULT_SSH_PORT);
    if port == 0 || port > u16::MAX as u32 {
        bail!("destination port must be between 1 and 65535");
    }
    Ok(port as u16)
}

fn is_socket_reachable(addr: SocketAddr) -> bool {
    let timeout = Duration::from_secs(CONNECT_TIMEOUT_SECS);
    TcpStream::connect_timeout(&addr, timeout).is_ok()
}

fn default_name_for_host(hostname: Option<&str>, ip: Option<&str>) -> anyhow::Result<String> {
    if let Some(host) = hostname {
        if host.parse::<IpAddr>().is_ok() {
            return Ok(generate_random_name());
        }
        return Ok(host.to_string());
    }
    if ip.is_some() {
        return Ok(generate_random_name());
    }
    bail!("name default requires a hostname or ip")
}

fn generate_random_name() -> String {
    let mut rng = rand::rng();
    let adjective = ADJECTIVES.choose(&mut rng).copied().unwrap_or("curious");
    let scientist = SCIENTISTS.choose(&mut rng).copied().unwrap_or("einstein");
    format!("{adjective}_{scientist}")
}

fn prompt_with_default(label: &str, default: &str, hint: &str) -> anyhow::Result<String> {
    let hint = format_default_hint(default, hint);
    let input = prompt_line_with_default(&format!("{label}: "), &hint, Some(default))?;
    let trimmed = input.trim();
    if trimmed.is_empty() {
        Ok(default.to_string())
    } else {
        Ok(trimmed.to_string())
    }
}

fn prompt_line(prompt: &str, hint: &str) -> anyhow::Result<String> {
    prompt_line_with_default(prompt, hint, None)
}

fn format_default_hint(default: &str, hint: &str) -> String {
    let trimmed = hint.trim();
    if trimmed.is_empty() {
        default.to_string()
    } else {
        format!("{default} - {trimmed}")
    }
}

fn prompt_line_with_default(
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
        execute!(
            stdout,
            cursor::MoveToColumn(self.start_col),
            terminal::Clear(ClearType::UntilNewLine),
            Print(&self.prompt),
        )?;
        if buffer_string.is_empty() {
            if !self.hint.is_empty() {
                execute!(
                    stdout,
                    SetForegroundColor(HINT_COLOR),
                    Print(&self.hint),
                    ResetColor,
                )?;
            }
        } else {
            execute!(stdout, Print(&buffer_string))?;
        }
        let cursor_col = self
            .start_col
            .saturating_add(self.prompt_len)
            .saturating_add(self.cursor.min(u16::MAX as usize) as u16);
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

const ADJECTIVES: &[&str] = &[
    "bold", "brisk", "calm", "clever", "curious", "daring", "eager", "gentle", "grand", "happy",
    "jolly", "kind", "lively", "mighty", "noble", "proud", "quiet", "rapid", "sharp", "witty",
];

const SCIENTISTS: &[&str] = &[
    "bohr", "curie", "darwin", "einstein", "fermi", "feynman", "gauss", "galilei", "goodall",
    "hawking", "kepler", "lovelace", "mendel", "newton", "noether", "planck", "tesla", "turing",
    "weber", "wilson",
];

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::TcpListener;

    #[test]
    fn resolve_add_cluster_headless_defaults() {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                return;
            }
            Err(err) => panic!("failed to bind listener: {err}"),
        };
        let port = listener.local_addr().unwrap().port();
        let args = AddClusterArgs {
            destination: Some(format!("alex@localhost:{port}")),
            name: None,
            identity_path: None,
            default_base_path: None,
            headless: true,
        };

        let resolved = resolve_add_cluster_args(args).unwrap();
        assert_eq!(resolved.port, port as u32);
        assert_eq!(resolved.identity_path, DEFAULT_IDENTITY_PATH);
        assert_eq!(resolved.default_base_path.as_deref(), Some(DEFAULT_BASE_PATH));
        assert_eq!(resolved.username, "alex");
        assert_eq!(resolved.name, "localhost");
    }

    #[test]
    fn resolve_add_cluster_headless_requires_destination() {
        let args = AddClusterArgs {
            destination: None,
            name: None,
            identity_path: Some("~/.ssh/id_rsa".into()),
            default_base_path: None,
            headless: true,
        };

        let err = resolve_add_cluster_args(args).unwrap_err();
        assert!(err.to_string().contains("destination"));
    }

    #[test]
    fn parse_destination_with_user_port() {
        let parsed = parse_destination("user@example.com:2222").unwrap();
        assert_eq!(parsed.username.as_deref(), Some("user"));
        assert_eq!(parsed.host, "example.com");
        assert_eq!(parsed.port, Some(2222));
    }

    #[test]
    fn parse_destination_requires_username() {
        let err = parse_destination("example.com:2222").unwrap_err();
        assert!(err
            .to_string()
            .contains("destination must be in user@host[:port] format"));
    }

    #[test]
    fn resolve_add_cluster_headless_destination_reachable() {
        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(err) if err.kind() == std::io::ErrorKind::PermissionDenied => {
                return;
            }
            Err(err) => panic!("failed to bind listener: {err}"),
        };
        let port = listener.local_addr().unwrap().port();
        let args = AddClusterArgs {
            destination: Some(format!("alex@127.0.0.1:{port}")),
            name: Some("local".into()),
            identity_path: None,
            default_base_path: None,
            headless: true,
        };

        let resolved = resolve_add_cluster_args(args).unwrap();
        assert_eq!(resolved.username, "alex");
        assert_eq!(resolved.port, port as u32);
        assert_eq!(resolved.name, "local");
    }

    #[test]
    fn random_name_uses_known_words() {
        let name = generate_random_name();
        let mut parts = name.split('_');
        let adjective = parts.next().unwrap_or_default();
        let scientist = parts.next().unwrap_or_default();
        assert!(ADJECTIVES.contains(&adjective));
        assert!(SCIENTISTS.contains(&scientist));
    }
}
