//! zc — a minimal terminal client for the ZCode agent runtime.
//!
//! Talks to `zcode.cjs app-server` over newline-delimited JSON-RPC on stdio,
//! so it reuses ZCode's tools, sessions, config and login without Electron.

use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{BufRead, BufReader, IsTerminal, Write};
use std::os::unix::process::CommandExt;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Arc, Mutex};
use std::time::Duration;

const DEFAULT_RUNTIME: &str = "/usr/lib/zcode/glm/zcode.cjs";

// ---------- ANSI helpers ----------
const DIM: &str = "\x1b[2m";
const BOLD: &str = "\x1b[1m";
const RED: &str = "\x1b[31m";
const GREEN: &str = "\x1b[32m";
const YELLOW: &str = "\x1b[33m";
const CYAN: &str = "\x1b[36m";
const RESET: &str = "\x1b[0m";

// ---------- CLI options ----------
struct Opts {
    prompt: Option<String>,
    cwd: String,
    mode: Option<String>,
    resume: Option<String>,
    continue_latest: bool,
    runtime: String,
    node: String,
    show_thinking: bool,
    sessions: bool,
    quota: bool,
}

fn usage() -> ! {
    eprintln!(
        "zc — minimal CLI for the ZCode runtime

Usage: zc [options] [-- prompt words...]

  -p, --prompt <text>   run one prompt and exit (mode defaults to yolo)
  -c, --continue        resume the latest session for this directory
  --resume <sessionId>  resume a session by id (sess_...)
  --sessions            list sessions for this directory and exit
  --quota               show Z.AI plan quota (5-hour and weekly counters) and exit
  --cwd <path>          workspace directory (default: current dir)
  --mode <mode>         plan | build | edit | yolo | auto (default: build)
  --thinking            show model reasoning as it streams
  --runtime <path>      zcode.cjs path (env ZC_RUNTIME, default {DEFAULT_RUNTIME})
  --node <path>         node binary (env ZC_NODE, default: node)
  -h, --help            this help

In the REPL: /mode <m>  /model <provider/model>  /new  /stop  /sessions  /quit"
    );
    std::process::exit(2)
}

fn parse_args() -> Opts {
    let mut o = Opts {
        prompt: None,
        cwd: std::env::current_dir().unwrap().to_string_lossy().into_owned(),
        mode: None,
        resume: None,
        continue_latest: false,
        runtime: std::env::var("ZC_RUNTIME").unwrap_or_else(|_| DEFAULT_RUNTIME.into()),
        node: std::env::var("ZC_NODE").unwrap_or_else(|_| "node".into()),
        show_thinking: false,
        sessions: false,
        quota: false,
    };
    let mut args = std::env::args().skip(1);
    let mut positional: Vec<String> = Vec::new();
    while let Some(a) = args.next() {
        let mut next = |name: &str| args.next().unwrap_or_else(|| {
            eprintln!("{name} needs a value");
            usage()
        });
        match a.as_str() {
            "-p" | "--prompt" => o.prompt = Some(next("--prompt")),
            "-c" | "--continue" => o.continue_latest = true,
            "--resume" => o.resume = Some(next("--resume")),
            "--sessions" => o.sessions = true,
            "--quota" => o.quota = true,
            "--cwd" => o.cwd = next("--cwd"),
            "--mode" => o.mode = Some(next("--mode")),
            "--thinking" => o.show_thinking = true,
            "--runtime" => o.runtime = next("--runtime"),
            "--node" => o.node = next("--node"),
            "-h" | "--help" => usage(),
            "--" => positional.extend(args.by_ref()),
            s if s.starts_with('-') => {
                eprintln!("unknown option {s}");
                usage()
            }
            s => positional.push(s.to_string()),
        }
    }
    if !positional.is_empty() && o.prompt.is_none() {
        o.prompt = Some(positional.join(" "));
    }
    o.cwd = std::fs::canonicalize(&o.cwd)
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or(o.cwd);
    o
}

// ---------- JSON-RPC transport ----------
enum Msg {
    Event(Value),                                   // params of session/event
    ServerRequest { id: Value, method: String, params: Value },
    Closed,
}

struct Rpc {
    child: Mutex<Child>,
    stdin: Mutex<std::process::ChildStdin>,
    next_id: AtomicU64,
    pending: Mutex<HashMap<u64, Sender<Value>>>,
}

impl Rpc {
    fn spawn(node: &str, runtime: &str, cwd: &str) -> (Arc<Rpc>, Receiver<Msg>) {
        let mut child = Command::new(node)
            .arg(runtime)
            .arg("app-server")
            .current_dir(cwd)
            // Own process group so a terminal Ctrl-C reaches only zc, not the runtime.
            .process_group(0)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap_or_else(|e| {
                eprintln!("{RED}cannot start runtime{RESET}: {node} {runtime}: {e}");
                std::process::exit(1)
            });
        let stdin = child.stdin.take().unwrap();
        let stdout = child.stdout.take().unwrap();
        let rpc = Arc::new(Rpc {
            child: Mutex::new(child),
            stdin: Mutex::new(stdin),
            next_id: AtomicU64::new(1),
            pending: Mutex::new(HashMap::new()),
        });
        let (tx, rx) = mpsc::channel();
        let reader_rpc = rpc.clone();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { break };
                if line.trim().is_empty() {
                    continue;
                }
                let Ok(v) = serde_json::from_str::<Value>(&line) else { continue };
                let has_method = v.get("method").and_then(Value::as_str).is_some();
                let has_id = v.get("id").is_some();
                if has_method && has_id {
                    let _ = tx.send(Msg::ServerRequest {
                        id: v["id"].clone(),
                        method: v["method"].as_str().unwrap().to_string(),
                        params: v.get("params").cloned().unwrap_or(Value::Null),
                    });
                } else if has_method {
                    if v["method"] == "session/event" {
                        let _ = tx.send(Msg::Event(v.get("params").cloned().unwrap_or(Value::Null)));
                    }
                } else if let Some(s) = v.get("id").and_then(Value::as_u64).and_then(|id| reader_rpc.pending.lock().unwrap().remove(&id)) {
                    let _ = s.send(v);
                }
            }
            let _ = tx.send(Msg::Closed);
        });
        (rpc, rx)
    }

    fn write(&self, v: &Value) {
        let mut s = self.stdin.lock().unwrap();
        let _ = s.write_all(v.to_string().as_bytes());
        let _ = s.write_all(b"\n");
        let _ = s.flush();
    }

    /// Send a request; the response arrives later on the returned receiver.
    fn request(&self, method: &str, params: Value) -> Receiver<Value> {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let (tx, rx) = mpsc::channel();
        self.pending.lock().unwrap().insert(id, tx);
        self.write(&json!({"id": id, "method": method, "params": params}));
        rx
    }

    fn reply(&self, id: &Value, result: Value) {
        self.write(&json!({"id": id, "result": result}));
    }

    fn shutdown(&self) {
        let _ = self.child.lock().unwrap().kill();
    }
}

/// Wait for a response while still servicing events and server requests.
fn await_response(ui: &mut Ui, rx: &Receiver<Msg>, resp: Receiver<Value>) -> Result<Value, String> {
    loop {
        if let Ok(v) = resp.try_recv() {
            if let Some(e) = v.get("error") {
                return Err(e["message"].as_str().unwrap_or("request failed").to_string());
            }
            return Ok(v["result"].clone());
        }
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(msg) => ui.handle(msg),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(_) => return Err("runtime exited".into()),
        }
    }
}

// ---------- UI / event rendering ----------
struct Ui {
    rpc: Arc<Rpc>,
    editor: rustyline::DefaultEditor,
    history_path: Option<std::path::PathBuf>,
    stop: Arc<AtomicBool>,
    show_thinking: bool,
    interactive: bool,
    turn_done: bool,
    turn_failed: bool,
    in_thinking: bool,
    in_text: bool,
    tools: HashMap<String, String>, // toolCallId -> label
    answered: HashMap<String, Value>, // requestId -> reply (runtime re-announces every 1s)
}

impl Ui {
    /// Read one line with editing and history. None on Ctrl-C, Ctrl-D, or error;
    /// Ctrl-C also raises the stop flag so a running turn is cancelled.
    fn read(&mut self, prompt: &str) -> Option<String> {
        match self.editor.readline(prompt) {
            Ok(line) => {
                if !line.trim().is_empty() {
                    let _ = self.editor.add_history_entry(line.as_str());
                }
                Some(line)
            }
            Err(rustyline::error::ReadlineError::Interrupted) => {
                self.stop.store(true, Ordering::SeqCst);
                None
            }
            Err(_) => None,
        }
    }

    fn save_history(&mut self) {
        if let Some(p) = &self.history_path {
            if let Some(dir) = p.parent() {
                let _ = std::fs::create_dir_all(dir);
            }
            let _ = self.editor.save_history(p);
        }
    }

    fn end_line(&mut self) {
        if self.in_text || self.in_thinking {
            println!("{RESET}");
        }
        self.in_text = false;
        self.in_thinking = false;
    }

    fn handle(&mut self, msg: Msg) {
        match msg {
            Msg::Closed => {
                self.end_line();
                eprintln!("{RED}runtime exited{RESET}");
                std::process::exit(1);
            }
            Msg::ServerRequest { id, method, params } => self.server_request(id, &method, params),
            Msg::Event(p) => self.event(p),
        }
    }

    fn server_request(&mut self, id: Value, method: &str, params: Value) {
        let req_id = params["requestId"].as_str().map(String::from);
        if let Some(prev) = req_id.as_ref().and_then(|k| self.answered.get(k)) {
            let prev = prev.clone();
            self.rpc.reply(&id, prev);
            return;
        }
        let result = match method {
            "session/requestRuntimePreferences" => json!({
                "nativeSearchEnhancementsEnabled": false,
                "memoryEnabled": false,
                "askUserQuestionAutoResolutionEnabled": false
            }),
            "interaction/requestPermission" => self.ask_permission(&params),
            "interaction/requestUserInput" => self.ask_user_input(&params),
            _ => json!({}),
        };
        if let Some(k) = req_id {
            self.answered.insert(k, result.clone());
        }
        self.rpc.reply(&id, result);
    }

    fn ask_permission(&mut self, p: &Value) -> Value {
        self.end_line();
        let tool = p["toolName"].as_str().unwrap_or("tool");
        let reason = p["reason"].as_str().unwrap_or("");
        println!("{YELLOW}{BOLD}permission{RESET} {BOLD}{tool}{RESET} {DIM}{reason}{RESET}");
        println!("  {}", summarize_input(&p["input"], 400));
        let options = p["options"].as_array().cloned().unwrap_or_default();
        if !self.interactive {
            // Non-interactive: never grant silently.
            println!("  {DIM}non-interactive: denied (use --mode yolo to auto-approve){RESET}");
            return json!({"decision": "deny", "reason": "non-interactive"});
        }
        for (i, o) in options.iter().enumerate() {
            let name = o["name"].as_str().unwrap_or("?");
            let desc = o["description"].as_str().unwrap_or("");
            println!("  {CYAN}{}{RESET}) {name} {DIM}{desc}{RESET}", i + 1);
        }
        loop {
            let Some(line) = self.read("choice [1]: ") else {
                return json!({"decision": "deny", "reason": "Cancelled by user"});
            };
            let t = line.trim();
            if t == "d" || t == "n" || t == "deny" {
                return json!({"decision": "deny", "reason": "Denied by user"});
            }
            let idx = if t.is_empty() { 0 } else { t.parse::<usize>().ok().map(|n| n.wrapping_sub(1)).unwrap_or(usize::MAX) };
            if let Some(o) = options.get(idx) {
                return o["response"].clone();
            }
            println!("  invalid choice");
        }
    }

    fn ask_user_input(&mut self, p: &Value) -> Value {
        self.end_line();
        if let Some(prompt) = p["prompt"].as_str() {
            println!("{YELLOW}{BOLD}question{RESET} {prompt}");
        }
        if !self.interactive {
            return json!({"action": "decline"});
        }
        let mut answers = serde_json::Map::new();
        let mut last_text = String::new();
        for q in p["questions"].as_array().cloned().unwrap_or_default() {
            let header = q["header"].as_str().unwrap_or("answer").to_string();
            println!("{BOLD}{header}{RESET}: {}", q["question"].as_str().unwrap_or(""));
            let opts = q["options"].as_array().cloned().unwrap_or_default();
            for (i, o) in opts.iter().enumerate() {
                println!("  {CYAN}{}{RESET}) {} {DIM}{}{RESET}", i + 1, o["label"].as_str().unwrap_or("?"), o["description"].as_str().unwrap_or(""));
            }
            let Some(line) = self.read("> ") else {
                return json!({"action": "decline"});
            };
            let t = line.trim().to_string();
            let picked: Vec<String> = t
                .split(',')
                .filter_map(|s| s.trim().parse::<usize>().ok())
                .filter_map(|n| opts.get(n.wrapping_sub(1)))
                .map(|o| o["value"].as_str().or(o["label"].as_str()).unwrap_or("").to_string())
                .collect();
            let vals = if picked.is_empty() { vec![t.clone()] } else { picked };
            last_text = vals.join(", ");
            answers.insert(header, json!(vals));
        }
        json!({"action": "accept", "content": {"answers": answers, "answer": last_text}})
    }

    fn event(&mut self, p: Value) {
        let ty = p["type"].as_str().unwrap_or("");
        let pl = &p["payload"];
        match ty {
            "model.streaming" => match pl["kind"].as_str().unwrap_or("") {
                "text_delta" => {
                    if self.in_thinking {
                        self.end_line();
                    }
                    self.in_text = true;
                    print!("{}", pl["delta"].as_str().unwrap_or(""));
                    let _ = std::io::stdout().flush();
                }
                "reasoning_delta" if self.show_thinking => {
                    if !self.in_thinking {
                        self.end_line();
                        print!("{DIM}");
                    }
                    self.in_thinking = true;
                    print!("{}", pl["delta"].as_str().unwrap_or(""));
                    let _ = std::io::stdout().flush();
                }
                "tool_call" => {
                    self.end_line();
                    let name = pl["toolName"].as_str().unwrap_or("tool").to_string();
                    let label = format!("{name} {}", summarize_input(&pl["input"], 160));
                    println!("{CYAN}▸{RESET} {BOLD}{name}{RESET} {}", summarize_input(&pl["input"], 160));
                    if let Some(id) = pl["toolCallId"].as_str() {
                        self.tools.insert(id.to_string(), label);
                    }
                }
                _ => {}
            },
            "tool.updated" if pl["kind"] == "result" => {
                self.end_line();
                let r = &pl["result"];
                let ok = r["success"].as_bool().unwrap_or(false);
                let content = r["content"].as_str().unwrap_or("");
                let ms = pl["duration"].as_u64().unwrap_or(0);
                let first = content.lines().next().unwrap_or("");
                let short: String = first.chars().take(120).collect();
                if ok {
                    println!("  {GREEN}✓{RESET} {DIM}{ms}ms {short}{RESET}");
                } else {
                    println!("  {RED}✗{RESET} {short}");
                }
            }
            "permission.resolved" => {}
            "turn.completed" => {
                self.end_line();
                let u = &pl["usage"];
                println!(
                    "{DIM}── done in {:.1}s · {} in / {} out tokens · {} tool calls{RESET}",
                    pl["duration"].as_f64().unwrap_or(0.0) / 1000.0,
                    u["inputTokens"].as_u64().unwrap_or(0),
                    u["outputTokens"].as_u64().unwrap_or(0),
                    pl["toolCallCount"].as_u64().unwrap_or(0)
                );
                self.turn_done = true;
            }
            "turn.failed" => {
                self.end_line();
                let e = &pl["error"];
                println!("{RED}turn failed:{RESET} {}", e["message"].as_str().or(pl["message"].as_str()).unwrap_or("unknown error"));
                self.turn_done = true;
                self.turn_failed = true;
            }
            _ => {}
        }
    }
}

/// One-line description of a tool input: prefer command / file_path / pattern, else compact JSON.
fn summarize_input(input: &Value, max: usize) -> String {
    let s = if let Some(o) = input.as_object() {
        let pick = ["command", "file_path", "path", "pattern", "query", "url", "prompt", "description"];
        let mut parts = Vec::new();
        for k in pick {
            if let Some(v) = o.get(k).and_then(Value::as_str) {
                parts.push(v.replace('\n', " "));
            }
        }
        if parts.is_empty() { input.to_string() } else { parts.join("  ") }
    } else {
        input.to_string()
    };
    let mut out: String = s.chars().take(max).collect();
    if s.chars().count() > max {
        out.push('…');
    }
    format!("{DIM}{out}{RESET}")
}

// ---------- session helpers ----------

/// Build the `runtimeModel` object a resume needs, from ~/.zcode/cli/config.json.
/// Without it the runtime cannot verify the persisted model and refuses to run.
fn runtime_model_from_config() -> Option<Value> {
    let home = std::env::var("HOME").ok()?;
    let cfg: Value = serde_json::from_str(&std::fs::read_to_string(format!("{home}/.zcode/cli/config.json")).ok()?).ok()?;
    let main = cfg["model"]["main"].as_str()?;
    let (provider_id, model_id) = main.split_once('/')?;
    let prov = cfg["provider"].get(provider_id)?;
    let opts = &prov["options"];
    let models: Vec<Value> = prov["models"]
        .as_object()?
        .iter()
        .map(|(id, m)| json!({"modelId": id, "label": m["name"].as_str().unwrap_or(id)}))
        .collect();
    let mut provider = json!({
        "providerId": provider_id,
        "kind": prov["kind"].as_str().unwrap_or("anthropic"),
        "label": prov["name"].as_str().unwrap_or(provider_id),
        "apiKeyRequired": opts["apiKeyRequired"].as_bool().unwrap_or(true),
        "models": models,
    });
    if let Some(u) = opts["baseURL"].as_str() {
        provider["baseURL"] = json!(u);
    }
    if let Some(k) = opts["apiKey"].as_str() {
        provider["apiKey"] = json!({"source": "inline", "value": k});
    }
    let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).ok()?.as_millis() as u64;
    Some(json!({
        "revision": format!("zc-{now}"),
        "generatedAt": now,
        "model": {"providerId": provider_id, "modelId": model_id},
        "provider": provider,
    }))
}

/// Print the Z.AI plan quota counters using the CLI config's API key. Uses curl
/// so the binary stays dependency-free.
fn show_quota() -> Result<(), String> {
    let home = std::env::var("HOME").map_err(|e| e.to_string())?;
    let cfg: Value = serde_json::from_str(&std::fs::read_to_string(format!("{home}/.zcode/cli/config.json")).map_err(|e| e.to_string())?)
        .map_err(|e| e.to_string())?;
    let main = cfg["model"]["main"].as_str().unwrap_or("zai/");
    let provider_id = main.split('/').next().unwrap_or("zai");
    let opts = &cfg["provider"][provider_id]["options"];
    let key = opts["apiKey"].as_str().ok_or("no apiKey in config")?;
    let base = opts["baseURL"].as_str().unwrap_or("https://api.z.ai/api/anthropic");
    let host = base.split("/api/").next().unwrap_or("https://api.z.ai");
    let out = Command::new("curl")
        .args(["-s", "-m", "20", "-H", &format!("Authorization: Bearer {key}"), &format!("{host}/api/monitor/usage/quota/limit")])
        .output()
        .map_err(|e| format!("curl: {e}"))?;
    let v: Value = serde_json::from_slice(&out.stdout).map_err(|_| format!("bad response: {}", String::from_utf8_lossy(&out.stdout)))?;
    let limits = v["data"]["limits"].as_array().ok_or_else(|| v["msg"].as_str().unwrap_or("no limits in response").to_string())?;
    println!("{DIM}plan level: {}{RESET}", v["data"]["level"].as_str().unwrap_or("?"));
    for l in limits {
        let name = match l["unit"].as_u64() { Some(3) => "5-hour", Some(6) => "weekly", _ => "other" };
        let reset = l["nextResetTime"].as_u64().map(|ms| ms / 1000).unwrap_or(0);
        let reset_s = Command::new("date").args(["-d", &format!("@{reset}"), "+%a %H:%M"]).output().ok()
            .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string()).unwrap_or_default();
        println!(
            "{BOLD}{name:7}{RESET} used {} / {} ({}%)  {DIM}resets {reset_s}{RESET}",
            l["currentValue"].as_u64().unwrap_or(0),
            l["usage"].as_u64().unwrap_or(0),
            l["percentage"].as_u64().unwrap_or(0)
        );
    }
    Ok(())
}

fn workspace(cwd: &str) -> Value {
    json!({"workspacePath": cwd, "workspaceKey": cwd})
}

fn session_id_of(result: &Value) -> Option<String> {
    result["session"]["sessionId"].as_str().or(result["sessionId"].as_str()).map(String::from)
}

fn list_sessions(ui: &mut Ui, rx: &Receiver<Msg>, cwd: &str) -> Vec<Value> {
    let r = await_response(ui, rx, ui.rpc.request("session/list", json!({}))).unwrap_or(json!({}));
    let mut v: Vec<Value> = r["sessions"]
        .as_array()
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .filter(|s| s["workspace"]["workspacePath"].as_str() == Some(cwd))
        .collect();
    v.sort_by_key(|s| std::cmp::Reverse(s["updatedAt"].as_u64().unwrap_or(0)));
    v
}

fn print_sessions(list: &[Value]) {
    if list.is_empty() {
        println!("{DIM}no sessions for this directory{RESET}");
    }
    for s in list.iter().take(20) {
        println!(
            "{CYAN}{}{RESET}  {DIM}{}{RESET}  {}",
            s["sessionId"].as_str().unwrap_or("?"),
            s["mode"].as_str().unwrap_or(""),
            s["title"].as_str().unwrap_or("")
        );
    }
}

fn open_session(ui: &mut Ui, rx: &Receiver<Msg>, cwd: &str, resume: Option<&str>) -> Result<(String, Value), String> {
    let (method, params) = match resume {
        Some(id) => {
            let mut p = json!({"sessionId": id, "workspace": workspace(cwd)});
            match runtime_model_from_config() {
                Some(rm) => p["runtimeModel"] = rm,
                None => println!("{YELLOW}warning:{RESET} could not read ~/.zcode/cli/config.json; resume may fail with model unavailable"),
            }
            ("session/resume", p)
        }
        None => ("session/create", json!({"workspace": workspace(cwd)})),
    };
    let r = await_response(ui, rx, ui.rpc.request(method, params))?;
    let id = session_id_of(&r).ok_or("no sessionId in response")?;
    await_response(ui, rx, ui.rpc.request("session/subscribe", json!({"sessionId": id, "deliveryKind": "desktop-continuous"})))?;
    Ok((id, r["session"].clone()))
}

fn set_mode(ui: &mut Ui, rx: &Receiver<Msg>, sid: &str, mode: &str) {
    if let Err(e) = await_response(ui, rx, ui.rpc.request("session/setMode", json!({"sessionId": sid, "mode": mode}))) {
        println!("{RED}{e}{RESET}");
    } else {
        println!("{DIM}mode: {mode}{RESET}");
    }
}

fn run_turn(ui: &mut Ui, rx: &Receiver<Msg>, sid: &str, content: &str, stop_flag: &AtomicBool) -> bool {
    ui.turn_done = false;
    ui.turn_failed = false;
    stop_flag.store(false, Ordering::SeqCst);
    let resp = ui.rpc.request("session/send", json!({"sessionId": sid, "content": content}));
    if let Err(e) = await_response(ui, rx, resp) {
        println!("{RED}{e}{RESET}");
        return false;
    }
    let mut stop_sent = false;
    while !ui.turn_done {
        if stop_flag.load(Ordering::SeqCst) && !stop_sent {
            stop_sent = true;
            ui.end_line();
            println!("{YELLOW}stopping…{RESET}");
            let _ = ui.rpc.request("session/stop", json!({"sessionId": sid}));
        }
        match rx.recv_timeout(Duration::from_millis(50)) {
            Ok(m) => ui.handle(m),
            Err(mpsc::RecvTimeoutError::Timeout) => {}
            Err(_) => return false,
        }
    }
    !ui.turn_failed
}

fn main() {
    let o = parse_args();
    if o.quota {
        if let Err(e) = show_quota() {
            eprintln!("{RED}{e}{RESET}");
            std::process::exit(1);
        }
        return;
    }
    let interactive = std::io::stdin().is_terminal() && o.prompt.is_none();
    let (rpc, rx) = Rpc::spawn(&o.node, &o.runtime, &o.cwd);
    let stop_flag = Arc::new(AtomicBool::new(false));
    let history_path = std::env::var("HOME").ok().map(|h| std::path::PathBuf::from(h).join(".local/share/zc/history"));
    let mut editor = rustyline::DefaultEditor::new().unwrap_or_else(|e| {
        eprintln!("{RED}cannot initialise line editor{RESET}: {e}");
        std::process::exit(1)
    });
    if let Some(p) = &history_path {
        let _ = editor.load_history(p);
    }
    let mut ui = Ui {
        rpc: rpc.clone(),
        editor,
        history_path,
        stop: stop_flag.clone(),
        show_thinking: o.show_thinking,
        interactive,
        turn_done: false,
        turn_failed: false,
        in_thinking: false,
        in_text: false,
        tools: HashMap::new(),
        answered: HashMap::new(),
    };

    // Ctrl-C: first press stops the running turn; at the prompt it quits.
    let in_turn = Arc::new(AtomicBool::new(false));
    {
        let (stop_flag, in_turn, rpc) = (stop_flag.clone(), in_turn.clone(), rpc.clone());
        let _ = ctrlc::set_handler(move || {
            if in_turn.load(Ordering::SeqCst) {
                stop_flag.store(true, Ordering::SeqCst);
            } else {
                rpc.shutdown();
                std::process::exit(130);
            }
        });
    }

    if o.sessions {
        let l = list_sessions(&mut ui, &rx, &o.cwd);
        print_sessions(&l);
        rpc.shutdown();
        return;
    }

    let resume_id = if o.continue_latest {
        let l = list_sessions(&mut ui, &rx, &o.cwd);
        l.first().and_then(|s| s["sessionId"].as_str().map(String::from))
    } else {
        o.resume.clone()
    };

    let (mut sid, session) = match open_session(&mut ui, &rx, &o.cwd, resume_id.as_deref()) {
        Ok(v) => v,
        Err(e) => {
            eprintln!("{RED}{e}{RESET}");
            rpc.shutdown();
            std::process::exit(1);
        }
    };

    let mode = o.mode.clone().or_else(|| if o.prompt.is_some() { Some("yolo".into()) } else { None });
    if let Some(m) = &mode {
        set_mode(&mut ui, &rx, &sid, m);
    }

    if let Some(p) = &o.prompt {
        in_turn.store(true, Ordering::SeqCst);
        let ok = run_turn(&mut ui, &rx, &sid, p, &stop_flag);
        in_turn.store(false, Ordering::SeqCst);
        let _ = await_response(&mut ui, &rx, rpc.request("session/close", json!({"sessionId": sid})));
        rpc.shutdown();
        std::process::exit(if ok { 0 } else { 1 });
    }

    println!(
        "{DIM}zc · {} · {} · mode {} · {}{RESET}",
        sid,
        session["model"]["providerId"].as_str().map(|p| format!("{p}/{}", session["model"]["modelId"].as_str().unwrap_or(""))).unwrap_or_default(),
        mode.as_deref().or(session["mode"].as_str()).unwrap_or("build"),
        o.cwd
    );
    println!("{DIM}type a prompt, /help for commands, Ctrl-C to stop a turn{RESET}");

    loop {
        // Drain anything that arrived while idle.
        while let Ok(m) = rx.try_recv() {
            ui.handle(m);
        }
        println!();
        let Some(line) = ui.read("> ") else { break };
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(cmd) = line.strip_prefix('/') {
            let mut it = cmd.splitn(2, ' ');
            let name = it.next().unwrap_or("");
            let arg = it.next().map(str::trim).unwrap_or("");
            match name {
                "quit" | "exit" | "q" => break,
                "help" => println!("/mode <plan|build|edit|yolo|auto>  /model <provider/model>  /new  /sessions  /resume <id>  /thinking  /quit"),
                "mode" => set_mode(&mut ui, &rx, &sid, arg),
                "thinking" => {
                    ui.show_thinking = !ui.show_thinking;
                    println!("{DIM}thinking: {}{RESET}", if ui.show_thinking { "on" } else { "off" });
                }
                "model" => {
                    let (prov, model) = arg.split_once('/').unwrap_or(("zai", arg));
                    match await_response(&mut ui, &rx, rpc.request("session/setModel", json!({"sessionId": sid, "model": {"providerId": prov, "modelId": model}}))) {
                        Ok(_) => println!("{DIM}model: {prov}/{model}{RESET}"),
                        Err(e) => println!("{RED}{e}{RESET}"),
                    }
                }
                "sessions" => {
                    let l = list_sessions(&mut ui, &rx, &o.cwd);
                    print_sessions(&l);
                }
                "new" | "resume" => {
                    let target = if name == "resume" { Some(arg) } else { None };
                    let _ = await_response(&mut ui, &rx, rpc.request("session/close", json!({"sessionId": sid})));
                    match open_session(&mut ui, &rx, &o.cwd, target) {
                        Ok((id, _)) => {
                            sid = id;
                            println!("{DIM}session: {sid}{RESET}");
                        }
                        Err(e) => println!("{RED}{e}{RESET}"),
                    }
                }
                _ => println!("{RED}unknown command{RESET} /{name}"),
            }
            continue;
        }
        in_turn.store(true, Ordering::SeqCst);
        run_turn(&mut ui, &rx, &sid, line, &stop_flag);
        in_turn.store(false, Ordering::SeqCst);
    }
    ui.save_history();
    let _ = await_response(&mut ui, &rx, rpc.request("session/close", json!({"sessionId": sid})));
    rpc.shutdown();
}
