# zc

A minimal terminal client for the ZCode agent runtime. It drives the same
runtime the ZCode desktop app uses (`zcode.cjs app-server`) over JSON-RPC on
stdio, so you keep ZCode's tools, sessions, config and login, but without
Electron.

## Requirements

- ZCode installed (`/usr/lib/zcode/glm/zcode.cjs`); override with `--runtime`
  or `ZC_RUNTIME`.
- `node` on PATH (override with `--node` or `ZC_NODE`).
- Model access configured in `~/.zcode/cli/config.json` (the runtime reads it).

## Install

Arch Linux, from the release tarball:

```bash
makepkg -si
```

Arch Linux, latest commit (uses `PKGBUILD-git`):

```bash
makepkg -si -p PKGBUILD-git
```

Anywhere with a Rust toolchain:

```bash
cargo install --path .
```

That puts `zc` in `~/.cargo/bin`.

## License

MIT.

## Usage

```
zc                          interactive session in the current directory
zc -c                       continue the latest session for this directory
zc --resume sess_...        resume a specific session
zc --sessions               list sessions for this directory
zc -p "fix the failing test" one-shot prompt (mode defaults to yolo), exits when done
zc --mode plan              plan | build | edit | yolo | auto
zc --thinking               stream the model's reasoning (dimmed)
```

Inside the REPL: `/mode <m>`, `/model <provider/model>`, `/new`, `/sessions`,
`/resume <id>`, `/thinking`, `/quit`. Ctrl-C stops the running turn; at the
prompt it exits.

Permission requests from tools are shown with the runtime's own options
(allow once, always allow in project, deny). In non-interactive use they are
denied unless the mode is `yolo`.

## How it works

`src/main.rs` spawns the runtime, reads newline-delimited JSON on its stdout
in a thread, and routes three kinds of messages: responses to our requests,
`session/event` notifications (streamed text, tool calls, results, turn end),
and server-to-client requests (`session/requestRuntimePreferences`,
`interaction/requestPermission`, `interaction/requestUserInput`) which must be
answered or the runtime times out. The runtime re-announces unanswered
requests every second under the same `requestId`, so answers are cached.

Resuming a session needs a `runtimeModel` object describing the provider and
model, otherwise the runtime reports the persisted model as unavailable. zc
builds it from `~/.zcode/cli/config.json` (the `model.main` entry and its
provider block).

## Memory

Measured on this machine: zc itself ~2 MB resident; the Node runtime it drives
~450 MB. The runtime is the same one the desktop app uses, so the saving is the
Electron GUI (renderer, GPU and main processes), not the agent.
