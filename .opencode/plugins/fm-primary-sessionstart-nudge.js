import { spawn } from "node:child_process";
import { realpathSync } from "node:fs";
import { resolve } from "node:path";

// Session-start nudge for OpenCode: on a new session in the primary checkout,
// run bin/fm-sessionstart-nudge.sh and inject its one-line instruction so the
// session runs the session-start digest before doing anything else.
// OpenCode 2 loads a local plugin only from a default export carrying an `id`
// plus `setup(ctx)`; `setup` subscribes to the server event stream with
// ctx.event.subscribe() and returns the cleanup that aborts it. V2 event
// payloads carry their fields under `data`, so a new session's id is
// event.data.sessionID (verified 2026-10-13 against OpenCode 2.0.28). The
// owner script is itself inert outside the real primary checkout and when the
// home lock is already held, so a crewmate/scout worktree or a nested session
// is never nudged.

function runProcess(command, args) {
  return new Promise((resolveResult) => {
    const child = spawn(command, args, { stdio: ["ignore", "pipe", "ignore"] });
    let stdout = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.on("error", () => resolveResult({ code: 0, stdout: "" }));
    child.on("close", (code) => resolveResult({ code: code ?? 0, stdout }));
  });
}

function resolvePath(anchor) {
  try {
    return realpathSync(anchor);
  } catch {
    return resolve(anchor);
  }
}

async function resolveRoot(anchor) {
  if (!anchor) return "";
  const result = await runProcess("git", ["-C", anchor, "rev-parse", "--show-toplevel"]);
  const root = result.stdout.trim();
  if (result.code === 0 && root) return root;
  return resolvePath(anchor);
}

export default {
  id: "fm-primary-sessionstart-nudge",
  async setup(ctx) {
    const root = await resolveRoot(ctx.location.directory);
    const handledSessions = new Set();
    const controller = new AbortController();

    void (async () => {
      for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
        if (event.type !== "session.created") continue;
        const sessionID = event.data?.sessionID;
        if (!sessionID || handledSessions.has(sessionID) || !root) continue;
        handledSessions.add(sessionID);

        const result = await runProcess(`${root}/bin/fm-sessionstart-nudge.sh`, []);
        const nudge = result.code === 0 ? result.stdout.trim() : "";
        if (!nudge) continue;

        try {
          await ctx.session.prompt({ sessionID, text: nudge });
        } catch {
        }
      }
    })();

    return () => controller.abort();
  },
};
