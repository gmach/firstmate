import { spawn } from "node:child_process";
import { realpathSync } from "node:fs";
import { resolve } from "node:path";
import { encodeFirstmateOperationalInput } from "./lib/fm-operational-input.js";

// Turn-end guard for OpenCode: when a session goes idle without a healthy
// watcher cycle, force one follow-up turn so the turn does not end blind
// (bin/fm-turnend-guard.sh owns the verdict and the recovery instruction).
// OpenCode 2 loads a local plugin only from a default export carrying an `id`
// plus `setup(ctx)`; `setup` subscribes to the server event stream with
// ctx.event.subscribe() and returns the cleanup that aborts it. V2 event
// payloads carry their fields under `data`, so an idle session's id is
// event.data.sessionID, and the follow-up rides ctx.session.prompt (verified
// 2026-10-13 against OpenCode 2.0.28).

const COORDINATOR_KEY = "__firstmateOpenCodeWatchArm";

function runProcess(command, args, input = "") {
  return new Promise((resolve) => {
    const child = spawn(command, args, {
      stdio: ["pipe", "pipe", "pipe"],
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("error", () => resolve({ code: 0, stdout: "", stderr: "" }));
    child.on("close", (code) => resolve({ code: code ?? 0, stdout, stderr }));
    child.stdin.end(input);
  });
}

async function resolveRoot(anchor) {
  if (!anchor) return "";
  const result = await runProcess("git", ["-C", anchor, "rev-parse", "--show-toplevel"]);
  const root = result.stdout.trim();
  if (result.code === 0 && root) return root;
  return resolvePath(anchor);
}

function resolvePath(anchor) {
  try {
    return realpathSync(anchor);
  } catch {
    return resolve(anchor);
  }
}

function runGuard(root) {
  if (!root) return Promise.resolve({ code: 0, stderr: "" });
  return runProcess(`${root}/bin/fm-turnend-guard.sh`, [], '{"stop_hook_active":false}');
}

async function letWatchArmRun(sessionID) {
  const coordinator = globalThis[COORDINATOR_KEY];
  if (!coordinator?.ensureArmed) return false;
  const status = await coordinator.ensureArmed(sessionID);
  return status === "armed" || status === "wake" || status === "failed";
}

export default {
  id: "fm-primary-turnend-guard",
  async setup(ctx) {
    const root = await resolveRoot(ctx.location.directory);
    // The server event stream carries every session's idles, so the
    // one-follow-up marker is tracked per session instead of once per plugin.
    const skipNextIdle = new Set();
    const controller = new AbortController();

    void (async () => {
      for await (const event of ctx.event.subscribe({ signal: controller.signal })) {
        if (event.type !== "session.idle") continue;
        const sessionID = event.data?.sessionID;
        if (!sessionID) continue;

        if (skipNextIdle.has(sessionID)) {
          skipNextIdle.delete(sessionID);
          continue;
        }

        if (await letWatchArmRun(sessionID)) continue;

        const result = await runGuard(root);
        if (result.code !== 2) continue;

        try {
          const text = await encodeFirstmateOperationalInput(
            root,
            "turn-end-guard",
            "TURN WOULD END BLIND - supervision is off. " +
              "The watcher cycle is missing, failed, or unhealthy. Follow the harness recovery instruction below before ending the turn.\n\n" +
              result.stderr,
          );
          await ctx.session.prompt({ sessionID, text });
          skipNextIdle.add(sessionID);
        } catch {
          skipNextIdle.delete(sessionID);
        }
      }
    })();

    return () => controller.abort();
  },
};
