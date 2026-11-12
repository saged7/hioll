import {
  actor,
  audit,
  body,
  HttpError,
  repository,
  safePath,
  serve,
} from "../_shared/core.ts";
import { base, githubResponse, snapshot } from "../_shared/github.ts";
import { generatorStream, zipStream } from "../_shared/zip.ts";
export const handler = serve(async (req) => {
  const p = await actor(req);
  const b = await body(req);
  const r = await repository(p, b.repository_id);
  if (!Array.isArray(b.paths) || b.paths.length < 1 || b.paths.length > 1000) {
    throw new HttpError(400, "Select 1–1000 paths");
  }
  const paths = b.paths.map(safePath) as string[];
  const snap = await snapshot(r);
  const selected = snap.items.filter((e) =>
    e.type === "blob" &&
    paths.some((path) =>
      path === "" || e.path === path || e.path.startsWith(path + "/")
    )
  );
  if (!selected.length) throw new HttpError(404, "No files found");
  if (
    selected.length > 1000 || selected.reduce((n, e) =>
        n + (e.size ?? 0), 0) > 200 * 1024 * 1024 ||
    selected.some((e) => (e.size ?? 0) > 90 * 1024 * 1024)
  ) throw new HttpError(413, "Download exceeds limits");
  if (
    selected.some((e) => e.mode === "120000" || safePath(e.path) !== e.path)
  ) throw new HttpError(400, "Symlink or unsafe path unsupported");
  const raw = b.format === "file";
  if (
    raw &&
    (selected.length !== 1 || paths.length !== 1 ||
      selected[0].path !== paths[0])
  ) throw new HttpError(400, "Single file required");
  const target = r.id + ":" + paths.join(",");
  await audit(p.id, "download", target, "started");
  const open = async (e: { path: string; sha: string }) => {
    // Revalidate both profile and grant immediately before each blob stream.
    const current = await actor(req);
    await repository(current, r.id);
    const result = await githubResponse(
      base(r) + "/git/blobs/" + e.sha,
      "GET",
      undefined,
      "application/vnd.github.raw+json",
    );
    if (!result.body) throw new Error("No body");
    return result.body;
  };
  async function* single() {
    const reader = (await open(selected[0])).getReader();
    let count = 0;
    try {
      while (true) {
        const n = await reader.read();
        if (n.done) break;
        count += n.value.length;
        if (count > 90 * 1024 * 1024) throw new Error("File too large");
        yield n.value;
      }
    } finally {
      await reader.cancel().catch(() => {});
      reader.releaseLock();
    }
  }
  const stream = generatorStream(
    raw ? single() : zipStream(selected, open),
    (ok) => audit(p.id, "download_stream", target, ok ? "success" : "failed"),
  );
  return new Response(stream, {
    headers: {
      "Content-Type": raw ? "application/octet-stream" : "application/zip",
      "Content-Disposition": `attachment; filename*=UTF-8''${
        encodeURIComponent(
          raw ? selected[0].path.split("/").pop()! : "vault.zip",
        )
      }`,
      "Cache-Control": "no-store",
      "X-Vault-Sha": raw ? selected[0].sha : snap.head,
    },
  });
});
