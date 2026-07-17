import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { spawnSync } from "node:child_process";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { parse } from "smol-toml";
import { materializeCloudConfigs } from "./cloud-config.mjs";

const root = resolve(new URL("..", import.meta.url).pathname);
const output = await mkdtemp(join(tmpdir(), "lattice-w4-cloud-config-"));
try {
  const generated = await materializeCloudConfigs({
    root,
    output,
    names: {
      flow: "lattice-w4-offline-flow",
      extraction: "lattice-w4-offline-extract",
      provider: "lattice-w4-offline-provider",
    },
    kvNamespaceId: "0123456789abcdef0123456789abcdef",
    bucketName: "lattice-w4-offline-workspace",
  });
  for (const [name, text] of Object.entries({
    privateFlow: generated.privateFlow,
    publicFlow: generated.publicFlow,
    extraction: generated.extraction,
    privateProvider: generated.privateProvider,
    publicProvider: generated.publicProvider,
  })) {
    const value = parse(text);
    if (value.name === undefined || /=\s*"(?:REPLACE_WITH_|TODO)/.test(text)) {
      throw new Error(`${name} config is incomplete`);
    }
  }
  const dryRun = (config, outdir) => {
    const result = spawnSync(
      "npx",
      ["wrangler", "deploy", "--dry-run", "--config", config, "--outdir", outdir],
      { cwd: root, stdio: "inherit" },
    );
    if (result.status !== 0) throw new Error(`Wrangler rejected ${config}`);
  };
  dryRun(generated.paths.extraction, join(output, "dry-extraction"));
  dryRun(generated.paths.provider, join(output, "dry-private-provider"));
  dryRun(generated.paths.flow, join(output, "dry-private-flow"));
  await writeFile(generated.paths.provider, generated.publicProvider);
  await writeFile(generated.paths.flow, generated.publicFlow);
  dryRun(generated.paths.provider, join(output, "dry-public-provider"));
  dryRun(generated.paths.flow, join(output, "dry-public-flow"));
  console.log("verified private/public disposable Cloudflare configs");
} finally {
  await rm(output, { recursive: true, force: true });
}
