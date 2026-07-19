import { cp, mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

function replaceRequired(text, needle, replacement, label) {
  if (!text.includes(needle)) throw new Error(`rendered config is missing ${label}`);
  return text.replaceAll(needle, replacement);
}

export async function materializeCloudConfigs({ root, output, names, kvNamespaceId, bucketName }) {
  if (!/^[0-9a-f]{32}$/.test(kvNamespaceId)) throw new Error("KV namespace id must be 32 hex bytes");
  const flowRoot = join(output, "flow");
  const extractionRoot = join(output, "extraction");
  const providerRoot = join(output, "provider");
  await mkdir(flowRoot, { recursive: true });
  await mkdir(extractionRoot, { recursive: true });
  await mkdir(providerRoot, { recursive: true });
  await cp(join(root, "deploy/flow-worker"), join(flowRoot, "flow-worker"), { recursive: true });
  await cp(join(root, "deploy/extraction-worker/dist"), join(extractionRoot, "dist"), { recursive: true });
  await cp(join(root, "mock-provider/src"), join(providerRoot, "src"), { recursive: true });

  let flow = await readFile(join(root, "deploy/wrangler.toml"), "utf8");
  flow = replaceRequired(flow, "s21-w4-flow", names.flow, "flow Worker name");
  flow = replaceRequired(flow, "s21-w4-pdf-extract", names.extraction, "extraction service name");
  flow = replaceRequired(
    flow,
    'id = "REPLACE_WITH_KV_NAMESPACE_ID"',
    `id = "${kvNamespaceId}"`,
    "KV placeholder",
  );
  flow = flow.replace(/bucket_name = "[^"]+"/, `bucket_name = "${bucketName}"`);
  if (!/^workers_dev = true$/m.test(flow)) {
    throw new Error("rendered config is missing flow workers_dev policy");
  }
  flow = flow.replace(
    /^workers_dev = true$/m,
    "workers_dev = false\npreview_urls = false",
  );
  flow += `\n[[services]]\nbinding = "LATTICE_S21_PROVIDER"\nservice = "${names.provider}"\n`;
  flow = flow.replace("[vars]", '[vars]\nLATTICE_S21_HTTP_MODE = "service_binding"');
  const publicFlow = flow.replace(/^workers_dev = false$/m, "workers_dev = true");

  let extraction = await readFile(join(root, "deploy/extraction-worker/wrangler.toml"), "utf8");
  extraction = replaceRequired(extraction, "s21-w4-pdf-extract", names.extraction, "extraction Worker name");
  let provider = await readFile(join(root, "mock-provider/wrangler.toml"), "utf8");
  provider = replaceRequired(provider, "s21-w4-mock-provider", names.provider, "provider Worker name");
  const privateProvider = provider;
  if (!/^workers_dev = false$/m.test(provider)) {
    throw new Error("rendered config is missing provider workers_dev policy");
  }
  const publicProvider = provider.replace(/^workers_dev = false$/m, "workers_dev = true");

  for (const [label, text] of Object.entries({ flow, publicFlow, extraction, provider, privateProvider })) {
    if (/=\s*"(?:REPLACE_WITH_|TODO)/.test(text)) {
      throw new Error(`${label} config retains a deployment placeholder`);
    }
  }
  const paths = {
    flow: join(flowRoot, "wrangler.toml"),
    extraction: join(extractionRoot, "wrangler.toml"),
    provider: join(providerRoot, "wrangler.toml"),
  };
  await writeFile(paths.flow, flow);
  await writeFile(paths.extraction, extraction);
  await writeFile(paths.provider, privateProvider);
  return { paths, privateFlow: flow, publicFlow, extraction, privateProvider, publicProvider };
}
