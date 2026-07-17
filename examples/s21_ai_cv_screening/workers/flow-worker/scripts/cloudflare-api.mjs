export function createCloudflareApi(token, fetchImpl = fetch) {
  const envelope = async (path, init = {}) => {
    const response = await fetchImpl(`https://api.cloudflare.com/client/v4${path}`, {
      ...init,
      headers: {
        authorization: `Bearer ${token}`,
        "content-type": "application/json",
        ...(init.headers ?? {}),
      },
    });
    const body = await response.json().catch(() => ({}));
    if (!response.ok || body.success === false) {
      throw new Error(`Cloudflare API ${init.method ?? "GET"} ${path} failed (${response.status})`);
    }
    return body;
  };

  const request = async (path, init = {}) => (await envelope(path, init)).result;

  const listPaged = async (path) => {
    const output = [];
    const seen = new Set();
    for (let page = 1; ; page += 1) {
      const join = path.includes("?") ? "&" : "?";
      const body = await envelope(`${path}${join}page=${page}&per_page=100`);
      if (!Array.isArray(body.result)) throw new Error(`expected paged array from ${path}`);
      for (const item of body.result) {
        const identity = String(item.id ?? item.name ?? item.title ?? JSON.stringify(item));
        if (seen.has(identity)) throw new Error(`pagination repeated an item from ${path}`);
        seen.add(identity);
        output.push(item);
      }
      const info = body.result_info ?? {};
      if (Number.isInteger(info.total_pages)) {
        if (page >= info.total_pages) break;
      } else if (body.result.length < 100) {
        break;
      } else {
        throw new Error(`pagination metadata is absent for a full page from ${path}`);
      }
    }
    return output;
  };

  const listBuckets = async (accountId) => {
    const output = [];
    const seen = new Set();
    let cursor;
    for (;;) {
      const query = new URLSearchParams({ per_page: "1000" });
      if (cursor) query.set("cursor", cursor);
      const body = await envelope(`/accounts/${accountId}/r2/buckets?${query}`);
      const buckets = body.result?.buckets;
      if (!Array.isArray(buckets)) throw new Error("expected R2 bucket list");
      for (const bucket of buckets) {
        if (seen.has(bucket.name)) throw new Error("R2 pagination repeated a bucket");
        seen.add(bucket.name);
        output.push(bucket);
      }
      const next = body.result_info?.cursor ?? body.result?.result_info?.cursor;
      if (typeof next === "string" && next.length > 0) {
        cursor = next;
      } else if (buckets.length < 1000) {
        break;
      } else {
        throw new Error("R2 pagination cursor is absent for a full page");
      }
    }
    return output;
  };

  return { envelope, request, listPaged, listBuckets };
}
