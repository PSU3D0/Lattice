import { describe, it, expect, afterAll, beforeEach } from "vitest";
import { Miniflare } from "miniflare";

// Initialize Miniflare with our test worker and KV namespace
const mf = new Miniflare({
  workers: [
    {
      scriptPath: "./build/index.js",
      compatibilityDate: "2024-09-23",
      modules: true,
      modulesRules: [
        { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
      ],
      kvNamespaces: ["TEST_KV"],
    },
  ],
});

const mfUrl = await mf.ready;

// Helper to get KV namespace for direct manipulation in tests
const getKv = () => mf.getKVNamespace("TEST_KV");

// Helper functions for KV operations via HTTP
async function kvPut(
  key: string,
  value: string,
  options?: { ttl_seconds?: number; metadata?: unknown }
) {
  const response = await mf.dispatchFetch(`${mfUrl}kv/put`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ key, value, options }),
  });
  return response.json();
}

async function kvGet(key: string, options?: { cache_ttl?: number }) {
  const params = new URLSearchParams({ key });
  if (options?.cache_ttl) params.set("cache_ttl", options.cache_ttl.toString());
  const response = await mf.dispatchFetch(`${mfUrl}kv/get?${params.toString()}`);
  return response.json();
}

async function kvGetWithMetadata(key: string, options?: { cache_ttl?: number }) {
  const params = new URLSearchParams({ key });
  if (options?.cache_ttl) params.set("cache_ttl", options.cache_ttl.toString());
  const response = await mf.dispatchFetch(`${mfUrl}kv/get-with-metadata?${params.toString()}`);
  return response.json();
}

async function kvDelete(key: string) {
  const response = await mf.dispatchFetch(`${mfUrl}kv/delete?key=${encodeURIComponent(key)}`, {
    method: "DELETE",
  });
  return response.json();
}

async function kvList(options?: {
  prefix?: string;
  limit?: number;
  cursor?: string;
  include_metadata?: boolean;
  include_expiration?: boolean;
}) {
  const params = new URLSearchParams();
  if (options?.prefix) params.set("prefix", options.prefix);
  if (options?.limit) params.set("limit", options.limit.toString());
  if (options?.cursor) params.set("cursor", options.cursor);
  if (options?.include_metadata !== undefined) {
    params.set("include_metadata", options.include_metadata ? "true" : "false");
  }
  if (options?.include_expiration !== undefined) {
    params.set("include_expiration", options.include_expiration ? "true" : "false");
  }

  const url = `${mfUrl}kv/list${params.toString() ? "?" + params.toString() : ""}`;
  const response = await mf.dispatchFetch(url);
  return response.json();
}

afterAll(async () => {
  await mf.dispose();
});

// Clean up KV before each test to ensure isolation
beforeEach(async () => {
  const kv = await getKv();
  const { keys } = await kv.list();
  for (const key of keys) {
    await kv.delete(key.name);
  }
});

describe("cap-kv-workers E2E", () => {
  describe("health check", () => {
    it("should return 200 OK", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}health`);
      expect(response.status).toBe(200);
      const body = await response.json();
      expect(body).toEqual({ status: "ok" });
    });
  });

  describe("Basic CRUD", () => {
    it("should put then get a value", async () => {
      const putResult = await kvPut("test-key", "test-value");
      expect(putResult).toEqual({ success: true });

      const getResult = await kvGet("test-key");
      expect(getResult).toEqual({ found: true, value: "test-value" });
    });

    it("should get non-existent key returns found=false", async () => {
      const result = await kvGet("non-existent-key");
      expect(result).toEqual({ found: false, value: null });
    });

    it("should delete a value", async () => {
      // First put a value
      await kvPut("delete-me", "some-value");

      // Verify it exists
      const getResult1 = await kvGet("delete-me");
      expect(getResult1.found).toBe(true);

      // Delete it
      const deleteResult = await kvDelete("delete-me");
      expect(deleteResult).toEqual({ success: true });

      // Verify it's gone
      const getResult2 = await kvGet("delete-me");
      expect(getResult2.found).toBe(false);
    });

    it("should return NotFound when deleting missing key", async () => {
      const deleteResult = await kvDelete("missing-delete");
      expect(deleteResult.error).toContain("value not found");
    });

    it("should overwrite existing value", async () => {
      await kvPut("overwrite-key", "first-value");
      const result1 = await kvGet("overwrite-key");
      expect(result1.value).toBe("first-value");

      await kvPut("overwrite-key", "second-value");
      const result2 = await kvGet("overwrite-key");
      expect(result2.value).toBe("second-value");
    });

    it("should handle empty string value", async () => {
      await kvPut("empty-value", "");
      const result = await kvGet("empty-value");
      expect(result).toEqual({ found: true, value: "" });
    });

    it("should handle unicode keys and values", async () => {
      const key = "unicode-key-日本語";
      const value = "unicode-value-🎉-émojis";

      await kvPut(key, value);
      const result = await kvGet(key);
      expect(result).toEqual({ found: true, value });
    });
  });

  describe("Metadata", () => {
    it("should put with metadata and retrieve it", async () => {
      const metadata = { author: "test", version: 1, tags: ["a", "b"] };

      await kvPut("metadata-key", "metadata-value", { metadata });

      const result = await kvGetWithMetadata("metadata-key");
      expect(result.found).toBe(true);
      expect(result.value).toBe("metadata-value");
      expect(result.metadata).toEqual(metadata);
    });

    it("should return null metadata when none set", async () => {
      await kvPut("no-metadata-key", "some-value");

      const result = await kvGetWithMetadata("no-metadata-key");
      expect(result.found).toBe(true);
      expect(result.value).toBe("some-value");
      expect(result.metadata).toBeNull();
    });

    it("should handle complex nested metadata", async () => {
      const metadata = {
        user: {
          id: 123,
          name: "Test User",
          settings: {
            theme: "dark",
            notifications: true,
          },
        },
        timestamps: [1234567890, 1234567891],
      };

      await kvPut("complex-metadata", "value", { metadata });

      const result = await kvGetWithMetadata("complex-metadata");
      expect(result.metadata).toEqual(metadata);
    });
  });

  describe("Validation", () => {
    it("should reject empty key", async () => {
      const result = await kvPut("", "value");
      expect(result.error).toContain("invalid options");
    });

    it("should reject empty key on get", async () => {
      const result = await kvGet("");
      expect(result.error).toContain("invalid options");
    });

    it("should reject empty key on get_with_metadata", async () => {
      const result = await kvGetWithMetadata("");
      expect(result.error).toContain("invalid options");
    });

    it("should reject dot keys", async () => {
      const resultDot = await kvPut(".", "value");
      expect(resultDot.error).toContain("invalid options");

      const resultDotDot = await kvPut("..", "value");
      expect(resultDotDot.error).toContain("invalid options");
    });

    it("should reject dot keys on delete", async () => {
      const resultDot = await kvDelete(".");
      expect(resultDot.error).toContain("invalid options");

      const resultDotDot = await kvDelete("..");
      expect(resultDotDot.error).toContain("invalid options");
    });

    it("should reject oversized keys", async () => {
      const oversizedKey = "a".repeat(513);
      const result = await kvPut(oversizedKey, "value");
      expect(result.error).toContain("invalid options");
    });

    it("should reject oversized keys on get", async () => {
      const oversizedKey = "a".repeat(513);
      const result = await kvGet(oversizedKey);
      expect(result.error).toContain("invalid options");
    });
  });

  describe("List operations", () => {
    it("should list all keys", async () => {
      await kvPut("list-a", "value-a");
      await kvPut("list-b", "value-b");
      await kvPut("list-c", "value-c");

      const result = await kvList();
      expect(result.keys.length).toBe(3);
      expect(result.keys.map((k: { key: string }) => k.key).sort()).toEqual([
        "list-a",
        "list-b",
        "list-c",
      ]);
      expect(result.list_complete).toBe(true);
    });

    it("should omit metadata and expiration unless requested", async () => {
      await kvPut("list-omit-1", "value", {
        ttl_seconds: 60,
        metadata: { tag: "omit" },
      });

      const result = await kvList({ prefix: "list-omit-" });
      expect(result.keys.length).toBe(1);
      expect(result.keys[0].metadata).toBeNull();
      expect(result.keys[0].expires_at).toBeNull();
    });

    it("should list with prefix filter", async () => {
      await kvPut("prefix-foo-1", "value-1");
      await kvPut("prefix-foo-2", "value-2");
      await kvPut("prefix-bar-1", "value-3");
      await kvPut("other-key", "value-4");

      const result = await kvList({ prefix: "prefix-foo" });
      expect(result.keys.length).toBe(2);
      expect(result.keys.map((k: { key: string }) => k.key).sort()).toEqual([
        "prefix-foo-1",
        "prefix-foo-2",
      ]);
    });

    it("should respect limit", async () => {
      await kvPut("limit-1", "v1");
      await kvPut("limit-2", "v2");
      await kvPut("limit-3", "v3");
      await kvPut("limit-4", "v4");
      await kvPut("limit-5", "v5");

      const result = await kvList({ prefix: "limit-", limit: 3 });
      expect(result.keys.length).toBe(3);
      // When more keys exist than limit, list_complete should be false
      expect(result.list_complete).toBe(false);
      expect(result.cursor).toBeTruthy();
    });

    it("should paginate with cursor", async () => {
      // Create 5 keys
      await kvPut("page-1", "v1");
      await kvPut("page-2", "v2");
      await kvPut("page-3", "v3");
      await kvPut("page-4", "v4");
      await kvPut("page-5", "v5");

      // Get first page
      const page1 = await kvList({ prefix: "page-", limit: 2 });
      expect(page1.keys.length).toBe(2);
      expect(page1.list_complete).toBe(false);
      expect(page1.cursor).toBeTruthy();

      // Get second page
      const page2 = await kvList({ prefix: "page-", limit: 2, cursor: page1.cursor });
      expect(page2.keys.length).toBe(2);
      expect(page2.list_complete).toBe(false);

      // Get third page (last)
      const page3 = await kvList({ prefix: "page-", limit: 2, cursor: page2.cursor });
      expect(page3.keys.length).toBe(1);
      expect(page3.list_complete).toBe(true);

      // Collect all keys
      const allKeys = [
        ...page1.keys.map((k: { key: string }) => k.key),
        ...page2.keys.map((k: { key: string }) => k.key),
        ...page3.keys.map((k: { key: string }) => k.key),
      ].sort();
      expect(allKeys).toEqual(["page-1", "page-2", "page-3", "page-4", "page-5"]);
    });

    it("should return empty list for non-matching prefix", async () => {
      await kvPut("something", "value");

      const result = await kvList({ prefix: "nonexistent-prefix" });
      expect(result.keys).toEqual([]);
      expect(result.list_complete).toBe(true);
    });

    it("should include metadata in list results", async () => {
      await kvPut("list-meta-1", "value", { metadata: { tag: "first" } });
      await kvPut("list-meta-2", "value", { metadata: { tag: "second" } });

      const result = await kvList({ prefix: "list-meta-", include_metadata: true });
      expect(result.keys.length).toBe(2);

      // Find the specific keys and check their metadata
      const key1 = result.keys.find((k: { key: string }) => k.key === "list-meta-1");
      const key2 = result.keys.find((k: { key: string }) => k.key === "list-meta-2");

      expect(key1?.metadata).toEqual({ tag: "first" });
      expect(key2?.metadata).toEqual({ tag: "second" });
    });

    it("should include expiration when requested", async () => {
      await kvPut("list-exp-1", "value", { ttl_seconds: 60 });
      const result = await kvList({ prefix: "list-exp-", include_expiration: true });
      expect(result.keys.length).toBe(1);
      expect(result.keys[0].expires_at).toBeTypeOf("number");
    });
  });

  describe("TTL/Expiration", () => {
    // Note: Miniflare may not fully emulate TTL behavior in the same way as production
    // These tests verify the API accepts TTL parameters correctly

    it("should accept TTL parameter without error", async () => {
      // Workers KV requires minimum 60 seconds TTL
      const result = await kvPut("ttl-key", "ttl-value", { ttl_seconds: 60 });
      expect(result).toEqual({ success: true });

      // Verify value is stored
      const getResult = await kvGet("ttl-key");
      expect(getResult.found).toBe(true);
    });

    it("should accept longer TTL", async () => {
      const result = await kvPut("ttl-key-long", "value", { ttl_seconds: 3600 });
      expect(result).toEqual({ success: true });
    });

    it("should reject TTL shorter than minimum", async () => {
      const result = await kvPut("ttl-too-short", "value", { ttl_seconds: 30 });
      expect(result.error).toContain("invalid options");
    });

    it("should reject cache_ttl shorter than minimum", async () => {
      await kvPut("cache-ttl-key", "value");
      const result = await kvGet("cache-ttl-key", { cache_ttl: 30 });
      expect(result.error).toContain("invalid options");
    });

    it("should reject cache_ttl shorter than minimum for get_with_metadata", async () => {
      await kvPut("cache-ttl-meta-key", "value");
      const result = await kvGetWithMetadata("cache-ttl-meta-key", { cache_ttl: 30 });
      expect(result.error).toContain("invalid options");
    });
  });

  describe("Error handling", () => {
    it("should return 400 for missing key in get", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}kv/get`);
      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.error).toContain("missing 'key' query parameter");
    });

    it("should return 400 for missing key in delete", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}kv/delete`, {
        method: "DELETE",
      });
      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.error).toContain("missing 'key' query parameter");
    });

    it("should return 400 for invalid JSON in put", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}kv/put`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: "not valid json",
      });
      expect(response.status).toBe(400);
      const body = await response.json();
      expect(body.error).toContain("invalid request body");
    });

    it("should return 404 for unknown routes", async () => {
      const response = await mf.dispatchFetch(`${mfUrl}unknown`);
      expect(response.status).toBe(404);
    });
  });
});
