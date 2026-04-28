import { describe, it, expect, afterAll, beforeAll } from "vitest";
import { Miniflare } from "miniflare";

// Initialize Miniflare with our test worker and a D1 binding.
const mf = new Miniflare({
  workers: [
    {
      scriptPath: "./build/index.js",
      compatibilityDate: "2024-09-23",
      modules: true,
      modulesRules: [
        { type: "CompiledWasm", include: ["**/*.wasm"], fallthrough: true },
      ],
      d1Databases: { DB: "test-d1-database" },
    },
  ],
});

const mfUrl = await mf.ready;

type SqlValue =
  | null
  | boolean
  | number
  | string
  | { bytes_base64: string };

type SqlStatement = {
  sql: string;
  params?: SqlValue[];
  named_params?: Record<string, SqlValue>;
  options?: {
    timeout_ms?: number | null;
    max_rows?: number | null;
    statement_kind?: "Read" | "Write" | "Ddl" | null;
  };
};

async function postJson(path: string, body: unknown): Promise<any> {
  const response = await mf.dispatchFetch(`${mfUrl}${path}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return response.json();
}

const adminDdl = (stmt: SqlStatement) => postJson("admin/ddl", stmt);
const writeExecute = (stmt: SqlStatement) => postJson("write/execute", stmt);
const writeReturning = (stmt: SqlStatement) => postJson("write/returning", stmt);
const readQuery = (stmt: SqlStatement) => postJson("read/query", stmt);

beforeAll(async () => {
  // Ensure schema starts clean: drop and recreate.
  await adminDdl({ sql: "DROP TABLE IF EXISTS items" });
  const create = await adminDdl({
    sql:
      "CREATE TABLE items (" +
      "  id INTEGER PRIMARY KEY AUTOINCREMENT," +
      "  name TEXT NOT NULL UNIQUE," +
      "  qty INTEGER NOT NULL DEFAULT 0" +
      ")",
  });
  expect(create.ok).toBe(true);
});

afterAll(async () => {
  await mf.dispose();
});

describe("cap-sql-workers-d1 E2E", () => {
  it("health endpoint is up", async () => {
    const r = await mf.dispatchFetch(`${mfUrl}health`);
    expect(r.status).toBe(200);
    expect(await r.json()).toEqual({ status: "ok" });
  });

  it("reports CloudflareD1 capability_info", async () => {
    const r = await mf.dispatchFetch(`${mfUrl}capability_info`);
    expect(r.status).toBe(200);
    const info = await r.json();
    expect(info.dialect).toBe("CloudflareD1");
    expect(info.placeholder_styles).toEqual(["Question"]);
    expect(info.extensions.prototype).toBe(true);
    expect(info.extensions.provider).toBe("sqlx-d1");
  });

  it("inserts with RETURNING and reads it back", async () => {
    // Clear any prior test residue.
    await writeExecute({ sql: "DELETE FROM items" });

    const insert = await writeReturning({
      sql: "INSERT INTO items (name, qty) VALUES (?, ?) RETURNING id, name, qty",
      params: ["alpha", 1],
    });
    expect(insert.ok).toBe(true);
    expect(insert.rows_returned).toBe(1);
    expect(insert.columns.map((c: any) => c.name)).toEqual(["id", "name", "qty"]);
    const [id, name, qty] = insert.rows[0];
    expect(typeof id).toBe("number");
    expect(name).toBe("alpha");
    expect(qty).toBe(1);

    const select = await readQuery({
      sql: "SELECT id, name, qty FROM items WHERE name = ?",
      params: ["alpha"],
    });
    expect(select.ok).toBe(true);
    expect(select.rows_returned).toBe(1);
    expect(select.rows[0][1]).toBe("alpha");
    expect(select.rows[0][2]).toBe(1);
  });

  it("update with RETURNING reflects new values", async () => {
    await writeExecute({ sql: "DELETE FROM items" });
    await writeReturning({
      sql: "INSERT INTO items (name, qty) VALUES (?, ?) RETURNING id",
      params: ["beta", 2],
    });

    const updated = await writeReturning({
      sql: "UPDATE items SET qty = qty + ? WHERE name = ? RETURNING id, name, qty",
      params: [10, "beta"],
    });
    expect(updated.ok).toBe(true);
    expect(updated.rows_returned).toBe(1);
    expect(updated.rows[0][1]).toBe("beta");
    expect(updated.rows[0][2]).toBe(12);
  });

  it("delete with RETURNING removes the row and returns it", async () => {
    await writeExecute({ sql: "DELETE FROM items" });
    await writeReturning({
      sql: "INSERT INTO items (name, qty) VALUES (?, ?) RETURNING id",
      params: ["gamma", 7],
    });

    const deleted = await writeReturning({
      sql: "DELETE FROM items WHERE name = ? RETURNING id, name, qty",
      params: ["gamma"],
    });
    expect(deleted.ok).toBe(true);
    expect(deleted.rows_returned).toBe(1);
    expect(deleted.rows[0][1]).toBe("gamma");
    expect(deleted.rows[0][2]).toBe(7);

    const after = await readQuery({
      sql: "SELECT * FROM items WHERE name = ?",
      params: ["gamma"],
    });
    expect(after.rows_returned).toBe(0);
  });

  it("select returns multiple rows in column order", async () => {
    await writeExecute({ sql: "DELETE FROM items" });
    await writeReturning({
      sql: "INSERT INTO items (name, qty) VALUES (?, ?) RETURNING id",
      params: ["a", 1],
    });
    await writeReturning({
      sql: "INSERT INTO items (name, qty) VALUES (?, ?) RETURNING id",
      params: ["b", 2],
    });
    await writeReturning({
      sql: "INSERT INTO items (name, qty) VALUES (?, ?) RETURNING id",
      params: ["c", 3],
    });

    const r = await readQuery({
      sql: "SELECT name, qty FROM items ORDER BY name",
    });
    expect(r.ok).toBe(true);
    expect(r.rows_returned).toBe(3);
    expect(r.rows.map((row: any[]) => row[0])).toEqual(["a", "b", "c"]);
    expect(r.rows.map((row: any[]) => row[1])).toEqual([1, 2, 3]);
  });

  it("normalizes UNIQUE constraint violation to ConstraintViolation::Unique", async () => {
    await writeExecute({ sql: "DELETE FROM items" });
    const ok = await writeReturning({
      sql: "INSERT INTO items (name, qty) VALUES (?, ?) RETURNING id",
      params: ["dup", 1],
    });
    expect(ok.ok).toBe(true);

    const conflict = await writeReturning({
      sql: "INSERT INTO items (name, qty) VALUES (?, ?) RETURNING id",
      params: ["dup", 2],
    });
    expect(conflict.ok).toBe(false);
    expect(conflict.kind).toBe("ConstraintViolation");
    expect(conflict.constraint_kind).toBe("Unique");
    // Constraint name extraction is best-effort; just ensure something was captured
    // or the message at least mentions UNIQUE.
    expect(
      String(conflict.message).toLowerCase().includes("unique") ||
        String(conflict.constraint ?? "").length > 0,
    ).toBe(true);
  });

  it("rejects mixing positional and named parameters at the SqlStatement layer", async () => {
    const r = await readQuery({
      sql: "SELECT ?",
      params: [1],
      named_params: { x: 2 },
    });
    expect(r.ok).toBe(false);
    expect(r.kind).toBe("InvalidParams");
  });

  it("rejects named parameters as Unsupported", async () => {
    const r = await readQuery({
      sql: "SELECT :x",
      named_params: { x: 1 },
    });
    expect(r.ok).toBe(false);
    expect(r.kind).toBe("Unsupported");
    expect(r.feature).toBe("NamedParams");
  });

  it("rejects empty statement as InvalidStatement", async () => {
    const r = await readQuery({ sql: "   " });
    expect(r.ok).toBe(false);
    expect(r.kind).toBe("InvalidStatement");
  });
});
