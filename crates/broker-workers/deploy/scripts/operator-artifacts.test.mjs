import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { generateKeyPairSync } from "node:crypto";
import { verifyBundle } from "./operator-artifacts.mjs";

test("operator tool signs and verifies every required C1 artifact and rejects mutations before remote work",async()=>{
 const dir=await mkdtemp(join(tmpdir(),"operator-artifacts-")),seed="Bw".repeat(21)+"Bw";
 const key=join(dir,"operator.seed"),configPath=join(dir,"config.json"),bundlePath=join(dir,"bundle.json"),trustPath=join(dir,"trust.json");await writeFile(key,Buffer.alloc(32,7).toString("base64url"),{mode:0o600});
 const vectors=JSON.parse(await readFile(new URL("../../../../impl-docs/spec/credential-plane-protocol-vectors.json",import.meta.url),"utf8"));const value=schema=>structuredClone(vectors.signed_artifact_vectors.find(item=>item.artifact_schema===schema).artifact);
 const config={key_id:"operator-test",activation_recipient:{key_id:"activation-test",public_key_b64u:Buffer.alloc(32,9).toString("base64url"),suite:"DHKEM(X25519,HKDF-SHA256)/HKDF-SHA256/AES-256-GCM"},not_before:"2020-01-01T00:00:00Z",expires_at:"2035-01-01T00:00:00Z",artifacts:{deployment_standing_authority:[{schema:"StandingAuthority",value:value("StandingAuthority")}],deployment_contract_set:[{schema:"ContractSet",value:value("ContractSet")}],registry_definitions:[{schema:"RegistryDefinition",value:value("RegistryDefinition")}],registry_decisions:[{schema:"RegistryDecision",value:value("RegistryDecision")}],historical_inventory:[{schema:"LegacyAdmissionInventory",value:value("LegacyAdmissionInventory")}],historical_key_evidence:[{schema:"HistoricalVerificationKeyArchive",value:value("HistoricalVerificationKeyArchive")}]}};await writeFile(configPath,JSON.stringify(config));
 const built=spawnSync(process.execPath,[new URL("./operator-artifacts.mjs",import.meta.url).pathname,"build","--config",configPath,"--key-file",key,"--output",bundlePath],{encoding:"utf8"});assert.equal(built.status,0,built.stderr);const exact=(await readFile(bundlePath,"utf8")).trim(),bundle=JSON.parse(exact),trust={key_id:bundle.key_id,public_key_b64u:bundle.public_key_b64u};await writeFile(trustPath,JSON.stringify(trust));assert.equal(verifyBundle(bundle,trust,Date.parse("2026-01-01T00:00:00Z")/1000),true);
 const checked=spawnSync(process.execPath,[new URL("./operator-artifacts.mjs",import.meta.url).pathname,"verify","--bundle",bundlePath,"--trust-root",trustPath],{encoding:"utf8"});assert.equal(checked.status,0,checked.stderr);
 let remote=0;for(const mutate of [b=>{b.expires_at="2021-01-01T00:00:00Z"},b=>{b.revoked_at="2025-01-01T00:00:00Z"},b=>{b.artifacts.registry_decisions[0].hash=`sha256:${"0".repeat(64)}`},b=>{b.signature.value=b.signature.value.replace(/^./,"A")}]){const bad=structuredClone(bundle);mutate(bad);assert.throws(()=>verifyBundle(bad,trust,Date.parse("2026-01-01T00:00:00Z")/1000));assert.equal(remote,0);}const wrong=generateKeyPairSync("ed25519").publicKey.export({format:"jwk"});assert.throws(()=>verifyBundle(bundle,{key_id:bundle.key_id,public_key_b64u:wrong.x},Date.parse("2026-01-01T00:00:00Z")/1000));assert.equal(remote,0);
});
