import { mkdir, readFile, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { verifyBundle } from "./operator-artifacts.mjs";

const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
for (const name of ["--account-id","--prefix","--evidence-dir","--d1-id","--signed-artifacts","--public-callback-base","--spend-limit-usd","--rate-limit-per-minute"]) {
  if (!args.has(name)) throw new Error(`missing ${name}`);
}
const accountId=args.get("--account-id"), prefix=args.get("--prefix"), evidenceDir=args.get("--evidence-dir");
if (!/^[0-9a-f]{32}$/.test(accountId) || !/^lattice-c5-[a-z0-9]{6,20}$/.test(prefix) || !isAbsolute(evidenceDir) || !isAbsolute(args.get("--signed-artifacts")) || !/^[0-9a-f]{32}$/.test(args.get("--d1-id"))) throw new Error("invalid C5 ownership inputs");
if(args.get("--public-callback-base")!==`https://${prefix}-broker-public.workers.dev`||!/^[1-9]\d{0,5}$/.test(args.get("--rate-limit-per-minute"))||!/^\d+(\.\d{1,2})?$/.test(args.get("--spend-limit-usd")))throw new Error("invalid callback or budget inputs");
const signedInputs=JSON.parse(await readFile(args.get("--signed-artifacts"),"utf8"));
if(typeof signedInputs.operator_bundle_jcs!=="string"||!signedInputs.operator_trust_root||!signedInputs.artifacts)throw new Error("signed operator input bundle is incomplete");
const signedBundle=JSON.parse(signedInputs.operator_bundle_jcs);verifyBundle(signedBundle,signedInputs.operator_trust_root);
const signedBundleHash=`sha256:${createHash("sha256").update(signedInputs.operator_bundle_jcs).digest("hex")}`;
if(signedInputs.operator_bundle_hash!==signedBundleHash)throw new Error("signed operator input hash mismatch");
const brokerRoot=resolve(new URL("../..",import.meta.url).pathname);
await mkdir(evidenceDir,{recursive:true,mode:0o700});const preflightBundle=join(evidenceDir,"operator-artifact-bundle.json"),preflightTrust=join(evidenceDir,"operator-trust-root.json");await writeFile(preflightBundle,signedInputs.operator_bundle_jcs,{mode:0o600});await writeFile(preflightTrust,JSON.stringify(signedInputs.operator_trust_root),{mode:0o600});
const sharedVerification=spawnSync("cargo",["run","--quiet","--bin","broker-artifact-verifier","--","--bundle",preflightBundle,"--trust-root",preflightTrust,"--now",new Date().toISOString().replace(/\.\d{3}Z$/,"Z")],{cwd:brokerRoot,encoding:"utf8"});if(sharedVerification.status!==0)throw new Error("shared Rust operator artifact verification failed");
const providerRoot=resolve(brokerRoot,"../provider-google-workers");
const tokenName=`${prefix}-google-token-egress`, providerName=`${prefix}-google-provider-egress`, authDriverName=`${prefix}-auth-driver`;
const callback=`${args.get("--public-callback-base")}/v0.2/credential-callback`;
const plan={schema_version:"0.2",owner:"lattice-c5-broker-plane",account_id:accountId,prefix,workers:[authDriverName,tokenName,providerName,`${prefix}-broker-private`,`${prefix}-broker-public`],steps:["local_preflight","deploy_owned_auth_driver","deploy_owned_token_egress","deploy_owned_provider_egress","pin_exact_private_dependencies","deploy_v2_broker","write_combined_evidence"],rollback_order:[`${prefix}-broker-public`,`${prefix}-broker-private`,providerName,tokenName,authDriverName]};
await mkdir(evidenceDir,{recursive:true,mode:0o700});
await writeFile(join(evidenceDir,"c5-plan.json"),`${JSON.stringify(plan,null,2)}\n`,{mode:0o600});
if(args.get("--mode")!=="apply"){console.log(JSON.stringify(plan,null,2));console.log("dry-run complete; zero remote commands executed");process.exit(0);}
if(args.get("--approve-create-disposable")!=="yes"||args.get("--approve-cleanup")!=="yes"||args.get("--approve-private-deploy")!=="yes"||!process.env.CLOUDFLARE_API_TOKEN)throw new Error("apply requires all exact approvals and CLOUDFLARE_API_TOKEN");
const run=(cwd,command)=>{const result=spawnSync(command[0],command.slice(1),{cwd,encoding:"utf8",env:{...process.env,CLOUDFLARE_ACCOUNT_ID:accountId}});if(result.status!==0)throw new Error(`command failed:${command.slice(0,3).join(" ")}`);return result.stdout;};
const providerEvidence=join(evidenceDir,"google-egress");
const providerState=join(providerEvidence,"google-egress-ownership.json");
let providerApplied=false, authDriverApplied=false;
try{
  const whoami=JSON.parse(run(providerRoot,["npx","wrangler","whoami","--json"]));
  if(whoami.account_id!==accountId)throw new Error("account mismatch");
  for(const name of [authDriverName,tokenName,providerName]){
    const existing=JSON.parse(run(providerRoot,["npx","wrangler","deployments","list","--name",name,"--json"]));
    if(!Array.isArray(existing)||existing.length!==0)throw new Error(`owned egress target already exists:${name}`);
  }
  const authSecrets=JSON.parse(run(brokerRoot,["npx","wrangler","secret","list","--name",authDriverName,"--format","json"]));
  if(!Array.isArray(authSecrets)||!authSecrets.some((value)=>value.name==="AUTH_DRIVER_SERVICE_AUTH"))throw new Error("secret missing:auth-driver");
  run(brokerRoot,["npx","wrangler","deploy","--config","deploy/auth-driver/wrangler.jsonc","--name",authDriverName]);
  authDriverApplied=true;
  run(providerRoot,["node","scripts/deploy.mjs","--account-id",accountId,"--prefix",prefix,"--callback-uri",callback,"--evidence-dir",providerEvidence,"--mode","apply","--approve-private-deploy","yes"]);
  providerApplied=true;
  const exactDeployment=(name)=>{const values=JSON.parse(run(providerRoot,["npx","wrangler","deployments","list","--name",name,"--json"]));if(!Array.isArray(values)||values.length!==1)throw new Error(`owned target is not an exact fresh deployment:${name}`);const value=values[0];const sourceHash=value.source_hash??value.metadata?.source_hash;if(!/^[0-9a-f]{64}$/.test(sourceHash??"")||!/^[A-Za-z0-9._:-]{6,256}$/.test(value.id??""))throw new Error(`deployment evidence invalid:${name}`);return {name,account_id:accountId,deployment_id:value.id,source_hash:sourceHash};};
  const dependencies={schema_version:"2",account_id:accountId,prefix,d1_database_id:args.get("--d1-id"),artifacts:signedInputs.artifacts,operator_bundle_jcs:signedInputs.operator_bundle_jcs,operator_bundle_hash:signedInputs.operator_bundle_hash,operator_trust_root:signedInputs.operator_trust_root,workers:{},services:{AUTH_DRIVER_SERVICE:exactDeployment(authDriverName),GOOGLE_TOKEN_SERVICE:exactDeployment(tokenName),GOOGLE_PROVIDER_SERVICE:exactDeployment(providerName)}};
  const dependencyPath=join(evidenceDir,"approved-dependencies.generated.json");
  await writeFile(dependencyPath,`${JSON.stringify(dependencies,null,2)}\n`,{mode:0o600});
  run(brokerRoot,["node","deploy/scripts/deploy.mjs","--account-id",accountId,"--prefix",prefix,"--evidence-dir",join(evidenceDir,"broker"),"--d1-id",args.get("--d1-id"),"--approved-dependencies",dependencyPath,"--google-provider-service",providerName,"--google-token-service",tokenName,"--auth-driver-service",authDriverName,"--public-callback-base",args.get("--public-callback-base"),"--spend-limit-usd",args.get("--spend-limit-usd"),"--rate-limit-per-minute",args.get("--rate-limit-per-minute"),"--mode","apply","--approve-create-disposable","yes","--approve-cleanup","yes"]);
  const deployments=plan.workers.map((name)=>exactDeployment(name));
  const state={...plan,status:"deployed",deployments,provider_ownership_state:providerState,approved_dependencies:dependencyPath,d1_database:{id:args.get("--d1-id"),owned:false}};
  await writeFile(join(evidenceDir,"c5-ownership.json"),`${JSON.stringify(state,null,2)}\n`,{mode:0o600});
  console.log("C5 broker plane deployed with owned private egress");
}catch(error){
  let forwardFix=false;
  try {
    const brokerEvidence=JSON.parse(await readFile(join(evidenceDir,"broker","qualification-evidence.json"),"utf8"));
    forwardFix=brokerEvidence.migration_state==="forward_fix_required";
  } catch {}
  if(forwardFix){
    await writeFile(join(evidenceDir,"c5-forward-fix-required.json"),`${JSON.stringify({schema_version:"0.2",status:"forward_fix_required",d1_preserved:true,private_fence_worker_preserved:true,owned_egress_preserved:true},null,2)}\n`,{mode:0o600});
  } else {
    if(providerApplied){try{run(providerRoot,["node","scripts/cleanup.mjs","--ownership-state",providerState,"--mode","apply","--approve-cleanup","yes"]);}catch{throw new Error("broker deployment failed and owned egress cleanup also failed");}}
    if(authDriverApplied){try{run(brokerRoot,["npx","wrangler","delete","--name",authDriverName,"--force"]);}catch{throw new Error("broker deployment failed and owned auth-driver cleanup also failed");}}
  }
  throw error;
}
