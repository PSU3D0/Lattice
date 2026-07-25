import { mkdir, readFile, writeFile } from "node:fs/promises";
import { isAbsolute, join, resolve } from "node:path";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { verifyBundle } from "./operator-artifacts.mjs";
import { assertAuthenticatedAccount, validateD1Id } from "./cloudflare-identifiers.mjs";
import { validatePublicCallbackBase, verifyLiveWorkersSubdomain } from "./workers-subdomain.mjs";
import {
  deleteWorkerAndVerifyAbsent,
  deployPrivateWorker,
  isCanonicalUuid,
  loadPrivateWorkerSecrets,
} from "./private-worker-deploy-lib.mjs";
import { computeUploadedSourceSha256 } from "./uploaded-source.mjs";

const args = new Map();
for (let i = 2; i < process.argv.length; i += 2) args.set(process.argv[i], process.argv[i + 1]);
for (const name of ["--account-id","--prefix","--evidence-dir","--d1-id","--signed-artifacts","--secrets-file","--workers-subdomain","--public-callback-base","--spend-limit-usd","--rate-limit-per-minute"]) {
  if (!args.has(name)) throw new Error(`missing ${name}`);
}
const accountId=args.get("--account-id"), prefix=args.get("--prefix"), evidenceDir=args.get("--evidence-dir");
if (!/^[0-9a-f]{32}$/.test(accountId) || !/^lattice-c5-[a-z0-9]{6,20}$/.test(prefix) || !isAbsolute(evidenceDir) || !isAbsolute(args.get("--signed-artifacts"))) throw new Error("invalid C5 ownership inputs");
validateD1Id(args.get("--d1-id"));
const workersSubdomain=args.get("--workers-subdomain");
const {publicCallbackBase,googleOauthRedirectUri}=validatePublicCallbackBase(prefix,workersSubdomain,args.get("--public-callback-base"));
if(!/^[1-9]\d{0,5}$/.test(args.get("--rate-limit-per-minute"))||!/^\d+(\.\d{1,2})?$/.test(args.get("--spend-limit-usd")))throw new Error("invalid budget inputs");
const signedInputs=JSON.parse(await readFile(args.get("--signed-artifacts"),"utf8"));
if(typeof signedInputs.operator_bundle_jcs!=="string"||!signedInputs.operator_trust_root||!signedInputs.artifacts)throw new Error("signed operator input bundle is incomplete");
const signedBundle=JSON.parse(signedInputs.operator_bundle_jcs);verifyBundle(signedBundle,signedInputs.operator_trust_root);
const signedBundleHash=`sha256:${createHash("sha256").update(signedInputs.operator_bundle_jcs).digest("hex")}`;
if(signedInputs.operator_bundle_hash!==signedBundleHash)throw new Error("signed operator input hash mismatch");
const brokerRoot=resolve(new URL("../..",import.meta.url).pathname);
const repositoryRoot=resolve(brokerRoot,"../..");
const secretValues=await loadPrivateWorkerSecrets(args.get("--secrets-file"),repositoryRoot);
await mkdir(evidenceDir,{recursive:true,mode:0o700});const preflightBundle=join(evidenceDir,"operator-artifact-bundle.json"),preflightTrust=join(evidenceDir,"operator-trust-root.json");await writeFile(preflightBundle,signedInputs.operator_bundle_jcs,{mode:0o600});await writeFile(preflightTrust,JSON.stringify(signedInputs.operator_trust_root),{mode:0o600});
const sharedVerification=spawnSync("cargo",["run","--quiet","--bin","broker-artifact-verifier","--","--bundle",preflightBundle,"--trust-root",preflightTrust,"--now",new Date().toISOString().replace(/\.\d{3}Z$/,"Z")],{cwd:brokerRoot,encoding:"utf8"});if(sharedVerification.status!==0)throw new Error("shared Rust operator artifact verification failed");
const providerRoot=resolve(brokerRoot,"../provider-google-workers");
const authDriverUploadedSourceSha256=await computeUploadedSourceSha256([{path:"deploy/auth-driver/src/index.mjs",file:join(brokerRoot,"deploy/auth-driver/src/index.mjs")}]);
const tokenName=`${prefix}-google-token-egress`, providerName=`${prefix}-google-provider-egress`, authDriverName=`${prefix}-auth-driver`;
const plan={schema_version:"0.3",owner:"lattice-c5-broker-plane",account_id:accountId,prefix,workers_subdomain:workersSubdomain,public_callback_base:publicCallbackBase,google_oauth_redirect_uri:googleOauthRedirectUri,workers:[authDriverName,tokenName,providerName,`${prefix}-broker-private`,`${prefix}-broker-public`],private_worker_secrets:{[authDriverName]:Object.keys(secretValues["auth-driver"]).map(name=>({name,install_status:"planned"})),[tokenName]:[...Object.keys(secretValues["google-token-egress"]),"GOOGLE_OAUTH_REDIRECT_URI"].map(name=>({name,install_status:"planned"})),[providerName]:Object.keys(secretValues["google-provider-egress"]).map(name=>({name,install_status:"planned"})),[`${prefix}-broker-private`]:Object.keys(secretValues["broker-private"]).map(name=>({name,install_status:"planned"})),[`${prefix}-broker-public`]:[]},steps:["local_preflight_and_exact_secrets_file_validation","verify_live_workers_subdomain","prove_deploy_install_verify_capture_auth_driver","prove_deploy_install_verify_capture_token_egress","prove_deploy_install_verify_capture_provider_egress","pin_cloudflare_deployment_and_version_ids_with_operator_computed_uploaded_source_digests","prove_deploy_install_verify_capture_fence_aware_broker_private","apply_0003_production_v2_cutover","prove_deploy_verify_capture_broker_public_last","verify_public_health_ready_and_callback","write_combined_evidence"],rollback_order:[`${prefix}-broker-public`,`${prefix}-broker-private`,providerName,tokenName,authDriverName]};
await mkdir(evidenceDir,{recursive:true,mode:0o700});
await writeFile(join(evidenceDir,"c5-plan.json"),`${JSON.stringify(plan,null,2)}\n`,{mode:0o600});
if(args.get("--mode")!=="apply"){console.log(JSON.stringify(plan,null,2));console.log("dry-run complete; zero remote commands executed");process.exit(0);}
if(args.get("--approve-create-disposable")!=="yes"||args.get("--approve-cleanup")!=="yes"||args.get("--approve-private-deploy")!=="yes"||!process.env.CLOUDFLARE_API_TOKEN)throw new Error("apply requires all exact approvals and CLOUDFLARE_API_TOKEN");
await verifyLiveWorkersSubdomain({accountId,workersSubdomain,apiToken:process.env.CLOUDFLARE_API_TOKEN});
const runResult=(cwd,command,options={})=>{const result=spawnSync(command[0],command.slice(1),{cwd,encoding:"utf8",input:options.input,env:{...process.env,CLOUDFLARE_ACCOUNT_ID:accountId}});return {status:result.status??1,stdout:result.stdout??"",stderr:result.stderr??""};};
// Diagnosability: a fail-closed deploy path is unusable when the child stderr is
// discarded. Secret values never reach argv and child scripts redact their own
// errors, so surfacing the stderr tail discloses no material.
const run=(cwd,command)=>{const result=runResult(cwd,command);if(result.status!==0)throw new Error(`command failed:${command.slice(0,3).join(" ")}\n--- stderr ---\n${result.stderr.slice(-4000)}\n--- stdout ---\n${result.stdout.slice(-2000)}`);return result.stdout;};
const authRunner={async run(_step,command,options){return runResult(brokerRoot,command,options);}};
const providerEvidence=join(evidenceDir,"google-egress");
const providerState=join(providerEvidence,"google-egress-ownership.json");
const runStartedAt=new Date(Math.floor(Date.now()/1000)*1000).toISOString();
let providerApplied=false, authDriverApplied=false, brokerApplyStarted=false, brokerApplyCompleted=false;
try{
  assertAuthenticatedAccount(run(providerRoot,["npx","wrangler","whoami","--json"]),accountId);
  const authDriverDeployment=await deployPrivateWorker({runner:authRunner,step:"auth_driver",name:authDriverName,accountId,config:"deploy/auth-driver/wrangler.jsonc",secrets:secretValues["auth-driver"],uploadedSourceSha256:authDriverUploadedSourceSha256,runStartedAt});
  authDriverApplied=authDriverDeployment.created_by_run;
  run(providerRoot,["node","scripts/deploy.mjs","--account-id",accountId,"--prefix",prefix,"--workers-subdomain",workersSubdomain,"--callback-uri",googleOauthRedirectUri,"--evidence-dir",providerEvidence,"--secrets-file",args.get("--secrets-file"),"--mode","apply","--approve-private-deploy","yes"]);
  providerApplied=true;
  const providerOwnership=JSON.parse(await readFile(providerState,"utf8"));
  const providerPins=new Map((providerOwnership.workers??[]).map(worker=>[worker.name,worker]));
  const normalizedPin=(worker,name)=>{if(worker?.name!==name||!/^[0-9a-f]{64}$/.test(worker.uploaded_source_sha256??"")||!isCanonicalUuid(worker.deployment_id)||!isCanonicalUuid(worker.version_id)||!Number.isInteger(worker.deployment_count)||worker.deployment_count<1||!Array.isArray(worker.triggered_by_annotations)||!Array.isArray(worker.secrets)||worker.secrets.some(secret=>secret.install_status!=="installed_and_verified"))throw new Error("private worker ownership evidence invalid");return {name,account_id:accountId,deployment_id:worker.deployment_id,version_id:worker.version_id,deployment_count:worker.deployment_count,triggered_by_annotations:worker.triggered_by_annotations,run_started_at:worker.run_started_at,uploaded_source_sha256:worker.uploaded_source_sha256};};
  const authPin=normalizedPin(authDriverDeployment,authDriverName),tokenPin=normalizedPin(providerPins.get(tokenName),tokenName),providerPin=normalizedPin(providerPins.get(providerName),providerName);
  const dependencies={schema_version:"3",account_id:accountId,prefix,d1_database_id:args.get("--d1-id"),artifacts:signedInputs.artifacts,operator_bundle_jcs:signedInputs.operator_bundle_jcs,operator_bundle_hash:signedInputs.operator_bundle_hash,operator_trust_root:signedInputs.operator_trust_root,workers:{},services:{AUTH_DRIVER_SERVICE:authPin,GOOGLE_TOKEN_SERVICE:tokenPin,GOOGLE_PROVIDER_SERVICE:providerPin}};
  const dependencyPath=join(evidenceDir,"approved-dependencies.generated.json");
  await writeFile(dependencyPath,`${JSON.stringify(dependencies,null,2)}\n`,{mode:0o600});
  brokerApplyStarted=true;
  run(brokerRoot,["node","deploy/scripts/deploy.mjs","--account-id",accountId,"--prefix",prefix,"--evidence-dir",join(evidenceDir,"broker"),"--d1-id",args.get("--d1-id"),"--approved-dependencies",dependencyPath,"--secrets-file",args.get("--secrets-file"),"--google-provider-service",providerName,"--google-token-service",tokenName,"--auth-driver-service",authDriverName,"--workers-subdomain",workersSubdomain,"--public-callback-base",publicCallbackBase,"--spend-limit-usd",args.get("--spend-limit-usd"),"--rate-limit-per-minute",args.get("--rate-limit-per-minute"),"--mode","apply","--approve-create-disposable","yes","--approve-cleanup","yes"]);
  brokerApplyCompleted=true;
  const brokerQualification=JSON.parse(await readFile(join(evidenceDir,"broker","qualification-evidence.json"),"utf8"));
  const brokerDeployment=(name)=>{const value=brokerQualification.deployments?.[name];if(!/^[0-9a-f]{64}$/.test(value?.uploaded_source_sha256??"")||!isCanonicalUuid(value?.deployment_id)||!isCanonicalUuid(value?.version_id)||!Number.isInteger(value?.deployment_count)||!Array.isArray(value?.triggered_by_annotations))throw new Error(`broker deployment evidence invalid:${name}`);return {name,account_id:accountId,...value};};
  const deployments=[authPin,tokenPin,providerPin,brokerDeployment(`${prefix}-broker-private`),brokerDeployment(`${prefix}-broker-public`)];
  const state={...plan,status:"deployed",private_worker_secrets:{[authDriverName]:authDriverDeployment.secrets,[tokenName]:providerPins.get(tokenName).secrets,[providerName]:providerPins.get(providerName).secrets,...brokerQualification.worker_secrets},deployments,provider_ownership_state:providerState,approved_dependencies:dependencyPath,d1_database:{id:args.get("--d1-id"),owned:false}};
  await writeFile(join(evidenceDir,"c5-ownership.json"),`${JSON.stringify(state,null,2)}\n`,{mode:0o600});
  console.log("C5 broker plane deployed with owned private egress");
}catch(error){
  let brokerEvidence;
  try {brokerEvidence=JSON.parse(await readFile(join(evidenceDir,"broker","qualification-evidence.json"),"utf8"));} catch {}
  const forwardFix=brokerApplyCompleted||brokerEvidence?.migration_state==="forward_fix_required";
  const safeCreationRollback=!brokerApplyStarted||(brokerEvidence?.status==="failed"&&brokerEvidence?.migration_state===undefined);
  if(forwardFix||!safeCreationRollback){
    await writeFile(join(evidenceDir,"c5-forward-fix-required.json"),`${JSON.stringify({schema_version:"0.3",status:"forward_fix_required",d1_preserved:true,private_fence_worker_preserved:true,owned_egress_preserved:true},null,2)}\n`,{mode:0o600});
  } else {
    if(providerApplied){try{run(providerRoot,["node","scripts/cleanup.mjs","--ownership-state",providerState,"--mode","apply","--approve-cleanup","yes"]);}catch(cleanupError){throw new Error(`${error instanceof Error?error.message:String(error)}; owned egress cleanup failed`,{cause:cleanupError});}}
    if(authDriverApplied){try{const cleanupEvidence=await deleteWorkerAndVerifyAbsent({runner:authRunner,name:authDriverName,deleteStep:"auth_driver:orchestrator_cleanup",failureMessage:"owned auth-driver cleanup failed"});if(error&&typeof error==="object")error.cleanupEvidence=cleanupEvidence;}catch(cleanupError){const combined=new Error(`${error instanceof Error?error.message:String(error)}; owned auth-driver cleanup failed`,{cause:cleanupError});combined.cleanupEvidence=cleanupError.cleanupEvidence;throw combined;}}
  }
  throw error;
}
