import { readFile } from "node:fs/promises";
import { isAbsolute } from "node:path";
import { spawnSync } from "node:child_process";
import { validatePublicCallbackBase } from "./workers-subdomain.mjs";
import { assertAuthenticatedAccount, validateD1Id } from "./cloudflare-identifiers.mjs";

const args=new Map();for(let i=2;i<process.argv.length;i+=2)args.set(process.argv[i],process.argv[i+1]);
const path=args.get("--ownership-state");if(!isAbsolute(path??""))throw new Error("ownership state must be absolute");
const state=JSON.parse(await readFile(path,"utf8"));
if(state.schema_version!=="0.3"||state.owner!=="lattice-c5-broker-plane"||state.status!=="deployed"||!/^lattice-c5-[a-z0-9]{6,20}$/.test(state.prefix??"")||!Array.isArray(state.deployments)||state.deployments.length!==5||state.d1_database?.owned!==false)throw new Error("C5 ownership state invalid");
validateD1Id(state.d1_database.id);
const callback=validatePublicCallbackBase(state.prefix,state.workers_subdomain,state.public_callback_base);if(state.google_oauth_redirect_uri!==callback.googleOauthRedirectUri)throw new Error("C5 ownership callback state invalid");
const expected=[`${state.prefix}-auth-driver`,`${state.prefix}-google-token-egress`,`${state.prefix}-google-provider-egress`,`${state.prefix}-broker-private`,`${state.prefix}-broker-public`];
if(expected.some((name,index)=>state.deployments[index]?.name!==name))throw new Error("C5 ownership target mismatch");
const plan={schema_version:"0.3",account_id:state.account_id,verify_before_delete:expected,delete:[...expected].reverse(),preserve_d1:state.d1_database.id};
if(args.get("--mode")!=="apply"){console.log(JSON.stringify(plan,null,2));console.log("cleanup dry-run complete; zero remote commands executed");process.exit(0);}
if(args.get("--approve-cleanup")!=="yes"||!process.env.CLOUDFLARE_API_TOKEN)throw new Error("cleanup approval and API token required");
const run=(command)=>{const result=spawnSync(command[0],command.slice(1),{encoding:"utf8",env:{...process.env,CLOUDFLARE_ACCOUNT_ID:state.account_id}});if(result.status!==0)throw new Error("cleanup command failed");return result.stdout;};
assertAuthenticatedAccount(run(["npx","wrangler","whoami","--json"]),state.account_id);
for(const target of state.deployments){if(!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(target.deployment_id??"")||!/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/.test(target.version_id??"")||!/^[0-9a-f]{64}$/.test(target.uploaded_source_sha256??""))throw new Error(`cleanup ownership evidence invalid:${target.name}`);const values=JSON.parse(run(["npx","wrangler","deployments","list","--name",target.name,"--json"]));const matches=values.filter((value)=>value.source==="wrangler"&&value.id===target.deployment_id&&value.versions?.some(version=>version?.version_id===target.version_id));if(matches.length!==1)throw new Error(`cleanup ownership mismatch:${target.name}`);}
for(const name of plan.delete)run(["npx","wrangler","delete","--name",name,"--force"]);
console.log("C5 broker plane cleanup complete; D1 preserved");
