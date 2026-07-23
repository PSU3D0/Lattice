const MAX_BODY = 256 * 1024;
function safeEqual(left, right) { const a=new TextEncoder().encode(left??""),b=new TextEncoder().encode(right??"");let mismatch=a.length^b.length;for(let i=0;i<Math.max(a.length,b.length);i++)mismatch|=(a[i]??0)^(b[i]??0);return mismatch===0; }
function fail(status=400){return Response.json({error:status===401?"unauthorized":"invalid"},{status});}
async function body(request){if(request.headers.get("content-type")?.split(";",1)[0]!=="application/json")throw new Error();const bytes=new Uint8Array(await request.arrayBuffer());if(bytes.length===0||bytes.length>MAX_BODY)throw new Error();return JSON.parse(new TextDecoder().decode(bytes));}
function opaque(value){return typeof value==="string"&&/^[A-Za-z0-9_-]{4,1048576}$/.test(value);}
function decode(value){if(!opaque(value))throw new Error();return Uint8Array.from(atob(value.replaceAll("-","+").replaceAll("_","/").padEnd(Math.ceil(value.length/4)*4,"=")),c=>c.charCodeAt(0));}
async function bounded(response){const bytes=new Uint8Array(await response.arrayBuffer());if(bytes.length>MAX_BODY||response.status<200||response.status>=300)throw new Error();return JSON.parse(new TextDecoder().decode(bytes));}
export default {async fetch(request,env){
  if(!safeEqual(request.headers.get("x-lattice-auth-driver-service-auth"),env.AUTH_DRIVER_SERVICE_AUTH))return fail(401);
  const path=new URL(request.url).pathname;if(request.method!=="POST"||!["/validate","/token-exchange","/bind-external","/authorize-and-dispatch","/dispatch","/revoke"].includes(path))return fail(404);
  let input;try{input=await body(request);}catch{return fail();}
  if(path==="/revoke"){
    if(!opaque(input?.material_or_remote_proof)||typeof input?.driver_config?.endpoint!=="string")return fail();
    try{const endpoint=new URL(input.driver_config.endpoint);if(endpoint.protocol!=="https:"||endpoint.username||endpoint.password||endpoint.hash)throw new Error();const result=await bounded(await fetch(new Request(endpoint,{method:"POST",headers:{accept:"application/json","content-type":"application/json"},body:JSON.stringify({action:"destroy",connection_ref:input.connection_ref,material_or_remote_proof:input.material_or_remote_proof})})));if(typeof result.remote_destruction_proof!=="string")throw new Error();return Response.json({revoked:true,remote_destruction_proof:result.remote_destruction_proof});}catch{return fail(409);}
  }
  if(["/dispatch","/authorize-and-dispatch"].includes(path)){
    const profile=input?.profile,config=input?.driver_config,plan=input?.plan;if(!profile||!plan||typeof config?.endpoint!=="string")return fail();
    try{const endpoint=new URL(config.endpoint);if(endpoint.protocol!=="https:"||endpoint.username||endpoint.password||endpoint.hash)throw new Error();let upstream;
      if(path==="/authorize-and-dispatch"){if(!opaque(input.remote_handle))throw new Error();upstream=await fetch(new Request(endpoint,{method:"POST",headers:{accept:"application/json","content-type":"application/json"},body:JSON.stringify({action:"authorize_and_dispatch",remote_handle:input.remote_handle,plan})}));}
      else {const secret=new TextDecoder().decode(decode(input.credential_b64u));const kind=profile.scheme_config?.kind;const headers=new Headers(plan.headers??{});if(kind==="api_key_header")headers.set(profile.scheme_config.header_name,`${profile.scheme_config.prefix}${secret}`);else if(kind==="api_key_query")endpoint.searchParams.set(profile.scheme_config.query_name,secret);else if(kind==="generic_bearer")headers.set(profile.scheme_config.header_name,`${profile.scheme_config.prefix}${profile.scheme_config.whitespace_rule==="single_space"?" ":""}${secret}`);else if(kind==="http_basic"){const pair=JSON.parse(secret);if(pair.username.includes(":"))throw new Error();headers.set("authorization",`Basic ${btoa(`${pair.username}:${pair.password}`)}`);}else if(kind==="oauth_token_exchange_workload_oidc")headers.set("authorization",`Bearer ${secret}`);else throw new Error();upstream=await fetch(new Request(endpoint,{method:plan.method??"POST",headers,body:["GET","HEAD"].includes(plan.method)?undefined:JSON.stringify(plan.body??null)}));}
      const result=await bounded(upstream);const response=Response.json(result,{headers:{"x-request-id":upstream.headers.get("x-request-id")??"auth-driver"}});if(path==="/authorize-and-dispatch"&&typeof result.remote_dispatch_proof!=="string")throw new Error();if(typeof result.remote_dispatch_proof==="string")response.headers.set("x-lattice-remote-dispatch-proof",result.remote_dispatch_proof);return response;
    }catch{return fail(409);}
  }
  const profile=input?.profile,submission=input?.submission,claims=input?.expected_claims,config=input?.driver_config;
  if(!profile||!submission||!Array.isArray(claims)||claims.length===0||typeof config?.endpoint!=="string")return fail();
  let endpoint;try{endpoint=new URL(config.endpoint);if(endpoint.protocol!=="https:"||endpoint.username||endpoint.password||endpoint.hash)throw new Error();}catch{return fail();}
  let upstream;
  try{
    if(path==="/validate"){
      const secret=new TextDecoder().decode(decode(submission.material_b64u));const kind=profile.scheme_config?.kind;const headers=new Headers({accept:"application/json"});
      if(kind==="api_key_header")headers.set(profile.scheme_config.header_name,`${profile.scheme_config.prefix}${secret}`);
      else if(kind==="api_key_query")endpoint.searchParams.set(profile.scheme_config.query_name,secret);
      else if(kind==="generic_bearer")headers.set(profile.scheme_config.header_name,`${profile.scheme_config.prefix}${profile.scheme_config.whitespace_rule==="single_space"?" ":""}${secret}`);
      else if(kind==="http_basic"){const pair=JSON.parse(secret);if(typeof pair.username!=="string"||pair.username.includes(":")||typeof pair.password!=="string")throw new Error();headers.set("authorization",`Basic ${btoa(`${pair.username}:${pair.password}`)}`);}
      else throw new Error();
      upstream=await fetch(new Request(endpoint,{method:"GET",headers}));
    } else {
      if(path==="/token-exchange"&&(!Array.isArray(config.trusted_issuers)||!config.trusted_issuers.includes(submission.issuer)))throw new Error();
      upstream=await fetch(new Request(endpoint,{method:"POST",headers:{accept:"application/json","content-type":"application/json"},body:JSON.stringify(path==="/token-exchange"?{assertion:submission.assertion_b64u,issuer:submission.issuer,audience:submission.audience,nonce:submission.nonce,issued_at:submission.issued_at}:{challenge:submission.challenge,custodian_ref:submission.custodian_ref,remote_proof:submission.remote_proof})}));
    }
    const result=await bounded(upstream);if(typeof result.account_subject!=="string"||result.account_subject.length===0)throw new Error();
    return Response.json({material_b64u:["/bind-external","/authorize-and-dispatch"].includes(path)?null:(path==="/token-exchange"?(result.material_b64u??submission.assertion_b64u):submission.material_b64u),account_subject:result.account_subject,claims,remote_proof:["/bind-external","/authorize-and-dispatch"].includes(path)?result.remote_proof:null});
  }catch{return fail(409);}
}};
