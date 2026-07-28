use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use broker_core::{
    credential::{SchemaType, parse, receipt::InvocationReceiptV2},
    signing::BrokerVerifyingKey,
};
use capabilities::http::{
    HttpError, HttpMethod, HttpRead, HttpRequest, HttpResponse, HttpResult, HttpWrite,
};
use ed25519_dalek::{Signer as _, SigningKey};
use futures::{StreamExt, lock::Mutex};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use worker::send::IntoSendFuture;
use worker::{Env, Fetcher, Headers, Method, Request, RequestInit, Result};

#[derive(Clone)]
struct ServiceFetcher(Fetcher);
unsafe impl Send for ServiceFetcher {}
unsafe impl Sync for ServiceFetcher {}

#[derive(Clone)]
struct Config {
    deployment_key: String,
    service_auth: String,
    binding_ref: String,
    bundle_id: String,
    flow_ir_hash: String,
    binding_lock_hash: String,
    flow_id: String,
    nodes: BTreeMap<&'static str, String>,
}

struct State {
    session_ref: Option<String>,
    counter: u64,
}

pub struct WorkersBrokerTransport {
    service: ServiceFetcher,
    signing: SigningKey,
    receipt_key: BrokerVerifyingKey,
    config: Config,
    run_id: String,
    state: Mutex<State>,
}

#[derive(Deserialize)]
struct SessionResponse {
    session_ref: String,
}

impl WorkersBrokerTransport {
    pub fn from_env(env: &Env) -> Result<Arc<Self>> {
        let secret = |name: &str| {
            env.secret(name).map(|v| v.to_string()).map_err(|_| {
                worker::Error::RustError(format!("required broker secret `{name}` is absent"))
            })
        };
        let var = |name: &str| {
            env.var(name).map(|v| v.to_string()).map_err(|_| {
                worker::Error::RustError(format!("required broker variable `{name}` is absent"))
            })
        };
        let seed = URL_SAFE_NO_PAD
            .decode(secret("LATTICE_BROKER_POP_SEED_B64U")?)
            .map_err(|_| worker::Error::RustError("invalid broker PoP seed".into()))?;
        let seed: [u8; 32] = seed
            .try_into()
            .map_err(|_| worker::Error::RustError("invalid broker PoP seed".into()))?;
        let receipt_key_bytes = URL_SAFE_NO_PAD
            .decode(var("LATTICE_BROKER_RECEIPT_PUBLIC_KEY_B64U")?)
            .map_err(|_| worker::Error::RustError("invalid pinned receipt key".into()))?;
        let receipt_key_bytes: [u8; 32] = receipt_key_bytes
            .try_into()
            .map_err(|_| worker::Error::RustError("invalid pinned receipt key".into()))?;
        let receipt_key_hash = var("LATTICE_BROKER_RECEIPT_PUBLIC_KEY_HASH")?;
        if receipt_key_hash != format!("sha256:{}", hex(Sha256::digest(receipt_key_bytes))) {
            return Err(worker::Error::RustError(
                "pinned receipt key hash mismatch".into(),
            ));
        }
        let receipt_key = BrokerVerifyingKey::from_bytes("broker-v2-receipt", receipt_key_bytes)
            .map_err(|_| worker::Error::RustError("invalid pinned receipt key".into()))?;
        let now = worker::js_sys::Date::now() as u64;
        let run_id = format!("s30-run-{:032x}", now);
        Ok(Arc::new(Self {
            service: ServiceFetcher(env.service("LATTICE_BROKER_PRIVATE")?),
            signing: SigningKey::from_bytes(&seed),
            receipt_key,
            config: Config {
                deployment_key: secret("LATTICE_BROKER_DEPLOYMENT_KEY")?,
                service_auth: secret("LATTICE_BROKER_SERVICE_AUTH")?,
                binding_ref: var("LATTICE_BROKER_BINDING_REF")?,
                bundle_id: var("LATTICE_BROKER_BUNDLE_ID")?,
                flow_ir_hash: var("LATTICE_BROKER_FLOW_IR_HASH")?,
                binding_lock_hash: var("LATTICE_BROKER_BINDING_LOCK_HASH")?,
                flow_id: var("LATTICE_BROKER_FLOW_ID")?,
                nodes: {
                    let bundle = s30_google_micro::bundle();
                    let mut nodes = BTreeMap::new();
                    for alias in ["create", "append", "notify"] {
                        let id = bundle
                            .validated_ir
                            .flow()
                            .nodes
                            .iter()
                            .find(|node| node.alias == alias)
                            .map(|node| node.id.0.clone())
                            .ok_or_else(|| {
                                worker::Error::RustError(format!(
                                    "S30 broker node `{alias}` is absent"
                                ))
                            })?;
                        nodes.insert(alias, id);
                    }
                    nodes
                },
            },
            run_id,
            state: Mutex::new(State {
                session_ref: None,
                counter: 0,
            }),
        }))
    }

    async fn fetch_json(
        &self,
        path: &str,
        body: &Value,
        session: Option<&str>,
        state: &mut State,
    ) -> HttpResult<Value> {
        let bytes =
            serde_json::to_vec(body).map_err(|_| invalid("broker request serialization failed"))?;
        let headers = Headers::new();
        headers
            .set("content-type", "application/json")
            .map_err(|_| invalid("broker header failed"))?;
        headers
            .set("x-lattice-service-auth", &self.config.service_auth)
            .map_err(|_| invalid("broker header failed"))?;
        if let Some(session_ref) = session {
            state.counter = state.counter.saturating_add(1);
            let timestamp = now_seconds();
            let jti = format!("s30-pop-{:020}-{:020}", timestamp, state.counter);
            let body_hash = format!("sha256:{}", hex(Sha256::digest(&bytes)));
            let transcript = framed(
                b"lattice.authenticated-request.ed25519.v1",
                &[
                    session_ref.as_bytes(),
                    b"lattice-broker",
                    b"POST",
                    path.as_bytes(),
                    body_hash.as_bytes(),
                    timestamp.to_string().as_bytes(),
                    jti.as_bytes(),
                ],
            );
            headers
                .set("authorization", &format!("Session {session_ref}"))
                .map_err(|_| invalid("broker header failed"))?;
            headers
                .set("x-lattice-pop-jti", &jti)
                .map_err(|_| invalid("broker header failed"))?;
            headers
                .set("x-lattice-pop-timestamp", &timestamp.to_string())
                .map_err(|_| invalid("broker header failed"))?;
            headers
                .set(
                    "x-lattice-pop-signature",
                    &URL_SAFE_NO_PAD.encode(self.signing.sign(&transcript).to_bytes()),
                )
                .map_err(|_| invalid("broker header failed"))?;
        }
        let mut init = RequestInit::new();
        init.with_method(Method::Post)
            .with_headers(headers)
            .with_body(Some(
                worker::js_sys::Uint8Array::from(bytes.as_slice()).into(),
            ));
        let request = Request::new_with_init(&format!("http://broker{path}"), &init)
            .map_err(|_| invalid("broker request failed"))?;
        let response = self
            .service
            .0
            .fetch_request(request)
            .into_send()
            .await
            .map_err(|_| invalid("broker unavailable"))?;
        let status = response.status().as_u16();
        let response_bytes = bounded_response(response).await?;
        if !(200..300).contains(&status) {
            return Err(invalid("broker rejected the exact effect"));
        }
        serde_json::from_slice(&response_bytes).map_err(|_| invalid("broker response was invalid"))
    }

    async fn ensure_session(&self, state: &mut State) -> HttpResult<String> {
        if let Some(value) = &state.session_ref {
            return Ok(value.clone());
        }
        let public = URL_SAFE_NO_PAD.encode(self.signing.verifying_key().to_bytes());
        state.counter = state.counter.saturating_add(1);
        let timestamp = now_seconds();
        let nonce = format!("s30-exchange-{:020}-{:020}", timestamp, state.counter);
        let deployment_key_id = format!(
            "sha256:{}",
            hex(Sha256::digest(
                [
                    b"deployment-key-id\0".as_slice(),
                    self.config.deployment_key.as_bytes()
                ]
                .concat()
            ))
        );
        let transcript = framed(
            b"lattice.session-exchange.ed25519.v1",
            &[
                deployment_key_id.as_bytes(),
                public.as_bytes(),
                nonce.as_bytes(),
                timestamp.to_string().as_bytes(),
                b"lattice-broker-session",
            ],
        );
        let body = json!({"deployment_key":self.config.deployment_key,"client_public_key":public,"client_nonce":nonce,"timestamp":timestamp,"audience":"lattice-broker-session","signature":URL_SAFE_NO_PAD.encode(self.signing.sign(&transcript).to_bytes())});
        let value = self
            .fetch_json("/v0.2/sessions", &body, None, state)
            .await?;
        let response: SessionResponse = serde_json::from_value(value)
            .map_err(|_| invalid("broker session response invalid"))?;
        state.session_ref = Some(response.session_ref.clone());
        Ok(response.session_ref)
    }

    async fn invoke(
        &self,
        alias: &'static str,
        contract: &str,
        slot: &str,
        input: Value,
    ) -> HttpResult<Value> {
        let mut state = self.state.lock().await;
        let session = self.ensure_session(&mut state).await?;
        let common = json!({
            "session_ref":session,"binding_ref":self.config.binding_ref,"bundle_id":self.config.bundle_id,
            "flow_ir_hash":self.config.flow_ir_hash,"binding_lock_hash":self.config.binding_lock_hash,
            "flow_id":self.config.flow_id,"run_id":self.run_id,"node_id":self.config.nodes[alias],
            "node_alias":alias,"activation_ordinal":1
        });
        let mut lease_body = common.clone();
        lease_body["operation_contract"] = Value::String(contract.into());
        let lease = self
            .fetch_json(
                "/internal/v0.2/node-leases",
                &lease_body,
                Some(&session),
                &mut state,
            )
            .await?;
        let mut grant_body = common;
        grant_body["node_lease_ref"] = lease["node_lease_ref"].clone();
        grant_body["semantic_effect_slot"] = Value::String(slot.into());
        grant_body["expected_cas_version"] = Value::from(0);
        grant_body["input"] = input.clone();
        let grant = self
            .fetch_json(
                "/internal/v0.2/grants",
                &grant_body,
                Some(&session),
                &mut state,
            )
            .await?;
        let result = self
            .fetch_json(
                "/internal/v0.2/invoke",
                &json!({"session_ref":session,"grant_ref":grant["grant_ref"],"input":input}),
                Some(&session),
                &mut state,
            )
            .await?;
        self.verify_receipt(&result["receipt"], &grant["grant"], &mut state)
            .await?;
        Ok(result["response"].clone())
    }

    async fn verify_receipt(
        &self,
        receipt: &Value,
        expected_grant: &Value,
        _state: &mut State,
    ) -> HttpResult<()> {
        let canonical_grant = broker_core::canonical::from_serde(
            expected_grant,
            broker_core::canonical::MAX_OPERATION_BYTES,
        )
        .map_err(|_| invalid("grant response invalid"))?;
        let expected_grant_hash =
            format!("sha256:{}", hex(Sha256::digest(canonical_grant.as_bytes())));
        if receipt.get("grant_hash").and_then(Value::as_str) != Some(expected_grant_hash.as_str())
            || receipt.get("outcome").and_then(Value::as_str) != Some("confirmed")
            || receipt
                .pointer("/claims/provider_dispatch_observed")
                .and_then(Value::as_bool)
                != Some(true)
        {
            return Err(invalid("receipt did not prove the exact effect"));
        }
        let canonical = broker_core::canonical::from_serde(receipt, InvocationReceiptV2::MAX_BYTES)
            .map_err(|_| invalid("receipt invalid"))?;
        let parsed = parse::<InvocationReceiptV2>(canonical.as_bytes())
            .map_err(|_| invalid("receipt invalid"))?;
        broker_core::credential::signing::verify_signed(&parsed, &self.receipt_key)
            .map_err(|_| invalid("receipt signature invalid"))
    }
}

#[async_trait]
impl HttpRead for WorkersBrokerTransport {
    async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
        if request.method == HttpMethod::Get && request.url.contains("sheets.googleapis.com") {
            return json_http(json!({"values":[["note"]]}));
        }
        Err(invalid("broker transport denies unbrokered reads"))
    }
}

#[async_trait]
impl HttpWrite for WorkersBrokerTransport {
    async fn send(&self, request: HttpRequest) -> HttpResult<HttpResponse> {
        if request.url.ends_with("/v4/spreadsheets") {
            let body: Value = serde_json::from_slice(request.body.as_deref().unwrap_or_default())
                .map_err(|_| invalid("Sheets create request invalid"))?;
            let title = body
                .pointer("/properties/title")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("Sheets title invalid"))?;
            let sheet = body
                .pointer("/sheets/0/properties/title")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("Sheets initial sheet invalid"))?;
            let headers = body
                .pointer("/sheets/0/data/0/rowData/0/values")
                .and_then(Value::as_array)
                .ok_or_else(|| invalid("Sheets headers invalid"))?
                .iter()
                .map(|value| {
                    value
                        .pointer("/userEnteredValue/stringValue")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                        .ok_or_else(|| invalid("Sheets header invalid"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let output = self.invoke(
                "create", "connector.google.sheets.create_spreadsheet@1", "create_spreadsheet",
                json!({"title":title,"locale":null,"time_zone":null,"initial_sheet_title":sheet,"header_row":headers})
            ).await?;
            return json_http(json!({
                "spreadsheetId":output["spreadsheet_id"],
                "spreadsheetUrl":output["spreadsheet_url"],
                "properties":{"title":title},
                "sheets":[{"properties":{"sheetId":0,"title":sheet}}]
            }));
        }
        if request.url.contains("sheets.googleapis.com") {
            let body: Value = serde_json::from_slice(request.body.as_deref().unwrap_or_default())
                .map_err(|_| invalid("Sheets append request invalid"))?;
            let note = body
                .pointer("/values/0/0")
                .cloned()
                .ok_or_else(|| invalid("Sheets note value invalid"))?;
            let path = request
                .url
                .split("/spreadsheets/")
                .nth(1)
                .ok_or_else(|| invalid("Sheets URL invalid"))?;
            let spreadsheet_id = percent_decode(path.split('/').next().unwrap_or_default())?;
            let range = percent_decode(
                path.split("/values/")
                    .nth(1)
                    .and_then(|value| value.split(":append").next())
                    .unwrap_or_default(),
            )?;
            let sheet = range
                .split('!')
                .next()
                .unwrap_or_default()
                .trim_matches('\'')
                .replace("''", "'");
            let output = self.invoke(
                "append", "connector.google.sheets.append_row@1", "append_row",
                json!({"spreadsheet_id":spreadsheet_id,"sheet":sheet,"row":{"note":note},"header_row":1,"value_input_option":"raw"})
            ).await?;
            return json_http(json!({"updates":{
                "updatedRange":output["updated_range"],"updatedRows":output["updated_rows"],
                "updatedColumns":output["updated_columns"],"updatedCells":output["updated_cells"]
            }}));
        }
        if request.url.contains("/gmail/v1/users/me/messages/send") {
            let body: Value = serde_json::from_slice(request.body.as_deref().unwrap_or_default())
                .map_err(|_| invalid("Gmail request invalid"))?;
            let raw = URL_SAFE_NO_PAD
                .decode(body["raw"].as_str().unwrap_or_default())
                .map_err(|_| invalid("Gmail message invalid"))?;
            let message = String::from_utf8(raw).map_err(|_| invalid("Gmail message invalid"))?;
            let (headers, text_body) = message
                .split_once("\r\n\r\n")
                .or_else(|| message.split_once("\n\n"))
                .ok_or_else(|| invalid("Gmail message invalid"))?;
            let field = |name: &str| {
                headers
                    .lines()
                    .find_map(|line| line.strip_prefix(&format!("{name}: ")))
                    .unwrap_or_default()
                    .trim_end_matches('\r')
                    .to_string()
            };
            let output = self
                .invoke(
                    "notify",
                    "connector.google.gmail.send_message@1",
                    "send_message",
                    json!({"to":field("To"),"subject":field("Subject"),"text_body":text_body}),
                )
                .await?;
            return json_http(
                json!({"id":output["id"],"threadId":output["thread_id"],"labelIds":[]}),
            );
        }
        Err(invalid("broker transport denies arbitrary writes"))
    }
}

fn now_seconds() -> i64 {
    (worker::js_sys::Date::now() / 1000.0).floor() as i64
}
fn framed(domain: &[u8], fields: &[&[u8]]) -> Vec<u8> {
    let mut out = domain.to_vec();
    out.push(0);
    for f in fields {
        out.extend_from_slice(&(f.len() as u32).to_be_bytes());
        out.extend_from_slice(f);
    }
    out
}
fn percent_decode(value: &str) -> HttpResult<String> {
    let bytes = value.as_bytes();
    let mut output = Vec::with_capacity(bytes.len());
    let mut index = 0;
    while index < bytes.len() {
        if bytes[index] == b'%' {
            if index + 2 >= bytes.len() {
                return Err(invalid("broker URL encoding invalid"));
            }
            let pair = std::str::from_utf8(&bytes[index + 1..index + 3])
                .map_err(|_| invalid("broker URL encoding invalid"))?;
            output.push(
                u8::from_str_radix(pair, 16).map_err(|_| invalid("broker URL encoding invalid"))?,
            );
            index += 3;
        } else {
            output.push(bytes[index]);
            index += 1;
        }
    }
    String::from_utf8(output).map_err(|_| invalid("broker URL encoding invalid"))
}

fn hex(bytes: impl AsRef<[u8]>) -> String {
    bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
}
fn invalid(message: &str) -> HttpError {
    HttpError::InvalidResponse(message.into())
}
fn json_http(value: Value) -> HttpResult<HttpResponse> {
    let mut headers = capabilities::http::HttpHeaders::default();
    headers.insert("content-type", "application/json");
    Ok(HttpResponse {
        status: 200,
        headers,
        body: serde_json::to_vec(&value).map_err(|_| invalid("response failed"))?,
    })
}
async fn bounded_response(response: worker::HttpResponse) -> HttpResult<Vec<u8>> {
    let mut body = Vec::new();
    let mut stream = response.into_body();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| invalid("broker response unavailable"))?;
        if body.len() + chunk.len() > 256 * 1024 {
            return Err(invalid("broker response too large"));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}
