#!/usr/bin/env python3
"""Verify the normative Credential Plane Protocol 0.2 golden vectors."""

import base64
import calendar
from collections import Counter
import datetime
import hashlib
import hmac
import json
import re
import subprocess
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
def strict_json_load(path):
    def no_duplicates(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise AssertionError(f"duplicate JSON member {key!r} in {path}")
            result[key] = value
        return result

    return json.loads(path.read_text(), object_pairs_hook=no_duplicates,
                      parse_constant=lambda value: (_ for _ in ()).throw(
                          AssertionError(f"non-finite JSON number {value!r} in {path}")))


SCHEMA = strict_json_load(HERE / "credential-plane-protocol.schema.json")
VECTORS = strict_json_load(HERE / "credential-plane-protocol-vectors.json")
DEFS = SCHEMA["$defs"]


def jcs(value):
    def check(v):
        if isinstance(v, float):
            raise AssertionError("vectors MUST NOT use non-integer JSON numbers")
        if isinstance(v, str) and not v.isascii():
            raise AssertionError("vector JCS subset is intentionally ASCII")
        if isinstance(v, dict):
            for key, item in v.items():
                check(key)
                check(item)
        elif isinstance(v, list):
            for item in v:
                check(item)
    check(value)
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":")).encode()


def b64u(raw):
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def b64u_decode(text):
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def schema_errors(value, schema, path="$"):
    if schema is True:
        return []
    if schema is False:
        return [f"{path}: prohibited by false schema"]
    errors = []
    if "$ref" in schema:
        name = schema["$ref"].removeprefix("#/$defs/")
        if name not in DEFS:
            return [f"{path}: unknown ref {name}"]
        errors += schema_errors(value, DEFS[name], path)
    if "const" in schema and value != schema["const"]:
        errors.append(f"{path}: expected const {schema['const']!r}")
    if "enum" in schema and value not in schema["enum"]:
        errors.append(f"{path}: not in enum")
    if "type" in schema:
        types = schema["type"] if isinstance(schema["type"], list) else [schema["type"]]
        matches = {"object": isinstance(value, dict), "array": isinstance(value, list),
                   "string": isinstance(value, str),
                   "integer": isinstance(value, int) and not isinstance(value, bool),
                   "boolean": isinstance(value, bool), "null": value is None}
        if not any(matches.get(kind, False) for kind in types):
            return errors + [f"{path}: wrong type, expected {types}"]
    if "oneOf" in schema:
        branch_results = [schema_errors(value, branch, path) for branch in schema["oneOf"]]
        if sum(not result for result in branch_results) != 1:
            errors.append(f"{path}: expected exactly one oneOf branch; results={branch_results}")
    for part in schema.get("allOf", []):
        errors += schema_errors(value, part, path)
    if "if" in schema and not schema_errors(value, schema["if"], path):
        errors += schema_errors(value, schema.get("then", {}), path)
    if "not" in schema and not schema_errors(value, schema["not"], path):
        errors.append(f"{path}: matched prohibited schema")
    if isinstance(value, dict):
        required = schema.get("required", [])
        for key in required:
            if key not in value:
                errors.append(f"{path}: missing {key}")
        props = schema.get("properties", {})
        for key, item in value.items():
            if key in props:
                errors += schema_errors(item, props[key], f"{path}/{key}")
            elif schema.get("additionalProperties") is False:
                errors.append(f"{path}: unexpected property {key}")
            elif isinstance(schema.get("additionalProperties"), dict):
                errors += schema_errors(item, schema["additionalProperties"], f"{path}/{key}")
        if len(value) < schema.get("minProperties", 0):
            errors.append(f"{path}: too few properties")
        if len(value) > schema.get("maxProperties", 1 << 30):
            errors.append(f"{path}: too many properties")
        if "propertyNames" in schema:
            for key in value:
                errors += schema_errors(key, schema["propertyNames"], f"{path}/<key>")
    if isinstance(value, list):
        if len(value) < schema.get("minItems", 0):
            errors.append(f"{path}: too few items")
        if len(value) > schema.get("maxItems", 1 << 30):
            errors.append(f"{path}: too many items")
        prefix = schema.get("prefixItems", [])
        for index, item in enumerate(value):
            item_schema = prefix[index] if index < len(prefix) else schema.get("items", {})
            errors += schema_errors(item, item_schema, f"{path}/{index}")
        if schema.get("uniqueItems") and len({jcs(item) for item in value}) != len(value):
            errors.append(f"{path}: duplicate items")
        if schema.get("x-lattice-sorted") and value != sorted(value, key=jcs):
            errors.append(f"{path}: items are not JCS sorted")
    if isinstance(value, str):
        if len(value) < schema.get("minLength", 0):
            errors.append(f"{path}: too short")
        if len(value) > schema.get("maxLength", 1 << 30):
            errors.append(f"{path}: too long")
        if "pattern" in schema and re.search(schema["pattern"], value) is None:
            errors.append(f"{path}: pattern mismatch {schema['pattern']}")
    if isinstance(value, int) and not isinstance(value, bool):
        if value < schema.get("minimum", -(1 << 63)):
            errors.append(f"{path}: below minimum")
        if value > schema.get("maximum", 1 << 63):
            errors.append(f"{path}: above maximum")
    return errors


def verify_signatures():
    keys = {item["key_id"]: item for item in VECTORS["signing_keys"]}
    expected = Counter({
        ("RegistryDefinition", "lattice.registry-definition.v0.2"): 1,
        ("RegistryDecision", "lattice.registry-decision.v0.2"): 1,
        ("DeploymentEndpointSet", "lattice.deployment-endpoint-set.v0.2"): 1,
        ("DeploymentPublicConfig", "lattice.deployment-public-config.v0.2"): 1,
        ("StandingAuthority", "lattice.standing-authority.v0.2"): 1,
        ("ContractSet", "lattice.contract-set.v0.2"): 1,
        ("PrivateMaterialSubmission", "lattice.private-material-submission.v0.2"): 1,
        ("LegacyAdmissionInventory", "lattice.legacy-admission-inventory.v0.2"): 1,
        ("LegacyInventoryDecision", "lattice.legacy-inventory-decision.v0.2"): 1,
        ("HistoricalKeyValidityEvidence", "lattice.historical-key-validity-evidence.v0.2"): 1,
        ("HistoricalKeyRevocationEvidence", "lattice.historical-key-revocation-evidence.v0.2"): 2,
        ("HistoricalVerificationKeyArchive", "lattice.historical-verification-key-archive.v0.2"): 1,
        ("BindingAttestation", "lattice.binding-attestation.v0.2"): 1,
        ("NodeLease", "lattice.node-lease.v0.2"): 1,
        ("InvocationReceipt", "lattice.invocation-receipt.v0.2"): 1,
        ("RemoteEnvelope", "lattice.remote-custody-envelope.v0.2"): 1,
    })
    actual = Counter((item["artifact_schema"], item["domain"])
                     for item in VECTORS["signed_artifact_vectors"])
    assert actual == expected
    verified = []
    for vector in VECTORS["signed_artifact_vectors"]:
        key = keys[vector["key_id"]]
        artifact = vector["artifact"]
        errors = schema_errors(artifact, DEFS[vector["artifact_schema"]])
        assert not errors, (vector["vector_id"], errors)
        assert artifact["signature"]["key_id"] == vector["key_id"]
        signature_object = artifact["signature"]
        if vector["signature_construction"] == "omit_top_level_signature":
            unsigned = dict(artifact)
            unsigned.pop("signature")
            unsigned_bytes = jcs(unsigned)
            preimage = vector["domain"].encode() + b"\0" + unsigned_bytes
            assert unsigned_bytes.hex() == vector["unsigned_jcs_hex"]
        else:
            assert vector["signature_construction"] == "remote_aad_ciphertext"
            aad_object = {key: value for key, value in artifact.items()
                          if key not in ("ciphertext", "signature", "aad_hash")}
            aad_bytes = jcs(aad_object)
            aad_digest = hashlib.sha256(aad_bytes).digest()
            ciphertext = b64u_decode(artifact["ciphertext"])
            assert aad_bytes.hex() == vector["aad_jcs_hex"]
            assert aad_digest.hex() == vector["aad_hash_hex"]
            assert artifact["aad_hash"] == "sha256:" + aad_digest.hex()
            preimage = vector["domain"].encode() + b"\0" + aad_digest + ciphertext
        signed_bytes = jcs(artifact)
        assert preimage.hex() == vector["preimage_hex"]
        assert signed_bytes.hex() == vector["signed_jcs_hex"]
        assert "sha256:" + hashlib.sha256(signed_bytes).hexdigest() == vector["signed_artifact_hash"]
        assert signature_object["value"] == vector["signature_base64url"]
        seed = bytes.fromhex(key["seed_hex"])
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            der = bytes.fromhex("302e020100300506032b657004220420") + seed
            (directory / "private.der").write_bytes(der)
            (directory / "preimage").write_bytes(preimage)
            subprocess.run(["openssl", "pkey", "-inform", "DER", "-in",
                            str(directory / "private.der"), "-out",
                            str(directory / "private.pem")], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            public_der = subprocess.check_output([
                "openssl", "pkey", "-in", str(directory / "private.pem"),
                "-pubout", "-outform", "DER"])
            assert public_der[-32:].hex() == key["public_key_hex"]
            subprocess.run(["openssl", "pkeyutl", "-sign", "-rawin", "-inkey",
                            str(directory / "private.pem"), "-in",
                            str(directory / "preimage"), "-out",
                            str(directory / "signature")], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            actual_signature = (directory / "signature").read_bytes()
            assert b64u(actual_signature) == vector["signature_base64url"]
            (directory / "public.pem").write_bytes(subprocess.check_output([
                "openssl", "pkey", "-in", str(directory / "private.pem"), "-pubout"]))
            subprocess.run(["openssl", "pkeyutl", "-verify", "-rawin", "-pubin",
                            "-inkey", str(directory / "public.pem"), "-in",
                            str(directory / "preimage"), "-sigfile",
                            str(directory / "signature")], check=True,
                           stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        verified.append(vector)
    by_schema = {}
    for vector in verified:
        by_schema.setdefault(vector["artifact_schema"], []).append(vector)
    inventory = by_schema["LegacyAdmissionInventory"][0]
    decision = by_schema["LegacyInventoryDecision"][0]["artifact"]
    assert decision["inventory_ref"] == inventory["artifact"]["inventory_ref"]
    assert decision["inventory_hash"] == inventory["signed_artifact_hash"]
    validity = by_schema["HistoricalKeyValidityEvidence"][0]["artifact"]
    archive = by_schema["HistoricalVerificationKeyArchive"][0]["artifact"]
    assert archive["validity_evidence"] == validity
    assert archive["revocation_evidence"] in [
        item["artifact"] for item in by_schema["HistoricalKeyRevocationEvidence"]]
    for field in ("issuer", "key_id", "algorithm", "public_key_encoding",
                  "public_key_base64url", "valid_from", "valid_until"):
        assert archive[field] == validity[field]


def u32(number):
    return number.to_bytes(4, "big")


def u64(number):
    return number.to_bytes(8, "big")


def verify_commitments():
    for vector in VECTORS["commitment_vectors"]:
        root = bytes.fromhex(vector["root_key_hex"])
        org = bytes.fromhex(vector["org_id_bytes_hex"])
        field = bytes.fromhex(vector["field_name_utf8_hex"])
        context = bytes.fromhex(vector["context_jcs_hex"])
        value = bytes.fromhex(vector["value_bytes_hex"])
        assert field.decode() == vector["field_name"]
        assert jcs(vector["context"]) == context
        if vector["value_encoding"] == "jcs":
            assert jcs(vector["value"]) == value
        else:
            assert vector["value_encoding"] == "opaque_bytes"
            assert bytes.fromhex(vector["value_hex"]) == value
        derive = (b"lattice.commitment-opening.v0.1\0" + u32(len(org)) + org +
                  u32(len(field)) + field + u32(len(context)) + context)
        scoped = hmac.new(root, derive, hashlib.sha256).digest()
        commitment_preimage = (b"lattice.commitment.v0.1\0" + u32(len(org)) + org +
                               u32(len(field)) + field + u32(len(context)) + context +
                               u64(len(value)) + value)
        result = hmac.new(scoped, commitment_preimage, hashlib.sha256).digest()
        assert derive.hex() == vector["derivation_preimage_hex"]
        assert scoped.hex() == vector["scoped_key_hex"]
        assert commitment_preimage.hex() == vector["commitment_preimage_hex"]
        assert result.hex() == vector["commitment_hex"]


def pointer_get(document, pointer):
    value = document
    if pointer == "/":
        return value
    for token in pointer.removeprefix("/").split("/"):
        token = token.replace("~1", "/").replace("~0", "~")
        value = value[int(token)] if isinstance(value, list) else value[token]
    return value


def discover_unions(value, pointer=""):
    found = []
    if isinstance(value, dict):
        if "oneOf" in value:
            found.append((pointer or "/", len(value["oneOf"])))
        for key, item in value.items():
            escaped = key.replace("~", "~0").replace("/", "~1")
            found.extend(discover_unions(item, pointer + "/" + escaped))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found.extend(discover_unions(item, pointer + "/" + str(index)))
    return found


def verify_union_fixtures():
    claims = VECTORS["union_fixture_claims"]
    fixtures = VECTORS["union_fixtures"]
    discovered = discover_unions(SCHEMA)
    claimed = [(item["schema_pointer"], item["branch_count"]) for item in claims]
    assert claimed == discovered
    expected = {(pointer, index) for pointer, count in discovered for index in range(count)}
    actual = {(item["schema_pointer"], item["branch_index"]) for item in fixtures}
    assert actual == expected
    assert len(actual) == len(fixtures)
    for item in fixtures:
        union = pointer_get(SCHEMA, item["schema_pointer"])
        branch = union["oneOf"][item["branch_index"]]
        instance = item["instance"]
        errors = schema_errors(instance, branch)
        assert not errors, (item["schema_pointer"], item["branch_index"], errors)
        matching = [i for i, candidate in enumerate(union["oneOf"])
                    if not schema_errors(instance, candidate)]
        assert matching == [item["branch_index"]], (item["schema_pointer"], matching)
        encoded = jcs(instance)
        assert encoded.hex() == item["jcs_hex"]
        assert "sha256:" + hashlib.sha256(encoded).hexdigest() == item["sha256"]


def ed25519_verify(public_key, preimage, signature):
    with tempfile.TemporaryDirectory() as directory:
        directory = Path(directory)
        public_der = bytes.fromhex("302a300506032b6570032100") + public_key
        (directory / "public.der").write_bytes(public_der)
        (directory / "preimage").write_bytes(preimage)
        (directory / "signature").write_bytes(signature)
        result = subprocess.run([
            "openssl", "pkeyutl", "-verify", "-rawin", "-pubin",
            "-keyform", "DER", "-inkey", str(directory / "public.der"),
            "-in", str(directory / "preimage"), "-sigfile",
            str(directory / "signature"),
        ], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return result.returncode == 0


def ed25519_material_from_seed(seed, preimage=None):
    with tempfile.TemporaryDirectory() as directory:
        directory = Path(directory)
        private_der = bytes.fromhex("302e020100300506032b657004220420") + seed
        (directory / "private.der").write_bytes(private_der)
        subprocess.run(["openssl", "pkey", "-inform", "DER", "-in",
                        str(directory / "private.der"), "-out",
                        str(directory / "private.pem")], check=True,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        public_der = subprocess.check_output([
            "openssl", "pkey", "-in", str(directory / "private.pem"),
            "-pubout", "-outform", "DER"])
        signature = None
        if preimage is not None:
            (directory / "preimage").write_bytes(preimage)
            subprocess.run([
                "openssl", "pkeyutl", "-sign", "-rawin", "-inkey",
                str(directory / "private.pem"), "-in", str(directory / "preimage"),
                "-out", str(directory / "signature")], check=True,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            signature = (directory / "signature").read_bytes()
        return public_der[-32:], signature


def audit_schema_keywords(schema):
    allowed = {
        "$schema", "$id", "$comment", "$defs", "$ref", "title", "description",
        "type", "const", "enum", "oneOf", "allOf", "if", "then", "not",
        "required", "properties", "additionalProperties", "propertyNames",
        "minProperties", "maxProperties", "items", "prefixItems", "minItems",
        "maxItems", "uniqueItems", "x-lattice-sorted", "minLength", "maxLength",
        "pattern", "minimum", "maximum",
    }

    def walk(value, path="$", mapping=False):
        if isinstance(value, list):
            for index, item in enumerate(value):
                walk(item, f"{path}/{index}")
            return
        if not isinstance(value, dict):
            return
        if mapping:
            for key, item in value.items():
                walk(item, f"{path}/{key}")
            return
        unknown = set(value) - allowed
        assert not unknown, (path, "unsupported schema keywords", sorted(unknown))
        for key, item in value.items():
            walk(item, f"{path}/{key}", key in {"properties", "$defs"})

    walk(schema)


def decode_length_delimited_context(encoded):
    offset = 0
    fields = []
    while offset < len(encoded):
        assert offset + 4 <= len(encoded)
        name_length = int.from_bytes(encoded[offset:offset + 4], "big")
        offset += 4
        assert 1 <= name_length <= 64 and offset + name_length <= len(encoded)
        name = encoded[offset:offset + name_length]
        offset += name_length
        assert offset + 8 <= len(encoded)
        value_length = int.from_bytes(encoded[offset:offset + 8], "big")
        offset += 8
        assert value_length <= 4096 and offset + value_length <= len(encoded)
        value = encoded[offset:offset + value_length]
        offset += value_length
        fields.append((name.decode("ascii"), value))
    assert offset == len(encoded)
    return fields


def lifecycle_relationship_errors(artifacts, vector_hashes, context):
    errors = set()
    observation = artifacts["AuthorizationObservation"]
    grant = artifacts["ProviderGrantVersion"]
    adoption = artifacts["ProviderGrantAdoptionRecord"]
    binding = artifacts["CorrectedBindingAttestation"]
    admission = artifacts["DispatchAdmission"]
    receipt = artifacts["InvocationReceipt"]
    standing = artifacts["StandingAuthority"]
    contracts = artifacts["ContractSet"]
    policy = artifacts["PolicyInstance"]
    registry_definition = artifacts["RegistryDefinition"]
    registry_decision = artifacts["RegistryDecision"]
    registry = artifacts["RegistryDecisionVector"]
    acceptance = grant["provider_grant_acceptance"]

    if registry_decision["registry_definition_ref"] != registry_definition["registry_definition_ref"] or \
            registry_decision["definition_hash"] != registry_definition["definition_hash"] or \
            registry_decision["registry_definition_artifact_hash"] != vector_hashes["RegistryDefinition"]:
        errors.add("registry_definition_decision_mismatch")
    if registry_decision["tenant_id"] != registry_definition["tenant_id"] or \
            registry_decision["deployment_id"] != registry_definition["deployment_id"]:
        errors.add("registry_definition_decision_tenant_deployment_mismatch")
    if len(registry["entries"]) != 1:
        errors.add("registry_decision_vector_unbacked_entry")
    for entry in registry["entries"]:
        if entry["entry_ref"] != registry_definition["registry_definition_ref"] or \
                entry["definition_hash"] != registry_decision["definition_hash"] or \
                entry["decision_ref"] != registry_decision["registry_decision_ref"] or \
                entry["decision_hash"] != vector_hashes["RegistryDecision"] or \
                entry["decision_epoch"] != registry_decision["decision_epoch"]:
            errors.add("registry_decision_vector_mismatch")
    if registry["tenant_id"] != registry_decision["tenant_id"] or \
            registry["deployment_id"] != registry_decision["deployment_id"]:
        errors.add("registry_decision_vector_tenant_deployment_mismatch")

    shared_observation = [
        "tenant_id", "provider", "auth_profile_ref", "auth_profile_version",
        "oauth_client_audience_commitment", "account_subject_commitment",
    ]
    if any(grant[field] != observation[field] for field in shared_observation):
        errors.add("observation_grant_account_mismatch")
    if grant["source_observation_ref"] != observation["observation_ref"] or \
            grant["source_observation_hash"] != vector_hashes["AuthorizationObservation"]:
        errors.add("grant_source_observation_mismatch")
    nested_outer = [
        "provider", "auth_profile_ref", "auth_profile_version",
        "oauth_client_audience_commitment", "account_subject_commitment",
        "provider_grant_lineage_ref", "accepted_claims_profile_ref",
        "accepted_claims_commitment",
    ]
    if acceptance["accepted_claims_commitment"] != grant["accepted_claims_commitment"]:
        errors.add("grant_nested_accepted_claims_mismatch")
    if any(acceptance[field] != grant[field] for field in nested_outer):
        errors.add("grant_nested_authority_partition_mismatch")
    if acceptance["observed_claims_commitment"] != observation["normalized_claims_commitment"]:
        errors.add("grant_observed_claims_mismatch")
    if grant["provider"] == "google" and acceptance["relation"] != "exact":
        errors.add("google_provider_grant_acceptance_not_exact")

    if adoption["provider_grant_version_ref"] != grant["provider_grant_version_ref"]:
        errors.add("adoption_grant_ref_mismatch")
    if adoption["provider_grant_version_hash"] != vector_hashes["ProviderGrantVersion"]:
        errors.add("adoption_grant_hash_mismatch")
    adoption_fields = ["tenant_id", "provider_grant_lineage_ref",
                       "account_subject_commitment", "custody_ref",
                       "source_observation_ref", "source_observation_hash",
                       "provider_authority_epoch"]
    if any(adoption[field] != grant[field] for field in adoption_fields):
        errors.add("adoption_grant_authority_mismatch")

    if binding["provider_grant_version_ref"] != grant["provider_grant_version_ref"]:
        errors.add("binding_grant_ref_mismatch")
    if binding["provider_grant_version_hash"] != vector_hashes["ProviderGrantVersion"]:
        errors.add("binding_grant_hash_mismatch")
    if binding["provider_grant_lineage_ref"] != grant["provider_grant_lineage_ref"]:
        errors.add("binding_grant_lineage_mismatch")
    if binding["account_subject_commitment"] != grant["account_subject_commitment"]:
        errors.add("binding_grant_account_mismatch")
    if binding["connection_budget_partition"]["provider_grant_lineage_ref"] != grant["provider_grant_lineage_ref"]:
        errors.add("binding_connection_budget_lineage_mismatch")
    account_partition = binding["account_budget_partition"]
    for field in ("provider", "auth_profile_ref", "auth_profile_version",
                  "account_subject_commitment"):
        if account_partition[field] != grant[field]:
            errors.add("binding_account_budget_partition_mismatch")
    heads = [
        ("StandingAuthority", "standing_authority_ref", "standing_authority_hash", standing),
        ("ContractSet", "contract_set_ref", "contract_set_hash", contracts),
        ("PolicyInstance", "policy_instance_ref", "policy_instance_hash", policy),
        ("RegistryDecisionVector", "registry_vector_ref", "registry_vector_hash", registry),
    ]
    for class_name, ref_field, hash_field, target in heads:
        target_ref = next(value for key, value in target.items() if key == ref_field)
        if binding[ref_field] != target_ref or binding[hash_field] != vector_hashes[class_name]:
            errors.add("binding_current_head_mismatch")
        if target["tenant_id"] != binding["tenant_id"] or target["deployment_id"] != binding["deployment_id"]:
            errors.add("binding_head_tenant_deployment_mismatch")
    if binding["registry_vector_epoch"] != registry["vector_epoch"]:
        errors.add("binding_registry_vector_epoch_mismatch")
    acl = context["acl_record"]
    acl_canonical = {key: value for key, value in acl.items()
                     if key not in {"record_commitment", "record_hash"}}
    if acl["record_hash"] != "sha256:" + hashlib.sha256(jcs(acl_canonical)).hexdigest():
        errors.add("acl_record_hash_mismatch")
    acl_pairs = {
        "tenant_id": "tenant_id", "deployment_id": "deployment_id",
        "actor_subject_commitment": "actor_subject_commitment",
        "provider_grant_version_ref": "provider_grant_version_ref",
        "account_subject_commitment": "account_subject_commitment",
        "acl_epoch": "acl_epoch", "acl_selector_hash": "selector_hash",
        "acl_record_commitment": "record_commitment", "acl_record_hash": "record_hash",
    }
    for artifact_field, acl_field in acl_pairs.items():
        if binding[artifact_field] != acl[acl_field]:
            errors.add("binding_acl_selector_mismatch" if artifact_field == "acl_selector_hash"
                       else "binding_acl_record_mismatch")

    if admission["tenant_id"] != binding["tenant_id"] or admission["deployment_id"] != binding["deployment_id"]:
        errors.add("admission_binding_tenant_deployment_mismatch")
    if not any(entry["contract_id"] == admission["contract_id"] and
               entry["contract_hash"] == admission["contract_hash"]
               for entry in contracts["contracts"]):
        errors.add("admission_contract_not_in_contract_set")
    if admission["binding_ref"] != binding["binding_ref"]:
        errors.add("admission_binding_ref_mismatch")
    if admission["binding_hash"] != vector_hashes["CorrectedBindingAttestation"]:
        errors.add("admission_binding_hash_mismatch")
    admission_grant_fields = ["provider_grant_version_ref", "provider_grant_lineage_ref",
                              "account_subject_commitment"]
    if any(admission[field] != grant[field] for field in admission_grant_fields) or \
            admission["provider_grant_version_hash"] != vector_hashes["ProviderGrantVersion"]:
        errors.add("admission_provider_grant_mismatch")
    for field in ("actor_subject_commitment", "acl_epoch", "acl_selector_hash",
                  "acl_record_commitment", "acl_record_hash"):
        if admission[field] != binding[field]:
            errors.add("admission_acl_mismatch")
    if admission["connection_budget_partition"] != binding["connection_budget_partition"]:
        errors.add("admission_connection_budget_lineage_mismatch")
    if admission["account_budget_partition"] != binding["account_budget_partition"]:
        errors.add("admission_account_budget_partition_mismatch")
    if admission["registry_vector_ref"] != registry["registry_vector_ref"] or \
            admission["registry_vector_hash"] != vector_hashes["RegistryDecisionVector"] or \
            admission["registry_vector_epoch"] != registry["vector_epoch"]:
        errors.add("admission_registry_vector_mismatch")
    connection_partition = next((item for item in admission["run_budget_snapshot"]["connection_partitions"]
                                 if item["provider_grant_lineage_ref"] == grant["provider_grant_lineage_ref"]), None)
    if connection_partition is None:
        errors.add("admission_run_ledger_connection_partition_missing")
    account_ledger_key = (grant["provider"], grant["auth_profile_ref"],
                          grant["auth_profile_version"], grant["account_subject_commitment"])
    account_partition_entry = next((item for item in admission["run_budget_snapshot"]["account_partitions"]
                                    if (item["provider"], item["auth_profile_ref"],
                                        item["auth_profile_version"], item["account_subject_commitment"])
                                    == account_ledger_key), None)
    if account_partition_entry is None:
        errors.add("admission_run_ledger_account_partition_missing")

    receipt_admission_fields = ["tenant_id", "deployment_id", "run_id",
                                "logical_effect_id", "attempt",
                                "provider_grant_version_ref", "provider_grant_lineage_ref",
                                "account_subject_commitment", "actor_subject_commitment",
                                "acl_epoch", "acl_selector_hash", "acl_record_commitment",
                                "acl_record_hash", "binding_ref", "canonical_input_commitment",
                                "registry_vector_ref", "registry_vector_hash", "registry_vector_epoch"]
    if any(receipt[field] != admission[field] for field in receipt_admission_fields):
        errors.add("receipt_account_mismatch" if receipt["account_subject_commitment"] != admission["account_subject_commitment"]
                   else "receipt_admission_fields_mismatch")
    if receipt["dispatch_admission_ref"] != admission["admission_ref"] or \
            receipt["dispatch_admission_hash"] != vector_hashes["DispatchAdmission"]:
        errors.add("receipt_admission_hash_mismatch")
    if receipt["provider_grant_version_hash"] != vector_hashes["ProviderGrantVersion"]:
        errors.add("receipt_provider_grant_hash_mismatch")
    receipt_heads = {
        "standing_authority_hash": "StandingAuthority",
        "contract_set_hash": "ContractSet", "policy_instance_hash": "PolicyInstance",
        "registry_vector_hash": "RegistryDecisionVector",
    }
    if any(receipt[field] != vector_hashes[class_name]
           for field, class_name in receipt_heads.items()):
        errors.add("receipt_current_head_mismatch")

    inventory = artifacts["LegacyAttemptInventory"]
    snapshot = context["legacy_staging_snapshot"]
    if snapshot["snapshot_hash"] != "sha256:" + hashlib.sha256(jcs(snapshot["snapshot_record"])).hexdigest():
        errors.add("legacy_snapshot_record_hash_mismatch")
    if inventory["source_attempt_count"] != len(inventory["entries"]):
        errors.add("legacy_source_count_mismatch")
    if inventory["accepted_staging_snapshot_ref"] != snapshot["snapshot_ref"] or \
            inventory["accepted_staging_snapshot_hash"] != snapshot["snapshot_hash"]:
        errors.add("legacy_snapshot_mismatch")
    expected_sources = {(item["source_kind"], item["source_record_ref"], item["source_record_hash"])
                        for item in snapshot["sources"]}
    actual_sources = {(item["source_kind"], item["source_record_ref"], item["source_record_hash"])
                      for item in inventory["entries"]}
    if actual_sources != expected_sources:
        errors.add("legacy_source_record_mismatch")
    return errors


def parse_protocol_timestamp(value):
    match = re.fullmatch(
        r"([0-9]{4})-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])"
        r"T([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])"
        r"(?:\.([0-9]{1,9}))?Z", value)
    assert match is not None, ("non-canonical protocol timestamp", value)
    year, month, day, hour, minute, second = map(int, match.groups()[:6])
    instant = datetime.datetime(year, month, day, hour, minute, second,
                                tzinfo=datetime.timezone.utc)
    seconds = calendar.timegm(instant.utctimetuple())
    fraction = match.group(7) or ""
    nanoseconds = int(fraction.ljust(9, "0")) if fraction else 0
    return seconds, nanoseconds


def classify_historical_receipt(vector, keyset, compromise, public_key):
    artifact = vector["artifact"]
    if vector["domain"] != "lattice.invocation-receipt.v0.2":
        return "invalid"
    if artifact["issuer"] != vector["issuer"] or artifact["broker_key_id"] != vector["key_id"] or \
            artifact["signature"]["key_id"] != vector["key_id"] or artifact["issued_at"] != vector["signing_time"]:
        return "invalid"
    key = next((item for item in keyset["keys"] if item["key_id"] == vector["key_id"]), None)
    if key is None or keyset["receipt_issuer"] != vector["issuer"] or key["algorithm"] != "Ed25519":
        return "invalid"
    if b64u_decode(key["public_key_base64url"]) != public_key:
        return "invalid"
    issued_at = parse_protocol_timestamp(vector["signing_time"])
    valid_from = parse_protocol_timestamp(key["valid_from"])
    valid_until = parse_protocol_timestamp(key["valid_until"])
    if not (valid_from <= issued_at <= valid_until):
        return "invalid"
    unsigned = dict(artifact)
    signature_object = unsigned.pop("signature")
    preimage = vector["domain"].encode("ascii") + b"\0" + jcs(unsigned)
    if preimage.hex() != vector["preimage_hex"] or signature_object["value"] != vector["signature_base64url"] or \
            not ed25519_verify(public_key, preimage, b64u_decode(signature_object["value"])):
        return "invalid"
    if compromise["receipt_issuer"] != vector["issuer"] or \
            compromise["affected_key_id"] != vector["key_id"] or \
            compromise["keyset_ref"] != keyset["keyset_ref"]:
        return "historically_ambiguous"
    revoked_at = parse_protocol_timestamp(compromise["revoked_at"])
    affected_start = parse_protocol_timestamp(compromise["affected_interval_start"])
    affected_end = parse_protocol_timestamp(compromise["affected_interval_end"])
    if issued_at >= revoked_at:
        return "invalid"
    if affected_start <= issued_at <= affected_end:
        return "historically_ambiguous"
    return "valid"


def verify_lifecycle_separated():
    audit_schema_keywords(SCHEMA)
    packet = VECTORS["lifecycle_separated_1"]
    assert packet["authority_model_revision"] == "lifecycle-separated-1"
    expected_suffixes = {
        "AuthorityModelCutover": "authority-model-cutover",
        "AuthorizationObservation": "authorization-observation",
        "ProviderGrantVersion": "provider-grant-version",
        "ProviderGrantAdoptionRecord": "provider-grant-adoption",
        "ConnectionAliasRecord": "connection-alias-record",
        "StandingAuthority": "standing-authority",
        "ContractSet": "contract-set",
        "PolicyInstance": "policy-instance",
        "RegistryDefinition": "registry-definition",
        "RegistryDecision": "registry-decision",
        "RegistryDecisionVector": "registry-decision-vector",
        "CeilingAmendment": "ceiling-amendment",
        "CorrectedBindingAttestation": "binding-attestation",
        "DispatchAdmission": "dispatch-admission",
        "InvocationReceipt": "invocation-receipt",
        "LegacyAttemptInventory": "legacy-attempt-inventory",
        "ReceiptVerificationKeyset": "receipt-verification-keyset",
        "ReceiptKeyCompromiseRecord": "receipt-key-compromise",
    }
    prefix = "lattice.credential-plane.0.2.lifecycle-separated-1."
    expected_domains = {name: prefix + suffix
                        for name, suffix in expected_suffixes.items()}
    assert packet["class_domain_map"] == expected_domains
    assert len(set(expected_domains.values())) == len(expected_domains)

    key = packet["signing_key"]
    seed = bytes.fromhex(key["seed_hex"])
    assert len(seed) == 32
    public_key = bytes.fromhex(key["public_key_hex"])
    assert len(public_key) == 32
    derived_public_key, _ = ed25519_material_from_seed(seed)
    assert derived_public_key == public_key
    vectors = packet["signed_artifact_vectors"]
    assert Counter(item["artifact_class"] for item in vectors) == Counter(expected_domains.keys())
    assert Counter(item["domain"] for item in vectors) == Counter(expected_domains.values())
    positives = {}
    for vector in vectors:
        artifact_class = vector["artifact_class"]
        artifact = vector["artifact"]
        schema_name = "LS1" + artifact_class
        assert vector["artifact_schema"] == schema_name
        assert artifact["artifact_type"] == artifact_class
        assert artifact["schema_version"] == "0.2"
        assert artifact["authority_model_revision"] == "lifecycle-separated-1"
        assert artifact["critical_fields"] == sorted(artifact["critical_fields"])
        assert len(set(artifact["critical_fields"])) == len(artifact["critical_fields"])
        assert "/authority_model_revision" in artifact["critical_fields"]
        for pointer in artifact["critical_fields"]:
            pointer_get(artifact, pointer)
        errors = schema_errors(artifact, DEFS[schema_name])
        assert not errors, (vector["vector_id"], errors)
        root_matches = [index for index, branch in enumerate(SCHEMA["oneOf"])
                        if not schema_errors(artifact, branch)]
        assert len(root_matches) == 1, (vector["vector_id"], root_matches)
        unsigned = dict(artifact)
        signature_object = unsigned.pop("signature")
        unsigned_jcs = jcs(unsigned)
        domain = expected_domains[artifact_class]
        preimage = domain.encode("ascii") + b"\0" + unsigned_jcs
        signed_jcs = jcs(artifact)
        signature = b64u_decode(signature_object["value"])
        assert signature_object["algorithm"] == "Ed25519"
        assert artifact["key_id"] == signature_object["key_id"] == key["key_id"] == vector["key_id"]
        assert vector["domain"] == domain
        assert unsigned_jcs.hex() == vector["unsigned_jcs_hex"]
        assert preimage.hex() == vector["preimage_hex"]
        assert signature_object["value"] == vector["signature_base64url"]
        assert signed_jcs.hex() == vector["signed_jcs_hex"]
        assert vector["signed_artifact_hash"] == (
            "sha256:" + hashlib.sha256(signed_jcs).hexdigest())
        resigned_public_key, deterministic_signature = ed25519_material_from_seed(seed, preimage)
        assert resigned_public_key == public_key
        assert deterministic_signature == signature
        assert ed25519_verify(public_key, preimage, signature)
        positives[artifact_class] = artifact

    vector_hashes = {item["artifact_class"]: item["signed_artifact_hash"] for item in vectors}
    relationship_errors = lifecycle_relationship_errors(
        positives, vector_hashes, packet["relationship_context"])
    assert not relationship_errors, sorted(relationship_errors)

    provider_grant = positives["ProviderGrantVersion"]
    if provider_grant["provider"] == "google":
        assert provider_grant["provider_grant_acceptance"]["relation"] == "exact"
    assert provider_grant["operation_claim_coverage"] == "subset_or_equal"
    acceptance = provider_grant["provider_grant_acceptance"]
    for field in ("provider", "auth_profile_ref", "auth_profile_version",
                  "oauth_client_audience_commitment", "account_subject_commitment",
                  "provider_grant_lineage_ref", "accepted_claims_profile_ref",
                  "accepted_claims_commitment"):
        assert acceptance[field] == provider_grant[field]
    binding = positives["CorrectedBindingAttestation"]
    assert "connection_alias" not in binding
    assert "material_generation" not in binding
    assert binding["connection_budget_partition"]["provider_grant_lineage_ref"] == (
        binding["provider_grant_lineage_ref"])
    assert binding["account_budget_partition"]["account_subject_commitment"] == (
        binding["account_subject_commitment"])
    admission = positives["DispatchAdmission"]
    assert admission["authority_owner"] == "admission_authority"
    assert admission["one_use"] is True
    receipt = positives["InvocationReceipt"]
    assert receipt["terminal_state"].startswith("terminal_")
    inventory = positives["LegacyAttemptInventory"]
    assert inventory["authority_owner"] == "admission_authority"
    assert inventory["completeness"] == "complete" and inventory["sealed"] is True
    assert inventory["source_attempt_count"] == len(inventory["entries"])
    assert all(not item["dispatch_enabled"] for item in inventory["entries"])
    keyset = positives["ReceiptVerificationKeyset"]
    assert keyset["compromise_classifications"] == [
        "historically_ambiguous", "invalid", "valid"]
    compromise = positives["ReceiptKeyCompromiseRecord"]
    assert compromise["affected_or_unknown_classification"] == "historically_ambiguous"

    internal_names = [
        "AuthenticatedActorContext", "ActorConnectionAcl", "MaterialState",
        "CustodyReservation", "CustodyCancellationEvidence",
        "CrossingAuthorization", "CrossingEvidence", "RunLedger",
    ]
    for name in internal_names:
        definition = DEFS["LS1" + name]
        assert definition["additionalProperties"] is False
        properties = definition["properties"]
        assert properties["portable"]["const"] is False
        assert properties["dispatch_authority"]["const"] is False
        assert properties["authority_model_revision"]["const"] == "lifecycle-separated-1"
        critical = properties["critical_fields"]["const"]
        assert critical == sorted(critical) and len(critical) == len(set(critical))
        assert "/authority_model_revision" in critical
    crossing = DEFS["LS1CrossingAuthorization"]["properties"]
    assert crossing["authority_owner"]["const"] == "admission_authority"
    assert crossing["attempt_state"]["const"] == "crossing_risk_started"
    assert crossing["one_use"]["const"] is True
    run_ledger = DEFS["LS1RunLedger"]["properties"]
    assert run_ledger["connection_partitions"]["x-lattice-sorted"] is True
    assert run_ledger["account_partitions"]["x-lattice-sorted"] is True
    for name in ("LS1ConnectionLedgerPartition", "LS1AccountLedgerPartition"):
        fields = DEFS[name]["properties"]
        assert {"ceiling", "consumed", "reserved", "ceiling_amendment_head_hash"} <= set(fields)
    evidence_keys = {"reservation_hash", "dispatch_admission_commitment",
                     "custody_reservation_commitment", "cancellation_evidence_hash",
                     "crossing_authorization_commitment", "crossing_evidence_hash",
                     "terminal_evidence_hash"}
    expected_phase_evidence = [
        set(), {"reservation_hash"},
        {"reservation_hash", "dispatch_admission_commitment"},
        {"reservation_hash", "dispatch_admission_commitment", "custody_reservation_commitment"},
        {"reservation_hash", "dispatch_admission_commitment", "custody_reservation_commitment",
         "crossing_authorization_commitment"},
        {"reservation_hash", "dispatch_admission_commitment", "custody_reservation_commitment",
         "crossing_authorization_commitment", "crossing_evidence_hash"},
        {"reservation_hash", "dispatch_admission_commitment", "custody_reservation_commitment",
         "cancellation_evidence_hash", "terminal_evidence_hash"},
        {"reservation_hash", "dispatch_admission_commitment", "custody_reservation_commitment",
         "crossing_authorization_commitment", "terminal_evidence_hash"},
        {"reservation_hash", "dispatch_admission_commitment", "custody_reservation_commitment",
         "crossing_authorization_commitment", "crossing_evidence_hash", "terminal_evidence_hash"},
    ]
    for name in ("LS1EffectState", "LS1AttemptState"):
        branches = DEFS[name]["oneOf"]
        assert len(branches) == 9
        for index, branch in enumerate(branches):
            assert branch["additionalProperties"] is False
            fields = branch["properties"]
            assert {"tenant_id", "deployment_id", "run_id", "logical_effect_id",
                    "canonical_input_commitment"} <= set(fields)
            critical = fields["critical_fields"]["const"]
            assert critical == sorted(critical) and "/authority_model_revision" in critical
            assert set(fields) & evidence_keys == expected_phase_evidence[index]
        assert branches[6]["properties"]["terminal_boundary"]["const"] == "pre_crossing_cancelled"
        assert branches[7]["properties"]["terminal_boundary"]["const"] == "crossing_risk"
        assert branches[8]["properties"]["terminal_boundary"]["const"] == "provider_observed"

    required_contexts = [
        "account-subject", "normalized-claims", "oauth-client-audience",
        "provider-grant-ref", "actor-subject", "dispatch-admission",
        "canonical-input", "provider-response", "alias-intent",
    ]
    commitment_vectors = packet["commitment_vectors"]
    assert [item["context"].removeprefix(prefix + "commitment.")
            for item in commitment_vectors] == required_contexts
    commitments = set()
    private_values = set()
    required_fields = [
        "tenant_id", "issuer", "provider", "auth_profile_ref",
        "oauth_client_id", "audience", "artifact_ref", "purpose",
    ]
    for vector in commitment_vectors:
        context = vector["context"].encode("ascii")
        framed = bytes.fromhex(vector["framed_context_hex"])
        decoded = decode_length_delimited_context(framed)
        manifest_fields = [(item["name"], item["value_utf8"].encode())
                           for item in vector["fields"]]
        assert decoded == manifest_fields
        assert [name for name, _ in decoded] == required_fields
        assert decoded[-1][1].decode() == vector["context"].rsplit(".", 1)[-1]
        value = bytes.fromhex(vector["private_value_hex"])
        opening = (b"lattice.commitment-opening.v0.1\0" + u32(len(context)) +
                   context + u64(len(framed)) + framed)
        scoped = hmac.new(bytes(range(32)), opening, hashlib.sha256).digest()
        preimage = (b"lattice.commitment.v0.1\0" + u32(len(context)) + context +
                    u64(len(framed)) + framed + u64(len(value)) + value)
        result = hmac.new(scoped, preimage, hashlib.sha256).digest()
        assert opening.hex() == vector["opening_preimage_hex"]
        assert scoped.hex() == vector["scoped_key_hex"]
        assert preimage.hex() == vector["commitment_preimage_hex"]
        assert vector["commitment"] == "hmac-sha256:" + result.hex()
        commitments.add(vector["commitment"])
        private_values.add(value)
    assert len(private_values) == 1
    assert len(commitments) == len(commitment_vectors)

    negatives = packet["negative_vectors"]
    required_negative_ids = {
        "old-domain-valid-signature-rejected-corrected", "missing-critical-revision",
        "unknown-critical-revision", "revision-omitted-from-critical",
        "unknown-direct-member", "unsorted-critical-fields-resigned",
        "duplicate-critical-fields-resigned", "tampered-lineage",
        "tampered-account-commitment", "tampered-acl-epoch",
        "tampered-registry-vector", "tampered-admission-commitment",
        "tampered-material-generation", "binding-alias-injection",
        "binding-material-generation-injection", "unsorted-contracts-resigned",
        "unsorted-registry-vector-resigned", "unsorted-keyset-resigned",
        "unsorted-ledger-connections-resigned", "unsorted-ledger-accounts-resigned",
        "google-nonexact-acceptance-resigned", "grant-nested-claims-mismatch-resigned",
        "grant-nested-client-audience-mismatch-resigned",
        "observation-grant-account-mismatch-resigned",
        "adoption-grant-ref-mismatch-resigned", "binding-grant-hash-mismatch-resigned",
        "binding-lineage-budget-mismatch-resigned",
        "binding-account-budget-mismatch-resigned",
        "binding-acl-selector-mismatch-resigned",
        "binding-acl-record-hash-mismatch-resigned",
        "admission-binding-hash-mismatch-resigned",
        "admission-lineage-budget-mismatch-resigned",
        "admission-account-budget-mismatch-resigned",
        "receipt-admission-hash-mismatch-resigned", "receipt-account-mismatch-resigned",
        "legacy-count-mismatch-resigned", "legacy-snapshot-mismatch-resigned",
        "legacy-source-mismatch-resigned", "standing-cross-deployment-resigned",
        "contract-cross-deployment-resigned", "policy-cross-deployment-resigned",
        "registry-vector-cross-deployment-resigned",
        "registry-definition-decision-mismatch-resigned",
        "registry-decision-vector-mismatch-resigned",
        "admission-contract-out-of-set-resigned",
        "top-level-signature-key-id-mismatch-resigned",
        "effect-proposed-with-admission", "effect-reserved-with-custody",
        "effect-admitted-with-crossing", "effect-custody-with-terminal",
        "attempt-proposed-with-custody", "attempt-admitted-with-terminal",
        "attempt-crossing-with-terminal",
        "effect-pre-crossing-with-crossing-auth",
        "attempt-pre-crossing-missing-cancellation",
        "effect-crossing-ambiguous-with-crossing-evidence",
        "attempt-crossing-ambiguous-missing-authorization",
        "effect-provider-terminal-missing-crossing-evidence",
        "attempt-provider-terminal-with-cancellation",
    }
    assert {item["negative_id"] for item in negatives} == required_negative_ids
    for item in negatives:
        artifact = item["artifact"]
        errors = schema_errors(artifact, DEFS[item["artifact_schema"]])
        if item["rejection_stage"] == "internal_phase":
            assert errors, (item["negative_id"], "phase-impossible record accepted")
            branches = DEFS[item["artifact_schema"]]["oneOf"]
            assert not any(not schema_errors(artifact, branch) for branch in branches)
            continue
        unsigned = dict(artifact)
        signature = b64u_decode(unsigned.pop("signature")["value"])
        preimage = item["domain"].encode("ascii") + b"\0" + jcs(unsigned)
        if item["re_signed"]:
            resigned_key, resigned_signature = ed25519_material_from_seed(seed, preimage)
            assert resigned_key == public_key and resigned_signature == signature
            assert ed25519_verify(public_key, preimage, signature)
        if item["rejection_stage"] == "schema":
            assert errors, (item["negative_id"], "schema accepted")
            continue
        assert not errors, (item["negative_id"], errors)
        if item["rejection_stage"] == "domain_confusion":
            assert item["domain"] != item["mandatory_domain"] == expected_domains[item["artifact_class"]]
            assert ed25519_verify(public_key, preimage, signature)
            corrected_preimage = item["mandatory_domain"].encode("ascii") + b"\0" + jcs(unsigned)
            assert not ed25519_verify(public_key, corrected_preimage, signature)
            continue
        if item["rejection_stage"] == "semantic_identity":
            assert artifact["key_id"] != artifact["signature"]["key_id"]
            assert item["expected_rejection"] == "top_level_signature_key_id_mismatch"
            continue
        if item["rejection_stage"] == "relationship":
            mutated = dict(positives)
            mutated[item["artifact_class"]] = artifact
            mutated_hashes = dict(vector_hashes)
            mutated_hashes[item["artifact_class"]] = (
                "sha256:" + hashlib.sha256(jcs(artifact)).hexdigest())
            relationship_rejections = lifecycle_relationship_errors(
                mutated, mutated_hashes, packet["relationship_context"])
            assert item["expected_rejection"] in relationship_rejections, (
                item["negative_id"], sorted(relationship_rejections))
            continue
        assert item["rejection_stage"] == "signature"
        assert not ed25519_verify(public_key, preimage, signature), item["negative_id"]

    historical_vectors = packet["historical_receipt_classification_vectors"]
    assert [item["expected_classification"] for item in historical_vectors] == [
        "valid", "historically_ambiguous", "invalid", "valid",
        "historically_ambiguous", "valid", "invalid"]
    fractional_widths = [len(item["signing_time"].split(".", 1)[1][:-1])
                         for item in historical_vectors if "." in item["signing_time"]]
    assert fractional_widths == [1, 2, 9, 4]
    for historical in historical_vectors:
        assert historical["artifact_schema"] == "LS1HistoricalInvocationReceiptVerificationOnly"
        assert not schema_errors(historical["artifact"], DEFS[historical["artifact_schema"]])
        assert historical["dispatch_authority"] is False
        assert historical["permitted_use"] == "historical_verification_only"
        assert "authority_model_revision" not in historical["artifact"]
        assert historical["domain"] not in expected_domains.values()
        classification = classify_historical_receipt(
            historical, keyset, compromise, public_key)
        assert classification == historical["expected_classification"]
        assert historical["dispatch_authority"] is False

    forbidden_public_members = {
        "access_token", "refresh_token", "raw_subject", "raw_claims",
        "provider_body", "canonical_input_value", "sealed_material",
    }
    for artifact in positives.values():
        assert forbidden_public_members.isdisjoint(artifact)

    return (len(vectors), len(commitment_vectors), len(negatives),
            len(internal_names) + 2, len(historical_vectors))


verify_signatures()
verify_commitments()
verify_union_fixtures()
ls_signed, ls_commitments, ls_negatives, ls_internal, ls_historical = verify_lifecycle_separated()
print(f"credential-plane vectors: OK ({len(VECTORS['signed_artifact_vectors'])} pre-fix signed, "
      f"{len(VECTORS['commitment_vectors'])} pre-fix commitments, "
      f"{len(VECTORS['union_fixtures'])} union fixtures; "
      f"lifecycle-separated-1: {ls_signed} signed, {ls_commitments} commitments, "
      f"{ls_negatives} negatives, {ls_internal} internal records, "
      f"{ls_historical} historical classifications)")
