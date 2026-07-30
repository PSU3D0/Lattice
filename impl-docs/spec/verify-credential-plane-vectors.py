#!/usr/bin/env python3
"""Verify the normative Credential Plane Protocol 0.2 golden vectors."""

import base64
from collections import Counter
import hashlib
import hmac
import json
import re
import subprocess
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCHEMA = json.loads((HERE / "credential-plane-protocol.schema.json").read_text())
VECTORS = json.loads((HERE / "credential-plane-protocol-vectors.json").read_text())
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


verify_signatures()
verify_commitments()
verify_union_fixtures()
print(f"credential-plane vectors: OK ({len(VECTORS['signed_artifact_vectors'])} signed, "
      f"{len(VECTORS['commitment_vectors'])} commitments, "
      f"{len(VECTORS['union_fixtures'])} union fixtures)")
