public_type!(ConnectionAuthorityViewV2, AuthorityViewTag, "AuthorityView");
public_type!(MaterialMetaV2, MaterialMetaTag, "MaterialMeta");
public_type!(
    ConnectionSnapshotV2,
    ConnectionSnapshotTag,
    "ConnectionSnapshot"
);
public_type!(ChannelBindingV2, ChannelBindingTag, "ChannelBinding");
public_type!(FlowSubjectV2, FlowSubjectTag, "FlowSubject");

pub fn verify_snapshot_successor(
    current: &ConnectionSnapshotV2,
    next: &ConnectionSnapshotV2,
) -> Result<(), crate::BrokerError> {
    let current = current.as_value();
    let next = next.as_value();
    let number = |value: &serde_json::Value, field: &str| {
        value
            .get(field)
            .and_then(|v| v.as_u64())
            .ok_or(crate::BrokerError::Brk109)
    };
    if number(next, "cas_version")?
        != number(current, "cas_version")?
            .checked_add(1)
            .ok_or(crate::BrokerError::Brk401)?
        || number(next, "current_material_generation")?
            < number(current, "current_material_generation")?
    {
        return Err(crate::BrokerError::Brk106);
    }
    let old_view = current.get("authority_view_hash");
    let new_view = next.get("authority_view_hash");
    let old_epoch = current
        .pointer("/authority_view/authority_epoch")
        .and_then(|v| v.as_u64())
        .ok_or(crate::BrokerError::Brk109)?;
    let new_epoch = next
        .pointer("/authority_view/authority_epoch")
        .and_then(|v| v.as_u64())
        .ok_or(crate::BrokerError::Brk109)?;
    if (old_view == new_view && old_epoch != new_epoch)
        || (old_view != new_view && new_epoch <= old_epoch)
    {
        return Err(crate::BrokerError::Brk106);
    }
    Ok(())
}

pub fn verify_fence_successor(
    current: &crate::credential::CrossVersionCredentialFenceV2,
    next: &crate::credential::CrossVersionCredentialFenceV2,
) -> Result<(), crate::BrokerError> {
    let current = current.as_value();
    let next = next.as_value();
    let u64_field = |value: &serde_json::Value, field: &str| {
        value
            .get(field)
            .and_then(|v| v.as_u64())
            .ok_or(crate::BrokerError::Brk109)
    };
    if u64_field(next, "cas_version")?
        != u64_field(current, "cas_version")?
            .checked_add(1)
            .ok_or(crate::BrokerError::Brk401)?
        || u64_field(next, "fence_generation")? < u64_field(current, "fence_generation")?
    {
        return Err(crate::BrokerError::Brk106);
    }
    for field in [
        "v2_lease_ever_issued",
        "v2_rotation_ever_started",
        "v1_leasing_disabled",
    ] {
        if current.get(field).and_then(|v| v.as_bool()) == Some(true)
            && next.get(field).and_then(|v| v.as_bool()) != Some(true)
        {
            return Err(crate::BrokerError::Brk106);
        }
    }
    let phase = |value: &serde_json::Value| match value.get("phase").and_then(|v| v.as_str()) {
        Some("v1_authoritative") => Some(0),
        Some("v2_prepared") => Some(1),
        Some("v2_authoritative") => Some(2),
        _ => None,
    };
    let old_phase = phase(current).ok_or(crate::BrokerError::Brk109)?;
    let new_phase = phase(next).ok_or(crate::BrokerError::Brk109)?;
    if new_phase < old_phase
        && !(old_phase == 1
            && new_phase == 0
            && current
                .get("v2_lease_ever_issued")
                .and_then(|v| v.as_bool())
                == Some(false)
            && current
                .get("v2_rotation_ever_started")
                .and_then(|v| v.as_bool())
                == Some(false))
    {
        return Err(crate::BrokerError::Brk106);
    }
    Ok(())
}
