// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use proto::{ListClustersUnitResponse, ListJobsUnitResponse};
use serde_json::{Value, json};

use crate::adapters::presentation::{cluster_host_string, job_status};

pub(super) fn cluster_to_json(
    item: &ListClustersUnitResponse,
    include_reachability: bool,
) -> serde_json::Value {
    let status = match item.connected {
        true => "connected",
        false => "disconnected",
    };
    let mut out = serde_json::Map::new();
    out.insert("name".into(), json!(item.name.as_str()));
    out.insert("username".into(), json!(item.username.as_str()));
    out.insert("address".into(), json!(cluster_host_string(item)));
    out.insert("port".into(), json!(item.port));
    out.insert("connected".into(), json!(item.connected));
    if include_reachability {
        out.insert("reachable".into(), json!(item.reachable));
    }
    out.insert("status".into(), json!(status));
    out.insert(
        "identity_path".into(),
        json!(item.identity_path.as_deref()),
    );
    out.insert(
        "accounting_available".into(),
        json!(item.accounting_available),
    );
    out.insert(
        "default_base_path".into(),
        json!(item.default_base_path.as_deref()),
    );
    Value::Object(out)
}

pub(super) fn job_to_json(item: &ListJobsUnitResponse) -> serde_json::Value {
    let status = job_status(item);
    json!({
        "job_id": item.job_id,
        "local_path": item.local_path.as_str(),
        "remote_path": item.remote_path.as_str(),
        "name": item.name.as_str(),
        "project_name": item.project_name.as_deref(),
        "default_retrieve_path": item.default_retrieve_path.as_deref(),
        "status": status,
        "is_completed": item.is_completed,
        "terminal_state": item.terminal_state.as_deref(),
        "scheduler_state": item.scheduler_state.as_deref(),
        "created_at": item.created_at.as_str(),
        "finished_at": item.finished_at.as_deref(),
        "scheduler_id": item.scheduler_id,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_cluster(
        host: Option<proto::list_clusters_unit_response::Host>,
    ) -> ListClustersUnitResponse {
        ListClustersUnitResponse {
            username: "alice".to_string(),
            identity_path: Some("~/.ssh/id_ed25519".to_string()),
            port: 22,
            host,
            connected: true,
            reachable: true,
            name: "cluster-a".to_string(),
            accounting_available: false,
            default_base_path: None,
        }
    }

    #[test]
    fn cluster_to_json_includes_status_fields() {
        let cluster = sample_cluster(Some(proto::list_clusters_unit_response::Host::Hostname(
            "node".to_string(),
        )));
        let json = cluster_to_json(&cluster, true);
        assert_eq!(json["status"], "connected");
        assert_eq!(json["name"], "cluster-a");
        assert_eq!(json["address"], "node");
    }

    #[test]
    fn cluster_to_json_omits_reachability_when_disabled() {
        let cluster = sample_cluster(Some(proto::list_clusters_unit_response::Host::Hostname(
            "node".to_string(),
        )));
        let json = cluster_to_json(&cluster, false);
        assert!(json.get("reachable").is_none());
    }
}
