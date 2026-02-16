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
    out.insert("identity_path".into(), json!(item.identity_path.as_deref()));
    out.insert(
        "accounting_available".into(),
        json!(item.accounting_available),
    );
    out.insert(
        "default_base_path".into(),
        json!(item.default_base_path.as_deref()),
    );
    out.insert("is_default".into(), json!(item.is_default));
    Value::Object(out)
}

pub(super) fn job_to_json(item: &ListJobsUnitResponse) -> serde_json::Value {
    let status = job_status(item);
    json!({
        "job_id": item.job_id,
        "local_path": item.local_path.as_str(),
        "remote_path": item.remote_path.as_str(),
        "name": item.name.as_str(),
        "blueprint_name": item.blueprint_name.as_deref(),
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
            is_default: false,
            default_scratch_directory: None,
        }
    }

    fn sample_job(
        completed: bool,
        terminal_state: Option<&str>,
        scheduler_state: Option<&str>,
    ) -> ListJobsUnitResponse {
        ListJobsUnitResponse {
            name: "cluster-a".to_string(),
            job_id: 42,
            scheduler_id: Some(99),
            created_at: "2024-01-01T00:00:00Z".to_string(),
            finished_at: if completed {
                Some("2024-01-01T01:00:00Z".to_string())
            } else {
                None
            },
            is_completed: completed,
            terminal_state: terminal_state.map(|s| s.to_string()),
            scheduler_state: scheduler_state.map(|s| s.to_string()),
            local_path: "/tmp/project".to_string(),
            remote_path: "/remote/project".to_string(),
            blueprint_name: Some("project-a".to_string()),
            default_retrieve_path: Some("/tmp/retrieve".to_string()),
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
        assert_eq!(json["is_default"], false);
    }

    #[test]
    fn cluster_to_json_omits_reachability_when_disabled() {
        let cluster = sample_cluster(Some(proto::list_clusters_unit_response::Host::Hostname(
            "node".to_string(),
        )));
        let json = cluster_to_json(&cluster, false);
        assert!(json.get("reachable").is_none());
    }

    #[test]
    fn cluster_to_json_reports_disconnected_and_optional_fields() {
        let mut cluster = sample_cluster(None);
        cluster.connected = false;
        cluster.reachable = false;
        cluster.identity_path = None;
        cluster.default_base_path = Some("/cluster/base".to_string());
        cluster.accounting_available = true;

        let json = cluster_to_json(&cluster, true);
        assert_eq!(json["status"], "disconnected");
        assert_eq!(json["connected"], false);
        assert_eq!(json["reachable"], false);
        assert_eq!(json["address"], "<unknown>");
        assert!(json["identity_path"].is_null());
        assert_eq!(json["default_base_path"], "/cluster/base");
        assert_eq!(json["accounting_available"], true);
    }

    #[test]
    fn job_to_json_maps_queued_job_fields() {
        let job = sample_job(false, None, Some("PENDING"));
        let json = job_to_json(&job);

        assert_eq!(json["job_id"].as_i64(), Some(42));
        assert_eq!(json["local_path"], "/tmp/project");
        assert_eq!(json["remote_path"], "/remote/project");
        assert_eq!(json["blueprint_name"], "project-a");
        assert_eq!(json["default_retrieve_path"], "/tmp/retrieve");
        assert_eq!(json["status"], "queued");
        assert_eq!(json["is_completed"], false);
        assert!(json["finished_at"].is_null());
        assert!(json["terminal_state"].is_null());
        assert_eq!(json["scheduler_state"], "PENDING");
        assert_eq!(json["scheduler_id"].as_i64(), Some(99));
    }

    #[test]
    fn job_to_json_maps_completed_job_optional_fields() {
        let mut job = sample_job(true, Some("COMPLETED"), None);
        job.blueprint_name = None;
        job.default_retrieve_path = None;
        job.scheduler_id = None;

        let json = job_to_json(&job);

        assert_eq!(json["status"], "completed");
        assert_eq!(json["terminal_state"], "COMPLETED");
        assert!(json["scheduler_state"].is_null());
        assert!(json["blueprint_name"].is_null());
        assert!(json["default_retrieve_path"].is_null());
        assert_eq!(json["finished_at"], "2024-01-01T01:00:00Z");
        assert!(json["scheduler_id"].is_null());
    }
}
