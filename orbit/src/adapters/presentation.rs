// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use proto::{ListClustersUnitResponse, ListJobsUnitResponse};

pub(crate) fn cluster_host_string(item: &ListClustersUnitResponse) -> String {
    match item.host {
        Some(ref v) => match v {
            proto::list_clusters_unit_response::Host::Hostname(s) => s.to_string(),
            proto::list_clusters_unit_response::Host::Ipaddr(s) => s.to_string(),
        },
        None => "<unknown>".to_string(),
    }
}

pub(crate) fn job_status(item: &ListJobsUnitResponse) -> &'static str {
    if !item.is_completed {
        if item
            .scheduler_state
            .as_deref()
            .map(|state| state.eq_ignore_ascii_case("PENDING"))
            .unwrap_or(false)
        {
            return "queued";
        }
        return "running";
    }
    match item.terminal_state.as_deref() {
        Some(state) if state.eq_ignore_ascii_case("COMPLETED") => "completed",
        Some(state)
            if state.eq_ignore_ascii_case("CANCELED")
                || state.eq_ignore_ascii_case("CANCELLED") =>
        {
            "canceled"
        }
        Some(_) => "failed",
        None => "completed",
    }
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
            finished_at: Some("2024-01-01T01:00:00Z".to_string()),
            is_completed: completed,
            terminal_state: terminal_state.map(|s| s.to_string()),
            scheduler_state: scheduler_state.map(|s| s.to_string()),
            local_path: "/tmp/project".to_string(),
            remote_path: "/remote/project".to_string(),
            project_name: None,
            default_retrieve_path: None,
        }
    }

    #[test]
    fn cluster_host_string_handles_hostname_ip_none() {
        let hostname = sample_cluster(Some(proto::list_clusters_unit_response::Host::Hostname(
            "node".to_string(),
        )));
        let ip = sample_cluster(Some(proto::list_clusters_unit_response::Host::Ipaddr(
            "10.0.0.1".to_string(),
        )));
        let none = sample_cluster(None);

        assert_eq!(cluster_host_string(&hostname), "node");
        assert_eq!(cluster_host_string(&ip), "10.0.0.1");
        assert_eq!(cluster_host_string(&none), "<unknown>");
    }

    #[test]
    fn job_status_reports_running_and_terminal_states() {
        let running = sample_job(false, None, None);
        let queued = sample_job(false, None, Some("PENDING"));
        let completed = sample_job(true, Some("COMPLETED"), None);
        let failed = sample_job(true, Some("FAILED"), None);
        let canceled = sample_job(true, Some("CANCELED"), None);
        let completed_unknown = sample_job(true, None, None);

        assert_eq!(job_status(&running), "running");
        assert_eq!(job_status(&queued), "queued");
        assert_eq!(job_status(&completed), "completed");
        assert_eq!(job_status(&failed), "failed");
        assert_eq!(job_status(&canceled), "canceled");
        assert_eq!(job_status(&completed_unknown), "completed");
    }
}
