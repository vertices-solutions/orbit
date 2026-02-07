// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use crate::app::errors::{AppError, AppErrorKind, codes};
use crate::app::types::{
    ClusterStatus, JobRecord, ProjectRecord, SyncFilterAction, SyncFilterRule,
};
use proto::{
    ListClustersUnitResponse, ListJobsUnitResponse, SubmitPathFilterAction, SubmitPathFilterRule,
    list_clusters_unit_response,
};

pub fn build_sync_filters(
    filters: Vec<SubmitPathFilterRule>,
) -> Result<Vec<SyncFilterRule>, AppError> {
    let mut out = Vec::with_capacity(filters.len());
    for rule in filters {
        let action = match SubmitPathFilterAction::try_from(rule.action) {
            Ok(SubmitPathFilterAction::Include) => SyncFilterAction::Include,
            Ok(SubmitPathFilterAction::Exclude) => SyncFilterAction::Exclude,
            _ => {
                return Err(AppError::new(
                    AppErrorKind::InvalidArgument,
                    codes::INVALID_ARGUMENT,
                ));
            }
        };
        if rule.pattern.trim().is_empty() {
            return Err(AppError::new(
                AppErrorKind::InvalidArgument,
                codes::INVALID_ARGUMENT,
            ));
        }
        out.push(SyncFilterRule {
            action,
            pattern: rule.pattern,
        });
    }
    Ok(out)
}

pub fn cluster_status_to_response(status: &ClusterStatus) -> ListClustersUnitResponse {
    let host = &status.host;
    ListClustersUnitResponse {
        username: host.username.clone(),
        identity_path: host.identity_path.to_owned(),
        host: match host.address {
            crate::app::types::Address::Ip(ref ip) => {
                Some(list_clusters_unit_response::Host::Ipaddr(ip.to_string()))
            }
            crate::app::types::Address::Hostname(ref hostname) => Some(
                list_clusters_unit_response::Host::Hostname(hostname.to_owned()),
            ),
        },
        port: host.port as i32,
        connected: status.connected,
        name: host.name.to_owned(),
        accounting_available: host.accounting_available,
        default_base_path: host.default_base_path.to_owned(),
        reachable: status.reachable,
    }
}

pub fn job_record_to_response(jr: &JobRecord) -> ListJobsUnitResponse {
    ListJobsUnitResponse {
        name: jr.name.clone(),
        job_id: jr.id,
        scheduler_id: jr.scheduler_id,
        created_at: jr.created_at.clone(),
        finished_at: jr.finished_at.clone(),
        is_completed: jr.is_completed,
        terminal_state: jr.terminal_state.clone(),
        scheduler_state: jr.scheduler_state.clone(),
        local_path: jr.local_path.clone(),
        remote_path: jr.remote_path.clone(),
        project_name: jr.project_name.clone(),
        default_retrieve_path: jr.default_retrieve_path.clone(),
    }
}

pub fn project_record_to_response(project: &ProjectRecord) -> proto::ProjectRecord {
    proto::ProjectRecord {
        name: project.name.clone(),
        path: project.path.clone(),
        created_at: project.created_at.clone(),
        updated_at: project.updated_at.clone(),
        version_tag: project.version_tag.clone(),
        tarball_hash: project.tarball_hash.clone(),
        tool_version: project.tool_version.clone(),
        template_config_json: project.template_config_json.clone(),
        submit_sbatch_script: project.submit_sbatch_script.clone(),
        sbatch_scripts: project.sbatch_scripts.clone(),
        default_retrieve_path: project.default_retrieve_path.clone(),
        sync_include: project.sync_include.clone(),
        sync_exclude: project.sync_exclude.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::types::{Address, Distro, HostRecord, SlurmVersion};
    use std::net::{IpAddr, Ipv4Addr};

    #[test]
    fn build_sync_filters_accepts_include_and_exclude_rules() {
        let filters = vec![
            SubmitPathFilterRule {
                action: SubmitPathFilterAction::Include as i32,
                pattern: "src/**".to_string(),
            },
            SubmitPathFilterRule {
                action: SubmitPathFilterAction::Exclude as i32,
                pattern: "target/**".to_string(),
            },
        ];

        let result = build_sync_filters(filters).expect("rules should be accepted");
        assert_eq!(
            result,
            vec![
                SyncFilterRule {
                    action: SyncFilterAction::Include,
                    pattern: "src/**".to_string(),
                },
                SyncFilterRule {
                    action: SyncFilterAction::Exclude,
                    pattern: "target/**".to_string(),
                },
            ]
        );
    }

    #[test]
    fn build_sync_filters_rejects_unknown_action() {
        let filters = vec![SubmitPathFilterRule {
            action: SubmitPathFilterAction::Unspecified as i32,
            pattern: "src/**".to_string(),
        }];

        let err = build_sync_filters(filters).expect_err("unspecified action should fail");
        assert_eq!(err.kind(), AppErrorKind::InvalidArgument);
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn build_sync_filters_rejects_blank_pattern() {
        let filters = vec![SubmitPathFilterRule {
            action: SubmitPathFilterAction::Include as i32,
            pattern: "   ".to_string(),
        }];

        let err = build_sync_filters(filters).expect_err("blank pattern should fail");
        assert_eq!(err.kind(), AppErrorKind::InvalidArgument);
        assert_eq!(err.code(), codes::INVALID_ARGUMENT);
    }

    #[test]
    fn cluster_status_to_response_maps_ip_host() {
        let status = ClusterStatus {
            host: host_record(Address::Ip(IpAddr::V4(Ipv4Addr::new(10, 20, 30, 40)))),
            connected: true,
            reachable: false,
        };

        let response = cluster_status_to_response(&status);
        assert_eq!(response.username, "alice");
        assert_eq!(response.port, 2222);
        assert_eq!(response.name, "cluster-a");
        assert_eq!(response.connected, true);
        assert_eq!(response.reachable, false);
        assert_eq!(
            response.default_base_path.as_deref(),
            Some("/home/alice/jobs")
        );
        match response.host {
            Some(list_clusters_unit_response::Host::Ipaddr(value)) => {
                assert_eq!(value, "10.20.30.40")
            }
            other => panic!("expected ip host, got {other:?}"),
        }
    }

    #[test]
    fn cluster_status_to_response_maps_hostname_host() {
        let status = ClusterStatus {
            host: host_record(Address::Hostname("hpc.example".to_string())),
            connected: false,
            reachable: true,
        };

        let response = cluster_status_to_response(&status);
        match response.host {
            Some(list_clusters_unit_response::Host::Hostname(value)) => {
                assert_eq!(value, "hpc.example")
            }
            other => panic!("expected hostname host, got {other:?}"),
        }
    }

    #[test]
    fn job_record_to_response_maps_all_fields() {
        let record = JobRecord {
            id: 42,
            scheduler_id: Some(31415),
            name: "train".to_string(),
            created_at: "2026-02-07T00:00:00Z".to_string(),
            finished_at: Some("2026-02-07T00:10:00Z".to_string()),
            is_completed: true,
            terminal_state: Some("COMPLETED".to_string()),
            scheduler_state: Some("COMPLETED".to_string()),
            local_path: "/tmp/local".to_string(),
            remote_path: "/remote/path".to_string(),
            stdout_path: "/tmp/stdout".to_string(),
            stderr_path: Some("/tmp/stderr".to_string()),
            project_name: Some("p".to_string()),
            default_retrieve_path: Some("/tmp/out".to_string()),
            template_values: Some("{\"a\":\"b\"}".to_string()),
        };

        let response = job_record_to_response(&record);
        assert_eq!(response.job_id, 42);
        assert_eq!(response.scheduler_id, Some(31415));
        assert_eq!(response.name, "train");
        assert_eq!(response.created_at, "2026-02-07T00:00:00Z");
        assert_eq!(
            response.finished_at.as_deref(),
            Some("2026-02-07T00:10:00Z")
        );
        assert_eq!(response.is_completed, true);
        assert_eq!(response.terminal_state.as_deref(), Some("COMPLETED"));
        assert_eq!(response.scheduler_state.as_deref(), Some("COMPLETED"));
        assert_eq!(response.local_path, "/tmp/local");
        assert_eq!(response.remote_path, "/remote/path");
        assert_eq!(response.project_name.as_deref(), Some("p"));
        assert_eq!(response.default_retrieve_path.as_deref(), Some("/tmp/out"));
    }

    #[test]
    fn project_record_to_response_maps_all_fields() {
        let project = ProjectRecord {
            name: "proj".to_string(),
            path: "/tmp/project".to_string(),
            created_at: "2026-02-07T00:00:00Z".to_string(),
            updated_at: "2026-02-07T01:00:00Z".to_string(),
            version_tag: Some("v1".to_string()),
            tarball_hash: Some("abc123".to_string()),
            tool_version: Some("1.0.0".to_string()),
            template_config_json: Some("{\"key\":\"value\"}".to_string()),
            submit_sbatch_script: Some("submit.sbatch".to_string()),
            sbatch_scripts: vec!["a.sbatch".to_string(), "b.sbatch".to_string()],
            default_retrieve_path: Some("/tmp/out".to_string()),
            sync_include: vec!["src/**".to_string()],
            sync_exclude: vec!["target/**".to_string()],
        };

        let response = project_record_to_response(&project);
        assert_eq!(response.name, "proj");
        assert_eq!(response.path, "/tmp/project");
        assert_eq!(response.created_at, "2026-02-07T00:00:00Z");
        assert_eq!(response.updated_at, "2026-02-07T01:00:00Z");
        assert_eq!(response.version_tag.as_deref(), Some("v1"));
        assert_eq!(response.tarball_hash.as_deref(), Some("abc123"));
        assert_eq!(response.tool_version.as_deref(), Some("1.0.0"));
        assert_eq!(
            response.template_config_json.as_deref(),
            Some("{\"key\":\"value\"}")
        );
        assert_eq!(
            response.submit_sbatch_script.as_deref(),
            Some("submit.sbatch")
        );
        assert_eq!(
            response.sbatch_scripts,
            vec!["a.sbatch".to_string(), "b.sbatch".to_string()]
        );
        assert_eq!(response.default_retrieve_path.as_deref(), Some("/tmp/out"));
        assert_eq!(response.sync_include, vec!["src/**".to_string()]);
        assert_eq!(response.sync_exclude, vec!["target/**".to_string()]);
    }

    fn host_record(address: Address) -> HostRecord {
        HostRecord {
            id: 1,
            name: "cluster-a".to_string(),
            username: "alice".to_string(),
            address,
            port: 2222,
            identity_path: Some("~/.ssh/id_ed25519".to_string()),
            slurm: SlurmVersion {
                major: 25,
                minor: 5,
                patch: 0,
            },
            distro: Distro {
                name: "ubuntu".to_string(),
                version: "24.04".to_string(),
            },
            kernel_version: "6.8".to_string(),
            created_at: "2026-02-07T00:00:00Z".to_string(),
            updated_at: "2026-02-07T00:00:00Z".to_string(),
            accounting_available: true,
            default_base_path: Some("/home/alice/jobs".to_string()),
        }
    }
}
