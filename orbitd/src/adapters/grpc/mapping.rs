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
