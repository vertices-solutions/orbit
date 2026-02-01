// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use crate::app::errors::{codes, AppError, AppErrorKind};
use crate::app::types::{ClusterStatus, JobRecord, SyncFilterAction, SyncFilterRule};
use proto::{
    list_clusters_unit_response, ListClustersUnitResponse, ListJobsUnitResponse,
    SubmitPathFilterAction, SubmitPathFilterRule,
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
    }
}
