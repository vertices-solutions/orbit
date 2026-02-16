// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use proto::{ListClustersUnitResponse, ListJobsUnitResponse};

use crate::adapters::presentation::{cluster_host_string, job_status};
use crate::app::commands::BlueprintListItem;

pub(super) fn format_clusters_table(
    clusters: &[ListClustersUnitResponse],
    show_reachability: bool,
) -> String {
    if clusters.is_empty() {
        return "No clusters registered\n".to_string();
    }

    if show_reachability {
        let headers = [
            "name",
            "destination",
            "status",
            "default",
            "reachable",
            "accounting",
        ];
        let mut rows: Vec<(String, String, String, String, String, String)> = Vec::new();

        for item in clusters.iter() {
            let ssh_str = cluster_ssh_string(item);
            let connected_str = match item.connected {
                true => "connected",
                false => "disconnected",
            };
            let reachable_str = match item.reachable {
                true => "yes",
                false => "no",
            };
            let accounting_str = match item.accounting_available {
                true => "enabled",
                false => "disabled",
            };
            let default_str = match item.is_default {
                true => "yes",
                false => "no",
            };
            rows.push((
                item.name.clone(),
                ssh_str,
                connected_str.to_string(),
                default_str.to_string(),
                reachable_str.to_string(),
                accounting_str.to_string(),
            ));
        }

        let mut widths: [usize; 6] = [
            str_width(headers[0]),
            str_width(headers[1]),
            str_width(headers[2]),
            str_width(headers[3]),
            str_width(headers[4]),
            str_width(headers[5]),
        ];
        for row in rows.iter() {
            widths[0] = widths[0].max(str_width(&row.0));
            widths[1] = widths[1].max(str_width(&row.1));
            widths[2] = widths[2].max(str_width(&row.2));
            widths[3] = widths[3].max(str_width(&row.3));
            widths[4] = widths[4].max(str_width(&row.4));
            widths[5] = widths[5].max(str_width(&row.5));
        }

        let mut output = String::new();
        output.push_str(&format!(
            "{:<w0$}  {:<w1$}  {:<w2$}  {:<w3$}  {:<w4$}  {:<w5$}\n",
            headers[0],
            headers[1],
            headers[2],
            headers[3],
            headers[4],
            headers[5],
            w0 = widths[0],
            w1 = widths[1],
            w2 = widths[2],
            w3 = widths[3],
            w4 = widths[4],
            w5 = widths[5],
        ));

        for row in rows {
            output.push_str(&format!(
                "{:<w0$}  {:<w1$}  {:<w2$}  {:<w3$}  {:<w4$}  {:<w5$}\n",
                row.0,
                row.1,
                row.2,
                row.3,
                row.4,
                row.5,
                w0 = widths[0],
                w1 = widths[1],
                w2 = widths[2],
                w3 = widths[3],
                w4 = widths[4],
                w5 = widths[5],
            ));
        }

        return output;
    }

    let headers = ["name", "destination", "status", "default", "accounting"];
    let mut rows: Vec<(String, String, String, String, String)> = Vec::new();

    for item in clusters.iter() {
        let ssh_str = cluster_ssh_string(item);
        let connected_str = match item.connected {
            true => "connected",
            false => "disconnected",
        };
        let accounting_str = match item.accounting_available {
            true => "enabled",
            false => "disabled",
        };
        let default_str = match item.is_default {
            true => "yes",
            false => "no",
        };
        rows.push((
            item.name.clone(),
            ssh_str,
            connected_str.to_string(),
            default_str.to_string(),
            accounting_str.to_string(),
        ));
    }

    let mut widths: [usize; 5] = [
        str_width(headers[0]),
        str_width(headers[1]),
        str_width(headers[2]),
        str_width(headers[3]),
        str_width(headers[4]),
    ];
    for row in rows.iter() {
        widths[0] = widths[0].max(str_width(&row.0));
        widths[1] = widths[1].max(str_width(&row.1));
        widths[2] = widths[2].max(str_width(&row.2));
        widths[3] = widths[3].max(str_width(&row.3));
        widths[4] = widths[4].max(str_width(&row.4));
    }

    let mut output = String::new();
    output.push_str(&format!(
        "{:<w0$}  {:<w1$}  {:<w2$}  {:<w3$}  {:<w4$}\n",
        headers[0],
        headers[1],
        headers[2],
        headers[3],
        headers[4],
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3],
        w4 = widths[4],
    ));

    for row in rows {
        output.push_str(&format!(
            "{:<w0$}  {:<w1$}  {:<w2$}  {:<w3$}  {:<w4$}\n",
            row.0,
            row.1,
            row.2,
            row.3,
            row.4,
            w0 = widths[0],
            w1 = widths[1],
            w2 = widths[2],
            w3 = widths[3],
            w4 = widths[4],
        ));
    }

    output
}

pub(super) fn format_cluster_details(item: &ListClustersUnitResponse) -> String {
    let host_str = cluster_host_string(item);
    let connected_str = match item.connected {
        true => "connected",
        false => "disconnected",
    };
    let accounting_str = match item.accounting_available {
        true => "enabled",
        false => "disabled",
    };
    let default_str = match item.is_default {
        true => "yes",
        false => "no",
    };
    format!(
        "name: {}\nusername: {}\naddress: {}\nport: {}\nstatus: {}\ndefault: {}\naccounting: {}\nidentity_path: {}\ndefault_base_path: {}\n",
        item.name,
        item.username,
        host_str,
        item.port,
        connected_str,
        default_str,
        accounting_str,
        item.identity_path.as_deref().unwrap_or("-"),
        item.default_base_path.as_deref().unwrap_or("-")
    )
}

pub(super) fn format_jobs_table(jobs: &[ListJobsUnitResponse]) -> String {
    let headers = [
        "job id",
        "cluster id",
        "blueprint",
        "status",
        "created",
        "finished",
        "scheduler id",
    ];
    let mut rows: Vec<(String, String, String, String, String, String, String)> = Vec::new();

    for item in jobs.iter() {
        let job_id = item.job_id.to_string();
        let scheduler_id = item
            .scheduler_id
            .map(|id| id.to_string())
            .unwrap_or_else(|| "-".to_string());
        let blueprint = item
            .blueprint_name
            .clone()
            .unwrap_or_else(|| "none".to_string());
        let completed_str = job_status(item);
        let finished_at = item.finished_at.clone().unwrap_or_else(|| "-".to_string());
        rows.push((
            job_id,
            item.name.clone(),
            blueprint,
            completed_str.to_string(),
            item.created_at.clone(),
            finished_at,
            scheduler_id,
        ));
    }

    let mut widths: [usize; 7] = [
        str_width(headers[0]),
        str_width(headers[1]),
        str_width(headers[2]),
        str_width(headers[3]),
        str_width(headers[4]),
        str_width(headers[5]),
        str_width(headers[6]),
    ];
    for row in rows.iter() {
        widths[0] = widths[0].max(str_width(&row.0));
        widths[1] = widths[1].max(str_width(&row.1));
        widths[2] = widths[2].max(str_width(&row.2));
        widths[3] = widths[3].max(str_width(&row.3));
        widths[4] = widths[4].max(str_width(&row.4));
        widths[5] = widths[5].max(str_width(&row.5));
    }

    let mut output = String::new();
    output.push_str(&format!(
        "{:<w0$}  {:<w1$}  {:<w2$}  {:<w3$}  {:<w4$}  {:<w5$}  {:<w6$}\n",
        headers[0],
        headers[1],
        headers[2],
        headers[3],
        headers[4],
        headers[5],
        headers[6],
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3],
        w4 = widths[4],
        w5 = widths[5],
        w6 = widths[6],
    ));

    for row in rows {
        output.push_str(&format!(
            "{:<w0$}  {:<w1$}  {:<w2$}  {:<w3$}  {:<w4$}  {:<w5$}  {:<w6$}\n",
            row.0,
            row.1,
            row.2,
            row.3,
            row.4,
            row.5,
            row.6,
            w0 = widths[0],
            w1 = widths[1],
            w2 = widths[2],
            w3 = widths[3],
            w4 = widths[4],
            w5 = widths[5],
            w6 = widths[6],
        ));
    }

    output
}

pub(super) fn format_job_details(item: &ListJobsUnitResponse) -> String {
    let scheduler_id = item
        .scheduler_id
        .map(|id| id.to_string())
        .unwrap_or_else(|| "-".to_string());
    let completed_str = job_status(item);
    let blueprint = item.blueprint_name.as_deref().unwrap_or("none");
    format!(
        "job_id: {}\nlocal_path: {}\nremote_path: {}\nname: {}\nblueprint: {}\nstatus: {}\nterminal_state: {}\ncreated: {}\nfinished: {}\nscheduler_id: {}\n",
        item.job_id,
        item.local_path.as_str(),
        item.remote_path.as_str(),
        item.name,
        blueprint,
        completed_str,
        item.terminal_state.as_deref().unwrap_or("-"),
        item.created_at,
        item.finished_at.as_deref().unwrap_or("-"),
        scheduler_id
    )
}

pub(super) fn format_blueprints_table(blueprints: &[BlueprintListItem]) -> String {
    let headers = ["name", "latest tag", "path", "updated"];
    let mut rows: Vec<(String, String, String, String)> = Vec::new();
    for blueprint in blueprints {
        rows.push((
            blueprint.name.clone(),
            blueprint
                .latest_tag
                .clone()
                .unwrap_or_else(|| "-".to_string()),
            blueprint.path.clone(),
            blueprint.updated_at.clone(),
        ));
    }

    let mut widths: [usize; 4] = [
        str_width(headers[0]),
        str_width(headers[1]),
        str_width(headers[2]),
        str_width(headers[3]),
    ];
    for row in &rows {
        widths[0] = widths[0].max(str_width(&row.0));
        widths[1] = widths[1].max(str_width(&row.1));
        widths[2] = widths[2].max(str_width(&row.2));
        widths[3] = widths[3].max(str_width(&row.3));
    }

    let mut output = String::new();
    output.push_str(&format!(
        "{:<w0$}  {:<w1$}  {:<w2$}  {:<w3$}\n",
        headers[0],
        headers[1],
        headers[2],
        headers[3],
        w0 = widths[0],
        w1 = widths[1],
        w2 = widths[2],
        w3 = widths[3],
    ));
    for row in rows {
        output.push_str(&format!(
            "{:<w0$}  {:<w1$}  {:<w2$}  {:<w3$}\n",
            row.0,
            row.1,
            row.2,
            row.3,
            w0 = widths[0],
            w1 = widths[1],
            w2 = widths[2],
            w3 = widths[3],
        ));
    }
    output
}

fn cluster_ssh_string(item: &ListClustersUnitResponse) -> String {
    let host = cluster_host_string(item);
    let formatted_host = if host.contains(':') && !(host.starts_with('[') && host.ends_with(']')) {
        format!("[{host}]")
    } else {
        host
    };
    format!("{}@{}:{}", item.username, formatted_host, item.port)
}

fn str_width(value: &str) -> usize {
    value.chars().count()
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
            blueprint_name: None,
            default_retrieve_path: None,
        }
    }

    #[test]
    fn cluster_ssh_string_includes_username_host_port() {
        let cluster = sample_cluster(Some(proto::list_clusters_unit_response::Host::Hostname(
            "node".to_string(),
        )));
        assert_eq!(cluster_ssh_string(&cluster), "alice@node:22");
    }

    #[test]
    fn format_clusters_table_includes_headers_and_rows() {
        let cluster = sample_cluster(Some(proto::list_clusters_unit_response::Host::Hostname(
            "node".to_string(),
        )));
        let output = format_clusters_table(&[cluster], true);
        assert!(output.contains("name"));
        assert!(output.contains("destination"));
        assert!(output.contains("reachable"));
        assert!(output.contains("cluster-a"));
        assert!(output.contains("alice@node:22"));
        assert!(output.contains("yes"));
    }

    #[test]
    fn format_clusters_table_omits_reachability_when_disabled() {
        let cluster = sample_cluster(Some(proto::list_clusters_unit_response::Host::Hostname(
            "node".to_string(),
        )));
        let output = format_clusters_table(&[cluster], false);
        assert!(output.contains("name"));
        assert!(output.contains("destination"));
        assert!(!output.contains("reachable"));
    }

    #[test]
    fn format_clusters_table_reports_empty_list() {
        let output_with_reachability = format_clusters_table(&[], true);
        let output_without_reachability = format_clusters_table(&[], false);

        assert_eq!(output_with_reachability, "No clusters registered\n");
        assert_eq!(output_without_reachability, "No clusters registered\n");
    }

    #[test]
    fn format_jobs_table_includes_headers_and_rows() {
        let job = sample_job(true, Some("COMPLETED"), None);
        let output = format_jobs_table(&[job]);
        assert!(output.contains("job id"));
        assert!(output.contains("blueprint"));
        assert!(output.contains("cluster-a"));
        assert!(output.contains("none"));
        assert!(output.contains("completed"));
    }
}
