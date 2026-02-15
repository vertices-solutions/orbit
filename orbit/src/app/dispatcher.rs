// SPDX-License-Identifier: AGPL-3.0-only
// Copyright (C) 2026 Alex Sizykh

use crate::app::AppContext;
use crate::app::commands::{ClusterCommand, Command, JobCommand, ProjectCommand};
use crate::app::errors::AppResult;
use crate::app::handlers;

pub struct Dispatcher {
    ctx: AppContext,
}

impl Dispatcher {
    pub fn new(ctx: AppContext) -> Self {
        Self { ctx }
    }

    pub async fn dispatch(&self, command: Command) -> AppResult<i32> {
        let result = match command {
            Command::Ping(cmd) => handlers::handle_ping(&self.ctx, cmd).await,
            Command::Job(cmd) => match cmd {
                JobCommand::Submit(cmd) => handlers::handle_job_submit(&self.ctx, cmd).await,
                JobCommand::List(cmd) => handlers::handle_job_list(&self.ctx, cmd).await,
                JobCommand::Get(cmd) => handlers::handle_job_get(&self.ctx, cmd).await,
                JobCommand::Logs(cmd) => handlers::handle_job_logs(&self.ctx, cmd).await,
                JobCommand::Cancel(cmd) => handlers::handle_job_cancel(&self.ctx, cmd).await,
                JobCommand::Cleanup(cmd) => handlers::handle_job_cleanup(&self.ctx, cmd).await,
                JobCommand::Ls(cmd) => handlers::handle_job_ls(&self.ctx, cmd).await,
                JobCommand::Retrieve(cmd) => handlers::handle_job_retrieve(&self.ctx, cmd).await,
            },
            Command::Cluster(cmd) => match cmd {
                ClusterCommand::List(cmd) => handlers::handle_cluster_list(&self.ctx, cmd).await,
                ClusterCommand::Get(cmd) => handlers::handle_cluster_get(&self.ctx, cmd).await,
                ClusterCommand::Ls(cmd) => handlers::handle_cluster_ls(&self.ctx, cmd).await,
                ClusterCommand::Add(cmd) => handlers::handle_cluster_add(&self.ctx, cmd).await,
                ClusterCommand::Set(cmd) => handlers::handle_cluster_set(&self.ctx, cmd).await,
                ClusterCommand::Delete(cmd) => {
                    handlers::handle_cluster_delete(&self.ctx, cmd).await
                }
            },
            Command::Project(cmd) => match cmd {
                ProjectCommand::Init(cmd) => handlers::handle_project_init(&self.ctx, cmd).await,
                ProjectCommand::Build(cmd) => handlers::handle_project_build(&self.ctx, cmd).await,
                ProjectCommand::Submit(cmd) => {
                    handlers::handle_project_submit(&self.ctx, cmd).await
                }
                ProjectCommand::List(cmd) => handlers::handle_project_list(&self.ctx, cmd).await,
                ProjectCommand::Delete(cmd) => {
                    handlers::handle_project_delete(&self.ctx, cmd).await
                }
            },
        };

        match result {
            Ok(output) => {
                self.ctx.output.render(&output).await?;
                Ok(0)
            }
            Err(err) => {
                self.ctx.output.render_error(&err).await?;
                Ok(err.exit_code)
            }
        }
    }
}
