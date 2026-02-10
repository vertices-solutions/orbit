import React from 'react';
import ComponentCreator from '@docusaurus/ComponentCreator';

export default [
  {
    path: '/',
    component: ComponentCreator('/', 'c88'),
    routes: [
      {
        path: '/next',
        component: ComponentCreator('/next', '7d3'),
        routes: [
          {
            path: '/next',
            component: ComponentCreator('/next', '914'),
            routes: [
              {
                path: '/next/',
                component: ComponentCreator('/next/', '5f5'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/',
                component: ComponentCreator('/next/cli/', 'b27'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/cluster/orbit-cluster',
                component: ComponentCreator('/next/cli/commands/cluster/orbit-cluster', 'ebe'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/cluster/orbit-cluster-add',
                component: ComponentCreator('/next/cli/commands/cluster/orbit-cluster-add', '839'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/cluster/orbit-cluster-delete',
                component: ComponentCreator('/next/cli/commands/cluster/orbit-cluster-delete', 'e56'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/cluster/orbit-cluster-get',
                component: ComponentCreator('/next/cli/commands/cluster/orbit-cluster-get', '807'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/cluster/orbit-cluster-list',
                component: ComponentCreator('/next/cli/commands/cluster/orbit-cluster-list', '35b'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/cluster/orbit-cluster-ls',
                component: ComponentCreator('/next/cli/commands/cluster/orbit-cluster-ls', 'cd7'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/cluster/orbit-cluster-set',
                component: ComponentCreator('/next/cli/commands/cluster/orbit-cluster-set', 'e7b'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/job/orbit-job',
                component: ComponentCreator('/next/cli/commands/job/orbit-job', '819'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/job/orbit-job-cancel',
                component: ComponentCreator('/next/cli/commands/job/orbit-job-cancel', 'a9e'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/job/orbit-job-cleanup',
                component: ComponentCreator('/next/cli/commands/job/orbit-job-cleanup', 'fb7'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/job/orbit-job-get',
                component: ComponentCreator('/next/cli/commands/job/orbit-job-get', '129'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/job/orbit-job-list',
                component: ComponentCreator('/next/cli/commands/job/orbit-job-list', '80f'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/job/orbit-job-logs',
                component: ComponentCreator('/next/cli/commands/job/orbit-job-logs', '2fd'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/job/orbit-job-ls',
                component: ComponentCreator('/next/cli/commands/job/orbit-job-ls', 'aa5'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/job/orbit-job-retrieve',
                component: ComponentCreator('/next/cli/commands/job/orbit-job-retrieve', '59e'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/job/orbit-job-submit',
                component: ComponentCreator('/next/cli/commands/job/orbit-job-submit', '7af'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/orbit',
                component: ComponentCreator('/next/cli/commands/orbit', 'f7a'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/orbit-completions',
                component: ComponentCreator('/next/cli/commands/orbit-completions', '19b'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/orbit-ping',
                component: ComponentCreator('/next/cli/commands/orbit-ping', 'a41'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/project/orbit-project',
                component: ComponentCreator('/next/cli/commands/project/orbit-project', '00c'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/project/orbit-project-build',
                component: ComponentCreator('/next/cli/commands/project/orbit-project-build', '995'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/project/orbit-project-check',
                component: ComponentCreator('/next/cli/commands/project/orbit-project-check', 'b22'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/project/orbit-project-delete',
                component: ComponentCreator('/next/cli/commands/project/orbit-project-delete', '6c5'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/project/orbit-project-init',
                component: ComponentCreator('/next/cli/commands/project/orbit-project-init', '88d'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/project/orbit-project-list',
                component: ComponentCreator('/next/cli/commands/project/orbit-project-list', 'da2'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/project/orbit-project-submit',
                component: ComponentCreator('/next/cli/commands/project/orbit-project-submit', '23b'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/orbitfile',
                component: ComponentCreator('/next/orbitfile', 'b39'),
                exact: true
              },
              {
                path: '/next/quickstart',
                component: ComponentCreator('/next/quickstart', 'a10'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/versioning',
                component: ComponentCreator('/next/versioning', '123'),
                exact: true,
                sidebar: "docsSidebar"
              }
            ]
          }
        ]
      },
      {
        path: '/',
        component: ComponentCreator('/', '187'),
        routes: [
          {
            path: '/',
            component: ComponentCreator('/', 'fce'),
            routes: [
              {
                path: '/cli/',
                component: ComponentCreator('/cli/', '03c'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster', '182'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-add',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-add', '7af'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-delete',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-delete', '09a'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-get',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-get', 'be5'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-list',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-list', 'c7a'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-ls',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-ls', 'ea6'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-set',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-set', '9bb'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job',
                component: ComponentCreator('/cli/commands/job/orbit-job', '638'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-cancel',
                component: ComponentCreator('/cli/commands/job/orbit-job-cancel', '384'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-cleanup',
                component: ComponentCreator('/cli/commands/job/orbit-job-cleanup', '978'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-get',
                component: ComponentCreator('/cli/commands/job/orbit-job-get', 'cf3'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-list',
                component: ComponentCreator('/cli/commands/job/orbit-job-list', 'ca1'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-logs',
                component: ComponentCreator('/cli/commands/job/orbit-job-logs', '57a'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-ls',
                component: ComponentCreator('/cli/commands/job/orbit-job-ls', 'b97'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-retrieve',
                component: ComponentCreator('/cli/commands/job/orbit-job-retrieve', 'ef7'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-submit',
                component: ComponentCreator('/cli/commands/job/orbit-job-submit', 'ae1'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/orbit',
                component: ComponentCreator('/cli/commands/orbit', '98c'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/orbit-completions',
                component: ComponentCreator('/cli/commands/orbit-completions', 'eb7'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/orbit-ping',
                component: ComponentCreator('/cli/commands/orbit-ping', '53d'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project',
                component: ComponentCreator('/cli/commands/project/orbit-project', '0ef'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project-build',
                component: ComponentCreator('/cli/commands/project/orbit-project-build', '369'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project-check',
                component: ComponentCreator('/cli/commands/project/orbit-project-check', '305'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project-delete',
                component: ComponentCreator('/cli/commands/project/orbit-project-delete', '576'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project-init',
                component: ComponentCreator('/cli/commands/project/orbit-project-init', 'b76'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project-list',
                component: ComponentCreator('/cli/commands/project/orbit-project-list', '1c6'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project-submit',
                component: ComponentCreator('/cli/commands/project/orbit-project-submit', 'f9b'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/orbitfile',
                component: ComponentCreator('/orbitfile', '0ef'),
                exact: true
              },
              {
                path: '/quickstart',
                component: ComponentCreator('/quickstart', '0bc'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/versioning',
                component: ComponentCreator('/versioning', 'abc'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/',
                component: ComponentCreator('/', '9fc'),
                exact: true,
                sidebar: "docsSidebar"
              }
            ]
          }
        ]
      }
    ]
  },
  {
    path: '*',
    component: ComponentCreator('*'),
  },
];
