import React from 'react';
import ComponentCreator from '@docusaurus/ComponentCreator';

export default [
  {
    path: '/__docusaurus/debug',
    component: ComponentCreator('/__docusaurus/debug', '5ff'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/config',
    component: ComponentCreator('/__docusaurus/debug/config', '5ba'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/content',
    component: ComponentCreator('/__docusaurus/debug/content', 'a2b'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/globalData',
    component: ComponentCreator('/__docusaurus/debug/globalData', 'c3c'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/metadata',
    component: ComponentCreator('/__docusaurus/debug/metadata', '156'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/registry',
    component: ComponentCreator('/__docusaurus/debug/registry', '88c'),
    exact: true
  },
  {
    path: '/__docusaurus/debug/routes',
    component: ComponentCreator('/__docusaurus/debug/routes', '000'),
    exact: true
  },
  {
    path: '/',
    component: ComponentCreator('/', 'a94'),
    routes: [
      {
        path: '/0.14.0',
        component: ComponentCreator('/0.14.0', '647'),
        routes: [
          {
            path: '/0.14.0',
            component: ComponentCreator('/0.14.0', '4d1'),
            routes: [
              {
                path: '/0.14.0/',
                component: ComponentCreator('/0.14.0/', '137'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/',
                component: ComponentCreator('/0.14.0/cli/', '9c8'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/cluster/orbit-cluster',
                component: ComponentCreator('/0.14.0/cli/commands/cluster/orbit-cluster', '175'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/cluster/orbit-cluster-add',
                component: ComponentCreator('/0.14.0/cli/commands/cluster/orbit-cluster-add', 'c05'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/cluster/orbit-cluster-delete',
                component: ComponentCreator('/0.14.0/cli/commands/cluster/orbit-cluster-delete', '8c5'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/cluster/orbit-cluster-get',
                component: ComponentCreator('/0.14.0/cli/commands/cluster/orbit-cluster-get', '951'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/cluster/orbit-cluster-list',
                component: ComponentCreator('/0.14.0/cli/commands/cluster/orbit-cluster-list', 'a37'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/cluster/orbit-cluster-ls',
                component: ComponentCreator('/0.14.0/cli/commands/cluster/orbit-cluster-ls', '7aa'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/cluster/orbit-cluster-set',
                component: ComponentCreator('/0.14.0/cli/commands/cluster/orbit-cluster-set', 'ed6'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/job/orbit-job',
                component: ComponentCreator('/0.14.0/cli/commands/job/orbit-job', 'ea6'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/job/orbit-job-cancel',
                component: ComponentCreator('/0.14.0/cli/commands/job/orbit-job-cancel', 'b58'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/job/orbit-job-cleanup',
                component: ComponentCreator('/0.14.0/cli/commands/job/orbit-job-cleanup', '84e'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/job/orbit-job-get',
                component: ComponentCreator('/0.14.0/cli/commands/job/orbit-job-get', 'd2a'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/job/orbit-job-list',
                component: ComponentCreator('/0.14.0/cli/commands/job/orbit-job-list', '78d'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/job/orbit-job-logs',
                component: ComponentCreator('/0.14.0/cli/commands/job/orbit-job-logs', '6bb'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/job/orbit-job-ls',
                component: ComponentCreator('/0.14.0/cli/commands/job/orbit-job-ls', '453'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/job/orbit-job-retrieve',
                component: ComponentCreator('/0.14.0/cli/commands/job/orbit-job-retrieve', '49d'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/job/orbit-job-submit',
                component: ComponentCreator('/0.14.0/cli/commands/job/orbit-job-submit', '40a'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/orbit',
                component: ComponentCreator('/0.14.0/cli/commands/orbit', 'e36'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/orbit-completions',
                component: ComponentCreator('/0.14.0/cli/commands/orbit-completions', '8f0'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/orbit-ping',
                component: ComponentCreator('/0.14.0/cli/commands/orbit-ping', '103'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/project/orbit-project',
                component: ComponentCreator('/0.14.0/cli/commands/project/orbit-project', 'ab6'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/project/orbit-project-build',
                component: ComponentCreator('/0.14.0/cli/commands/project/orbit-project-build', '979'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/project/orbit-project-check',
                component: ComponentCreator('/0.14.0/cli/commands/project/orbit-project-check', 'aca'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/project/orbit-project-delete',
                component: ComponentCreator('/0.14.0/cli/commands/project/orbit-project-delete', 'a2a'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/project/orbit-project-init',
                component: ComponentCreator('/0.14.0/cli/commands/project/orbit-project-init', 'd95'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/project/orbit-project-list',
                component: ComponentCreator('/0.14.0/cli/commands/project/orbit-project-list', 'e26'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/cli/commands/project/orbit-project-submit',
                component: ComponentCreator('/0.14.0/cli/commands/project/orbit-project-submit', '5af'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/orbitfile',
                component: ComponentCreator('/0.14.0/orbitfile', '8b1'),
                exact: true
              },
              {
                path: '/0.14.0/quickstart',
                component: ComponentCreator('/0.14.0/quickstart', 'e4e'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/0.14.0/versioning',
                component: ComponentCreator('/0.14.0/versioning', '019'),
                exact: true,
                sidebar: "docsSidebar"
              }
            ]
          }
        ]
      },
      {
        path: '/next',
        component: ComponentCreator('/next', '640'),
        routes: [
          {
            path: '/next',
            component: ComponentCreator('/next', '86f'),
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
                path: '/next/cli/commands/blueprint/orbit-blueprint',
                component: ComponentCreator('/next/cli/commands/blueprint/orbit-blueprint', 'c54'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/blueprint/orbit-blueprint-build',
                component: ComponentCreator('/next/cli/commands/blueprint/orbit-blueprint-build', 'b53'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/blueprint/orbit-blueprint-delete',
                component: ComponentCreator('/next/cli/commands/blueprint/orbit-blueprint-delete', '368'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/blueprint/orbit-blueprint-list',
                component: ComponentCreator('/next/cli/commands/blueprint/orbit-blueprint-list', '9f2'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/cli/commands/blueprint/orbit-blueprint-run',
                component: ComponentCreator('/next/cli/commands/blueprint/orbit-blueprint-run', 'e32'),
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
                path: '/next/cli/commands/cluster/orbit-cluster-connect',
                component: ComponentCreator('/next/cli/commands/cluster/orbit-cluster-connect', 'cf8'),
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
                path: '/next/cli/commands/job/orbit-job-run',
                component: ComponentCreator('/next/cli/commands/job/orbit-job-run', 'dbb'),
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
                path: '/next/cli/commands/orbit-init',
                component: ComponentCreator('/next/cli/commands/orbit-init', 'e3d'),
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
                path: '/next/cli/commands/orbit-run',
                component: ComponentCreator('/next/cli/commands/orbit-run', 'b3b'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/next/orbitfile',
                component: ComponentCreator('/next/orbitfile', '2e0'),
                exact: true,
                sidebar: "docsSidebar"
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
        component: ComponentCreator('/', '996'),
        routes: [
          {
            path: '/',
            component: ComponentCreator('/', '29e'),
            routes: [
              {
                path: '/cli/',
                component: ComponentCreator('/cli/', 'c70'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster', '0d8'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-add',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-add', '683'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-delete',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-delete', 'e58'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-get',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-get', '8ad'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-list',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-list', 'c9b'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-ls',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-ls', 'c4d'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/cluster/orbit-cluster-set',
                component: ComponentCreator('/cli/commands/cluster/orbit-cluster-set', '846'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job',
                component: ComponentCreator('/cli/commands/job/orbit-job', 'fa4'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-cancel',
                component: ComponentCreator('/cli/commands/job/orbit-job-cancel', 'd8a'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-cleanup',
                component: ComponentCreator('/cli/commands/job/orbit-job-cleanup', '6cd'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-get',
                component: ComponentCreator('/cli/commands/job/orbit-job-get', '9be'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-list',
                component: ComponentCreator('/cli/commands/job/orbit-job-list', '51b'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-logs',
                component: ComponentCreator('/cli/commands/job/orbit-job-logs', '450'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-ls',
                component: ComponentCreator('/cli/commands/job/orbit-job-ls', 'e56'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-retrieve',
                component: ComponentCreator('/cli/commands/job/orbit-job-retrieve', 'c36'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/job/orbit-job-submit',
                component: ComponentCreator('/cli/commands/job/orbit-job-submit', 'f32'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/orbit',
                component: ComponentCreator('/cli/commands/orbit', '7f3'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/orbit-completions',
                component: ComponentCreator('/cli/commands/orbit-completions', '22f'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/orbit-ping',
                component: ComponentCreator('/cli/commands/orbit-ping', '80c'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project',
                component: ComponentCreator('/cli/commands/project/orbit-project', '716'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project-build',
                component: ComponentCreator('/cli/commands/project/orbit-project-build', 'dae'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project-delete',
                component: ComponentCreator('/cli/commands/project/orbit-project-delete', '6ed'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project-init',
                component: ComponentCreator('/cli/commands/project/orbit-project-init', 'd32'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project-list',
                component: ComponentCreator('/cli/commands/project/orbit-project-list', '6ec'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/cli/commands/project/orbit-project-submit',
                component: ComponentCreator('/cli/commands/project/orbit-project-submit', '562'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/orbitfile',
                component: ComponentCreator('/orbitfile', 'e7e'),
                exact: true
              },
              {
                path: '/quickstart',
                component: ComponentCreator('/quickstart', '106'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/versioning',
                component: ComponentCreator('/versioning', '011'),
                exact: true,
                sidebar: "docsSidebar"
              },
              {
                path: '/',
                component: ComponentCreator('/', 'cad'),
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
