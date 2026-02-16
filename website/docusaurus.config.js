// @ts-check

/** @type {import('@docusaurus/types').Config} */
const config = {
  title: 'Orbit Documentation',
  tagline: 'Local-first Slurm interface',
  url: 'https://orbit.vertices.solutions',
  baseUrl: '',
  onBrokenLinks: 'throw',
  markdown: {
    hooks: {
      onBrokenMarkdownLinks: 'warn',
    },
  },
  i18n: {
    defaultLocale: 'en',
    locales: ['en'],
  },
  presets: [
    [
      'classic',
      {
        docs: {
          routeBasePath: '/',
          sidebarPath: require.resolve('./sidebars.js'),
          editUrl: 'https://github.com/vertices-solutions/orbit/tree/main/website/',
          versions: {
            current: {
              label: 'next',
            },
          },
        },
        blog: false,
        pages: false,
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
  themeConfig: {
    navbar: {
      title: 'Orbit Docs',
      items: [
        {
          type: 'docSidebar',
          sidebarId: 'docsSidebar',
          position: 'left',
          label: 'Documentation',
        },
        {
          type: 'docsVersionDropdown',
          position: 'left',
          dropdownActiveClassDisabled: true,
        },
        {
          href: 'https://github.com/vertices-solutions/orbit',
          label: 'GitHub',
          position: 'right',
        },
      ],
    },
    footer: {
      style: 'dark',
      links: [
        {
          title: 'Project',
          items: [
            {
              label: 'GitHub',
              href: 'https://github.com/vertices-solutions/orbit',
            },

          ],
        },
      ],
      copyright: `Copyright © ${new Date().getFullYear()} Orbit Contributors`,
    },
    prism: {
      additionalLanguages: ['bash', 'toml'],
    },
  },
};

module.exports = config;
