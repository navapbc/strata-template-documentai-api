<p>
  <img src="template-only-docs/assets/Nava-Strata-Logo-V02.svg" alt="Nava Strata" width="400">
</p>
<p><i>Open source tools for every layer of government service delivery.</i></p>
<p><b>Strata is a gold-standard target architecture and suite of open-source tools that gives government agencies everything they need to run a modern service.</b></p>

<h4 align="center">
  <a href="https://github.com/navapbc/strata-template-documentai-api/blob/main/LICENSE">
    <img src="https://img.shields.io/badge/license-apache_2.0-red" alt="Nava Strata is released under the Apache 2.0 license" >
  </a>
  <a href="https://github.com/navapbc/strata-template-documentai-api/blob/main/CONTRIBUTING.md">
    <img src="https://img.shields.io/badge/PRs-Welcome-brightgreen" alt="PRs welcome!" />
  </a>
  <a href="https://github.com/navapbc/strata-template-documentai-api/commits/main">
    <img src="https://img.shields.io/github/commit-activity/m/navapbc/strata-template-documentai-api" alt="git commit activity" />
  </a>
</h4>

# Template DocumentAI API application

This is a template repository for a DocumentAI API application. Unlike some
other Strata templates, this is more of a complete application intended for use
almost out of the box.

See [`navapbc/platform`](https://github.com/navapbc/strata) for other template repos.

## Features

- API for identifying and extracting data from "document" files

## Repo structure
```text
.
├── template           # The template (the things that get installed/updated)
│   ├── .github        # GitHub workflows
│   ├── docs           # Project docs and decision records
│   └── {{app_name}}   # Application code
└── template-only-docs # Template repo docs
```

## Installation

To get started using the template application on your project, for an
application to be called `<APP_NAME>`:

1. [Install the nava-platform tool](https://github.com/navapbc/platform-cli).
2. Install template by running in your project's root:
    ```sh
    nava-platform app install --template-uri https://github.com/navapbc/strata-template-documentai-api . <APP_NAME>
    ```
3. Follow the steps in `<APP_NAME>/README.md` to set up the application locally.
4. Optional, if using the Platform infrastructure template: [Follow the steps in the `template-infra` README](https://github.com/navapbc/template-infra#installation) to set up the various pieces of your infrastructure.

## Updates

If you have previously installed this template and would like to update your
project to use a newer version of this template:

1. [Install the nava-platform tool](https://github.com/navapbc/platform-cli).
2. Update app template by running in your project's root:
    ```sh
    nava-platform app update . <APP_NAME>
    ```
## License

This project is licensed under the Apache 2.0 License. See the [LICENSE](LICENSE) file for details.

## Community

- [Code of Conduct](CODE_OF_CONDUCT.md)
- [Contributing Guidelines](CONTRIBUTING.md)
- [Security Policy](SECURITY.md)
