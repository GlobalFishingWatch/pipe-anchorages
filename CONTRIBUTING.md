[docker compose]: https://docs.docker.com/compose/install/linux/
[docker official instructions]: https://docs.docker.com/engine/install/
[Git Flow]: https://nvie.com/posts/a-successful-git-branching-model/
[GitHub Flow]: https://githubflow.github.io
[PEP8]: https://peps.python.org/pep-0008/
[pre-commit]: https://pre-commit.com
[How to Write a Git Commit Message]: https://cbea.ms/git-commit/
[interactive rebase]: https://git-scm.com/book/en/v2/Git-Tools-Rewriting-History

[Makefile]: Makefile
[pip-tools]: https://pip-tools.readthedocs.io/en/stable/
[pyproject.toml]: pyproject.toml
[requirements.txt]: requirements.txt
[requirements-test.txt]: requirements-test.txt

[GIT-FLOW.md]: docs/contributing/GIT-FLOW.md
[GITHUB-FLOW.md]: docs/contributing/GITHUB-FLOW.md

[Preparing the environment]: #preparing-the-environment
[Updating dependencies]: #updating-dependencies
[Git Workflow]: #git-workflow
[How to work on a feature branch]: #how-to-work-on-a-feature-branch
[How to deploy]: #how-to-deploy

# How to Contribute

<div align="justify">

These guidelines aim to maintain code quality,
clearly communicate the state of the codebase,
and standardize the development workflow.
Use your best judgment, and feel free to propose changes
to the guidelines by submitting a pull request.

</div>

## Table of Contents

- [Preparing the environment]
- [Updating dependencies]
- [Git Workflow]
- [How to work on a feature branch]
- [How to deploy]


## Preparing the environment

<div align="justify">

Install Docker Engine using the [docker official instructions] (avoid snap packages)
and the [docker compose] plugin. No other system dependencies are required.

1. First, clone the repository.
```shell
git clone https://github.com/GlobalFishingWatch/pipe-anchorages.git
```

2. Make sure you can build the docker image:
```shell
make docker-build
```

3. In order to be able to connect to BigQuery, authenticate and configure the project:
```shell
make docker-gcp
```

4. Create virtual environment and activate it:
```shell
make venv
./.venv/bin/activate
```

5. Install dependencies and the python package:
```shell
make install
```

6. (Optional) Install [pre-commit] hooks:
```shell
make hooks
```
This step is strongly recommended to maintain code quality and prevent technical debt,
particularly regarding documentation, [PEP8] compliance, spelling, and type-hinting.
If this feels too rigid for your current workflow,
at a minimum, make regular use of the provided [Makefile] commands:
`format`, `lint`, `codespell`, and `typecheck`.

[PEP8] checks will be enforced in the GitHub CI.

7. Make sure you can run unit tests:
```shell
make test
```
</div>

> [!NOTE]
> Alternatively,
  you can handle all the development inside a docker container
  without installing dependencies in a virtual environment.
  Use `make docker-shell` to enter a docker container.


## Updating dependencies

<div align="justify">

The [requirements.txt] file contains all transitive dependencies pinned to specific versions.
It is automatically generated using [pip-tools],
based on the dependencies specified in [pyproject.toml].
This process ensures reproducibility,
allowing the application to run consistently across different environments.

Use [pyproject.toml] to define high-level dependencies with flexible version constraints
(e.g., ~=1.2, >=1.0, <2.0, ...).

To re-compile dependencies, just run
```shell
make reqs
```

If you want to upgrade all dependencies to latest compatible versions, just run:
```shell
make reqs-upgrade
```
</div>

> [!IMPORTANT]
> Do not modify [requirements.txt] manually.

> [!NOTE]
> Remember that if you change the [requirements.txt],
you need to rebuild the docker image (`make docker-build`) in order to use it locally.

## Git Workflow

<div align="justify">

In this repository we follow [GitHub Flow].
You can read a summary in [GIT-FLOW.md].

</div>

## How to work on a feature branch

<div align="justify">

To work on a new feature or fix,
create a branch from `main`.

Try to follow these guidelines:

- Branch names should be descriptive and concise,
  all lowercase and with words separated by hyphens "-".
  Optionally, feature branch names can be prefixed with **JIRA** ticket.
  For example, you can use something like `PIPELINE-2020-name-of-the-branch`.

- Write clear commit messages. See [How to Write a Git Commit Message].

- Maintain a clean commit history in your feature branch.
  Use interactive rebase (`git rebase -i`) to squash, reorder, or edit commits.[^1]
  
- If you are not using [pre-commit] hooks,
  use the provided [Makefile] commands (`format`, `lint`, `codespell`, `typecheck`)
  as much as possible to maintain code quality.

- Add unit tests for each piece of code:
  * Avoid connecting to external services during unit tests. Use mocks as needed.
  * Ensure unit tests run as fast as possible.

- When submitting a Pull Request (**PR**), ensure it meets the following criteria:
  * It targets the correct base branch (depending on the chosen [Git Workflow]).
  * It is focused and limited in scope to simplify the review process.
  * Its description includes a link to the related **JIRA** ticket
    to support integration between the issue and the **PR**.

[^1]: When performing a rebase, it's acceptable to `push --force` to your own feature branch,
      but **never** on shared branches.
      Protect `main` from force pushes via GitHub settings.

</div>

## How to deploy

<div align="justify">

A Google Cloud build that publishes a Docker image is triggered in the following cases:  
- When a commit is merged into `main`.  
- When a new tag is created.


</div>
