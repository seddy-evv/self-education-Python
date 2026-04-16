# Poetry is a modern Python tool for dependency management and packaging. It streamlines the process of creating,
# managing, and publishing Python projects by using a single configuration file and a set of intuitive commands.
# Poetry ensures that your project’s dependencies are consistent and reproducible, making collaboration and
# deployment easier.


# Main Poetry Commands
"""
- poetry new <project-name>
Create a new Python project with a standard structure.

- poetry init
Interactively create a pyproject.toml file in an existing directory.

- poetry add <package>
Add a dependency to your project.

- poetry remove <package>
Remove a dependency from your project.

- poetry install
Install all dependencies listed in pyproject.toml.

- poetry update
Update dependencies to their latest allowed versions.

- poetry lock
Update the poetry.lock file with the exact versions of dependencies.

- poetry run <command>
Run a command inside the project's virtual environment.

- poetry shell
Spawn a shell within the project's virtual environment.

- poetry build
Build the source and wheel package for distribution.

- poetry publish
Publish the package to PyPI or another repository."""


# Poetry Files Description
"""
- pyproject.toml
The main configuration file for Poetry projects. It contains project metadata (name, version, description, authors),
dependencies, development dependencies, and build system requirements.

- poetry.lock
This file records the exact versions of all dependencies (including sub-dependencies) that were installed. It ensures
that all contributors and deployment environments use the same dependency versions, providing reproducibility.

- .venv/ (optional, local virtual environment)
If enabled, Poetry creates a .venv directory in the project root to isolate dependencies from the global
Python environment."""

