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


# Example pyproject.toml for a Typical Data Engineering Project:
"""
[tool.poetry]
name = "my-data-engineering-project"
version = "0.1.0"
description = "A sample data engineering project using Poetry"
authors = ["Your Name <your.email@example.com>"]
license = "MIT"

[tool.poetry.dependencies]
python = "^3.10"
pandas = "^2.2.0"
sqlalchemy = "^2.0.0"
pyarrow = "^15.0.0"

[tool.poetry.dev-dependencies]
pytest = "^8.0.0"
black = "^24.0.0"
mypy = "^1.8.0"
isort = "^5.13.0"

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"
"""


# Description of the main blocks you’ll find in the pyproject.toml
"""
[tool.poetry]
This is the main section for Poetry-specific project metadata.
Contains:
name: The project/package name.
version: The current version of the project.
description: A short description of the project.
authors: List of authors (name and email).
license: The license type (e.g., MIT, Apache-2.0).
readme: (Optional) Path to the README file.
homepage, repository, documentation: (Optional) URLs for project resources.
keywords: (Optional) List of keywords for the project.
classifiers: (Optional) List of PyPI classifiers.

[tool.poetry.dependencies]
Lists all the main (runtime) dependencies required for your project to run.
Format:
Each key is a package name, and the value is the version constraint (e.g., pandas = "^2.2.0").
You can specify Python version constraints here as well (e.g., python = "^3.10").

[tool.poetry.dev-dependencies]
Lists development dependencies—packages needed only for development and testing, not for production use.
Examples:
pytest, black, mypy, isort, etc.

[build-system]
Specifies the build backend and requirements for building the project.
Required fields:
requires: List of packages needed to build the project (usually ["poetry-core"]).
build-backend: The build backend to use (for Poetry, typically "poetry.core.masonry.api").


Other Common Blocks in pyproject.toml

[tool.poetry.scripts]
Defines CLI entry points for your project.
Example(toml):
[tool.poetry.scripts]
mycli = "my_package.cli:main"

This allows users to run mycli as a command after installing your package.

[tool.poetry.plugins]
Used to define plugin entry points for extensibility (e.g., for pytest plugins or other frameworks).

[tool.poetry.extras]
Defines optional dependency groups (extras) that users can install with pip install .[extra_name].
Example(toml):
[tool.poetry.extras]
docs = ["sphinx", "mkdocs"]

[tool.<other-tool>]
Other tools (like black, isort, pytest, etc.) can store their configuration in the same pyproject.toml under 
their own [tool.<tool-name>] section.
Examples:
[tool.black] for Black code formatter settings
[tool.isort] for isort import sorting settings
[tool.pytest.ini_options] for pytest configuration
"""


# Steps to Add and Use Poetry in a Python Project
"""
1. Install Poetry
bash:
curl -sSL https://install.python-poetry.org | python3 -

Or follow the latest instructions from the Poetry documentation.


2. Set Up Your Project
You have two options:

a) Create a New Project with Poetry
bash
poetry new my-project

This creates a new directory with a standard structure and a pyproject.toml.

b) Add Poetry to an Existing Project
bash
cd existing-project
poetry init

This interactively creates a pyproject.toml file.

c) Manually Create a pyproject.toml File
You can create the pyproject.toml file yourself in your project root.
Here’s a minimal example:

toml
[tool.poetry]
name = "my-project"
version = "0.1.0"
description = "A sample project"
authors = ["Your Name <your.email@example.com>"]

[tool.poetry.dependencies]
python = "^3.10"

[build-system]
requires = ["poetry-core"]
build-backend = "poetry.core.masonry.api"

After creating the file, run:

bash
poetry install

This will set up the environment and install dependencies based on your manual configuration.

3. Add Dependencies
Add a runtime dependency:
bash
poetry add pandas

Add a development dependency:
bash
poetry add --dev pytest

4. Install Dependencies
bash
poetry install

This installs all dependencies and creates a virtual environment if needed.

5. Activate the Virtual Environment
Spawn a shell within the environment:
bash
poetry shell

Or run a command inside the environment:
bash
poetry run python my_script.py

6. Update Dependencies
bash
poetry update

7. Build and Publish (Optional, for libraries/packages)
Build the package:
bash
poetry build

Publish to PyPI:
bash
poetry publish --username <your-username>

8. Other Useful Commands
Remove a dependency:
bash
poetry remove <package>

Check the dependency tree:
bash
poetry show --tree
"""
