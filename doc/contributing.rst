Contributing to pytd
====================

Development Setup
-----------------

This project uses `uv <https://docs.astral.sh/uv/>`__ for fast and reliable Python package management.

Installing uv
~~~~~~~~~~~~~

.. code:: sh

   # On macOS and Linux:
   curl -LsSf https://astral.sh/uv/install.sh | sh

   # On Windows:
   powershell -c "irm https://astral.sh/uv/install.ps1 | iex"

   # Or via pip:
   pip install uv

Setting Up Development Environment
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code:: sh

   # Clone the repository
   git clone https://github.com/treasure-data/pytd.git
   cd pytd

   # Sync dependencies (installs everything from uv.lock)
   uv sync

   # Run commands in the uv-managed environment
   uv run python --version
   uv run pytest

Code Formatting and Testing
----------------------------

We use `ruff <https://docs.astral.sh/ruff/>`__ for linting and formatting,
and `pyright <https://github.com/microsoft/pyright>`__ for type checking.

We use `nox <https://nox.thea.codes/>`__ to run tests and checks. Nox is configured
to use uv as the backend for fast virtual environment creation and package installation.

You can run formatter, linter, and tests by using nox:

.. code:: sh

   # Run all sessions
   uvx nox

   # Run specific sessions
   uvx nox --session lint
   uvx nox --session typecheck
   uvx nox --session tests

Pre-commit Hooks
~~~~~~~~~~~~~~~~

We highly recommend you to introduce
`pre-commit <https://pre-commit.com/>`__ to ensure your commit follows
required format.

You can install and set up pre-commit as follows:

.. code:: sh

   uvx pre-commit install

Now, ruff and other checks will run each time you commit changes.
You can skip these checks with ``git commit --no-verify``.

Documenting
-----------

.. code:: sh

   pip install .[doc]

Edit contents in ``doc/``:

.. code:: sh

   cd doc

Build HTML files to render Sphinx documentation:

.. code:: sh

   make html

The ``doc/`` folder is monitored and automatically published by `Read the Docs <https://readthedocs.org/projects/pytd-doc/>`__.

Releasing
---------

Update version in ``pyproject.toml``. Set it to ``1.0.0``, for example:

.. code:: toml

   [project]
   name = "pytd"
   version = "1.0.0"

Commit and push the latest code, and tag the version:

.. code:: sh

   git tag 1.0.0
   git push --tags

`GitHub Actions Workflow <https://github.com/treasure-data/pytd/blob/master/.github/workflows/pypi.yml>`__ then automatically releases the tagged version on PyPI. A tag and version number must be identical and following the `semantic versioning convention <https://semver.org/>`__.
