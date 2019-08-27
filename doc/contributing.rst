Contributing to pytd
====================

Code Formatting and Testing
---------------------------

We use `black <https://black.readthedocs.io/en/stable/>`__ and
`isort <https://github.com/timothycrosley/isort>`__ as a formatter, and
`flake8 <http://flake8.pycqa.org/en/latest/>`__ as a linter. Our CI
checks format with them.

Note that black requires Python 3.6+ while pytd supports 3.5+, so you
must need to have Python 3.6+ for development.

We highly recommend you to introduce
`pre-commit <https://pre-commit.com/>`__ to ensure your commit follows
required format.

You can install pre-commit as follows:

.. code:: sh

   pip install pre-commit
   pre-commit install

Now, black, isort, and flake8 will check each time you commit changes.
You can skip these check with ``git commit --no-verify``.

If you want to check code format manually, you can install them as
follows:

.. code:: sh

   pip install black isort flake8

Then, you can run those tool manually;

.. code:: sh

   black pytd
   flake8 pytd
   isort

You can run formatter, linter, and test by using nox as the following:

.. code:: sh

   pip install nox # You should install at the first time
   nox

Documenting
-----------

.. code:: sh

   pip install sphinx numpydoc ghp-import

Edit contents in ``doc/``:

.. code:: sh

   cd doc

Build HTML files and push them to the ``gh-pages`` branch:

.. code:: sh

   git checkout master  # make sure doc is built under the latest version
   make html
   ghp-import _build/html -p
