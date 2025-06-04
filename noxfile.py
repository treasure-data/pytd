import nox


@nox.session
def lint(session):
    lint_tools = ["black", "isort", "flake8"]
    targets = ["pytd", "setup.py", "noxfile.py"]
    session.install(*lint_tools)
    session.run("flake8", *targets)
    session.run("black", "--diff", "--check", *targets)
    session.run("isort", "--diff", "--check", *targets)


@nox.session
@nox.parametrize(
    "python,pandas",
    [
        (python, pandas)
        for python in ("3.9", "3.10", "3.11", "3.12", "3.13")
        for pandas in ("2.0.3", "2.1.4", "2.2.3")
        # Skip combinations that don't support each other
        if not (python in ("3.9",) and pandas in ("2.2.3",))
    ],
)
def tests(session, python, pandas):
    session.install(".[test,spark]")
    session.install(f"pandas=={pandas}")
    session.run("pytest", "-v")
