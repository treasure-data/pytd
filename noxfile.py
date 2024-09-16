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
        for python in ("3.8", "3.9", "3.10", "3.11")
        for pandas in ("1.3.5", "1.4.4", "1.5.3", "2.0.3", "2.1.4", "2.2.2")
        if (python, pandas) != ("3.10", "1.3.5")
        if (python, pandas) != ("3.10", "1.4.4")
        if (python, pandas) != ("3.8", "2.0.3")
        if (python, pandas) != ("3.8", "2.1.4")
        if (python, pandas) != ("3.8", "2.2.2")
    ],
)
def tests(session, python, pandas):
    session.install(".[test,spark]")
    session.install(f"pandas=={pandas}")
    session.run("pytest", "-v")
