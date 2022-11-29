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
        for python in ("3.7", "3.8", "3.9")
        for pandas in ("1.2.5", "1.3.5", "1.4.4", "1.5.2")
        if not (python == "3.7" and pandas in ["1.4.4", "1.5.2"])
        if not (python == "3.9" and pandas in ["1.2.5", "1.3.5", "1.4.4"])
    ],
)
def tests(session, python, pandas):
    session.install(".[test,spark]")
    session.install(f"pandas=={pandas}")
    session.run("pytest", "-v")
