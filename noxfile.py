import nox


@nox.session
def lint(session):
    session.install("ruff")
    targets = ["pytd", "noxfile.py"]
    session.run("ruff", "check", *targets)
    session.run("ruff", "format", "--diff", "--check", *targets)


@nox.session
def typecheck(session):
    session.install(".[dev]")
    session.run("pyright", "pytd")


@nox.session
@nox.parametrize(
    "python,pandas",
    [
        (python, pandas)
        for python in ("3.10", "3.11", "3.12", "3.13", "3.14")
        for pandas in ("2.3.3",)
    ],
)
def tests(session, python, pandas):
    session.install(".[test,spark]")
    session.install(f"pandas=={pandas}")

    # Set mock environment variables for testing
    session.env.update(
        {"TD_API_KEY": "1/test-key", "TD_API_SERVER": "https://api.treasure-data.com/"}
    )

    session.run("pytest", "-v")
