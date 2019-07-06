import nox


@nox.session
def lint(session):
    targets = ["pytd", "setup.py", "noxfile.py"]
    session.install("-e", ".[dev]")
    session.run("flake8", *targets)
    session.run("black", "--diff", "--check", *targets)
    session.run("isort", "--check-only")


@nox.session
def tests(session):
    session.install(".[dev,test,spark]")
    session.run("pytest", "-v")
