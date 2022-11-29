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
@nox.parametrize("pandas", ["1.2.0", "1.5.0"])
def tests(session, pandas):
    session.install(".[test,spark]")
    session.install(f"pandas=={pandas}")
    session.run("pytest", "-v")
