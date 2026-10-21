def test_diag():
    import os
    import rich.console
    from typer.testing import CliRunner

    from asftool.cli.main import app

    c = rich.console.Console()
    r = CliRunner().invoke(app, ["doctor", "--help"])
    print("CI_DIAG cols=", os.environ.get("COLUMNS"), "lines=", os.environ.get("LINES"))
    print("CI_DIAG term=", os.environ.get("TERM"))
    print("CI_DIAG richwidth=", c.width, "richheight=", c.height)
    print("CI_DIAG stdoutlen=", len(r.stdout))
    print("CI_DIAG stdoutrepr=", repr(r.stdout[:200]))
