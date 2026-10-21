import pathlib


def test_diag():
    import os
    import rich.console
    from typer.testing import CliRunner

    from asftool.cli.main import app

    c = rich.console.Console()
    r = CliRunner().invoke(app, ["doctor", "--help"])
    lines = [
        f"cols={os.environ.get('COLUMNS')}",
        f"lines={os.environ.get('LINES')}",
        f"term={os.environ.get('TERM')}",
        f"richwidth={c.width}",
        f"richheight={c.height}",
        f"stdoutlen={len(r.stdout)}",
        f"stdoutrepr={r.stdout[:300]!r}",
        f"has_alias={'--alias' in r.stdout}",
    ]
    pathlib.Path("/tmp/ci_diag.txt").write_text("\n".join(lines))
    assert True
