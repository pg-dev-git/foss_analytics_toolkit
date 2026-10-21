"""Tests that ASFTool's environment does not leak into SF CLI subprocesses.

`SF_API_VERSION` is read by ASFTool to choose the org's API version. The SF
CLI also reads it as an override, and when the value is not supported for
the org the CLI fails to resolve the org at all, reporting
"No authorization information found for <alias>" -- a credential error the
user cannot diagnose, caused by their own settings.
"""

import json
import subprocess

import pytest

from asftool.core.sf_cli import SFCLIManager, sf_cli_env


class TestSfCliEnv:
    def test_removes_sf_api_version(self, monkeypatch):
        monkeypatch.setenv("SF_API_VERSION", "v68.0")
        assert "SF_API_VERSION" not in sf_cli_env()

    def test_keeps_everything_else(self, monkeypatch):
        monkeypatch.setenv("SF_API_VERSION", "v68.0")
        monkeypatch.setenv("SOME_OTHER_VAR", "keep-me")
        env = sf_cli_env()
        assert env["SOME_OTHER_VAR"] == "keep-me"

    def test_inherits_the_rest_of_the_environment(self, monkeypatch):
        monkeypatch.setenv("PATH", "/usr/bin")
        assert sf_cli_env()["PATH"] == "/usr/bin"


class TestRunCommandEnv:
    def test_sync_run_does_not_pass_sf_api_version(self, monkeypatch):
        """The subprocess must not inherit ASFTool's API version override."""
        monkeypatch.setenv("SF_API_VERSION", "v68.0")
        captured: dict[str, object] = {}

        def fake_run(cmd, **kwargs):
            captured.update(kwargs)
            stdout = json.dumps(
                {"status": 0, "result": {"other": [], "nonScratchOrgs": []}}
            )
            return subprocess.CompletedProcess(cmd, 0, stdout=stdout, stderr="")

        monkeypatch.setattr(subprocess, "run", fake_run)

        manager = SFCLIManager()
        manager._cli_path = "/usr/bin/sf"
        manager.list_orgs()

        env = captured.get("env")
        assert env is not None, "subprocess.run was called without an explicit env"
        assert "SF_API_VERSION" not in env

    @pytest.mark.asyncio
    async def test_async_run_does_not_pass_sf_api_version(self, monkeypatch):
        """The async path is the one the interactive menu uses."""
        monkeypatch.setenv("SF_API_VERSION", "v68.0")
        captured: dict[str, object] = {}

        class FakeProcess:
            returncode = 0

            async def communicate(self):
                return b"{}", b""

        async def fake_exec(*args, **kwargs):
            captured.update(kwargs)
            return FakeProcess()

        monkeypatch.setattr("asyncio.create_subprocess_exec", fake_exec)

        manager = SFCLIManager()
        manager._cli_path = "/usr/bin/sf"
        await manager._run_command_async(["org", "list", "--json", "--all"])

        env = captured.get("env")
        assert env is not None, "create_subprocess_exec was called without an explicit env"
        assert "SF_API_VERSION" not in env
