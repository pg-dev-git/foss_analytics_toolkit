"""SF CLI wrapper for managing Salesforce CLI authentication."""

import asyncio
import json
import shutil
import subprocess
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


@dataclass
class SFCLIAuthResult:
    """Result from SF CLI authentication."""
    access_token: str
    instance_url: str
    refresh_token: str | None = None
    expires_at: datetime | None = None
    alias: str = "default"
    username: str | None = None


class SFCLIError(Exception):
    """SF CLI operation error."""
    pass


class SFCLINotFoundError(SFCLIError):
    """SF CLI not installed or not in PATH."""
    pass


class SFCLIManager:
    """Manages SF CLI subprocess calls for authentication."""

    def __init__(self, cli_command: str = "sf"):
        """
        Initialize SF CLI manager.

        Args:
            cli_command: SF CLI command name ('sf' or 'sfdx')
        """
        self.cli_command = cli_command
        self._cli_path: str | None = None

    def is_available(self) -> bool:
        """Check if SF CLI is installed and available."""
        self._cli_path = self._find_cli_path()
        return self._cli_path is not None

    def _find_cli_path(self) -> str | None:
        """Find SF CLI executable path, checking common locations."""
        # First try standard PATH lookup
        path = shutil.which(self.cli_command)
        if path:
            return path

        # On Windows, check common installation locations
        import os
        import sys

        if sys.platform == "win32":
            # Common Windows locations for SF CLI
            possible_paths = [
                # npm global bin
                os.path.expandvars(r"%APPDATA%\npm\sf.cmd"),
                os.path.expandvars(r"%APPDATA%\npm\sfdx.cmd"),
                # Local npm bin
                os.path.expandvars(r"%LOCALAPPDATA%\npm\sf.cmd"),
                os.path.expandvars(r"%LOCALAPPDATA%\npm\sfdx.cmd"),
                # Program Files
                os.path.expandvars(r"%PROGRAMFILES%\Salesforce CLI\bin\sf.cmd"),
                os.path.expandvars(r"%PROGRAMFILES%\Salesforce CLI\bin\sfdx.cmd"),
                os.path.expandvars(r"%PROGRAMFILES(X86)%\Salesforce CLI\bin\sf.cmd"),
                os.path.expandvars(r"%PROGRAMFILES(X86)%\Salesforce CLI\bin\sfdx.cmd"),
                # User profile
                os.path.expandvars(r"%USERPROFILE%\AppData\Local\sf\bin\sf.cmd"),
                os.path.expandvars(r"%USERPROFILE%\AppData\Local\sf\bin\sfdx.cmd"),
            ]

            for p in possible_paths:
                if os.path.exists(p):
                    return p

        # On Unix-like systems, check common locations
        else:
            possible_paths = [
                "/usr/local/bin/sf",
                "/usr/local/bin/sfdx",
                "/opt/sfdx/bin/sf",
                "/opt/sfdx/bin/sfdx",
                os.path.expanduser("~/.local/bin/sf"),
                os.path.expanduser("~/.local/bin/sfdx"),
            ]

            for p in possible_paths:
                if os.path.exists(p):
                    return p

        return None

    def _run_command(self, args: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
        """
        Run SF CLI command synchronously.

        Args:
            args: Command arguments
            timeout: Timeout in seconds

        Returns:
            CompletedProcess result

        Raises:
            SFCLIError: If command fails
            SFCLINotFoundError: If CLI not found
        """
        if not self.is_available():
            raise SFCLINotFoundError(
                f"SF CLI ('{self.cli_command}') not found. "
                "Install from https://developer.salesforce.com/tools/sfdxcli"
            )

        # is_available() guarantees _cli_path is set
        assert self._cli_path is not None
        cmd: list[str] = [self._cli_path, *args]
        logger.debug("running_sf_cli", command=cmd)

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=timeout,
                check=False,
            )
        except subprocess.TimeoutExpired as e:
            raise SFCLIError(f"SF CLI command timed out after {timeout}s: {cmd}") from e
        except FileNotFoundError as e:
            raise SFCLINotFoundError(f"SF CLI not found: {self.cli_command}") from e
        except Exception as e:
            raise SFCLIError(f"Failed to run SF CLI: {e}") from e

        if result.returncode != 0:
            error_msg = result.stderr.strip() or result.stdout.strip()
            # SF CLI sometimes returns non-zero for warnings (e.g., update available)
            # Check if it's just a warning and the command actually succeeded
            if error_msg and "warning" in error_msg.lower() and "update available" in error_msg.lower():
                logger.warning("sf_cli_warning", command=cmd, warning=error_msg)
                # Return the result anyway since the command likely succeeded
                return result
            logger.error("sf_cli_failed", command=cmd, returncode=result.returncode, error=error_msg)
            raise SFCLIError(f"SF CLI failed (exit {result.returncode}): {error_msg}")

        return result

    async def _run_command_async(self, args: list[str], timeout: int = 120) -> subprocess.CompletedProcess:
        """Run SF CLI command asynchronously."""
        if not self.is_available():
            raise SFCLINotFoundError(
                f"SF CLI ('{self.cli_command}') not found. "
                "Install from https://developer.salesforce.com/tools/sfdxcli"
            )

        # is_available() guarantees _cli_path is set
        assert self._cli_path is not None
        cmd: list[str] = [self._cli_path, *args]
        logger.debug("running_sf_cli_async", command=cmd)

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=timeout,
            )
        except TimeoutError as e:
            raise SFCLIError(f"SF CLI command timed out after {timeout}s: {cmd}") from e
        except FileNotFoundError as e:
            raise SFCLINotFoundError(f"SF CLI not found: {self.cli_command}") from e
        except Exception as e:
            raise SFCLIError(f"Failed to run SF CLI: {e}") from e

        result = subprocess.CompletedProcess(
            args=cmd,  # type: ignore[arg-type]
            returncode=process.returncode or 0,
            stdout=stdout.decode() if stdout else "",
            stderr=stderr.decode() if stderr else "",
        )

        if result.returncode != 0:
            error_msg = result.stderr.strip() or result.stdout.strip()
            # SF CLI sometimes returns non-zero for warnings (e.g., update available)
            # Check if it's just a warning and the command actually succeeded
            if error_msg and "warning" in error_msg.lower() and "update available" in error_msg.lower():
                logger.warning("sf_cli_warning", command=cmd, warning=error_msg)
                # Return the result anyway since the command likely succeeded
                return result
            logger.error("sf_cli_failed", command=cmd, returncode=result.returncode, error=error_msg)
            raise SFCLIError(f"SF CLI failed (exit {result.returncode}): {error_msg}")

        return result

    async def login_web(
        self,
        alias: str = "default",
        instance_url: str | None = None,
        timeout: int = 300,
    ) -> SFCLIAuthResult:
        """
        Run SF CLI web login flow.

        Args:
            alias: Org alias to use
            instance_url: Optional custom instance URL (e.g., https://mydomain.my.salesforce.com)
            timeout: Timeout in seconds for the login flow

        Returns:
            SFCLIAuthResult with authentication details
        """
        args = ["org", "login", "web", "--alias", alias, "--json"]
        if instance_url:
            args.extend(["--instance-url", instance_url])

        logger.info("starting_sf_cli_web_login", alias=alias, instance_url=instance_url)

        result = await self._run_command_async(args, timeout=timeout)

        # Parse the JSON output
        try:
            output = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            raise SFCLIError(f"Failed to parse SF CLI JSON output: {e}") from e

        # The login command returns the org info directly
        if output.get("status") != 0:
            raise SFCLIError(f"Login failed: {output.get('message', 'Unknown error')}")

        result_data = output.get("result", {})
        return self._parse_auth_result(result_data, alias)

    async def login_device(
        self,
        alias: str = "default",
        instance_url: str | None = None,
        timeout: int = 300,
    ) -> SFCLIAuthResult:
        """
        Run SF CLI device login flow (headless / no browser).

        Args:
            alias: Org alias to use
            instance_url: Optional custom instance URL
            timeout: Timeout in seconds for the login flow

        Returns:
            SFCLIAuthResult with authentication details
        """
        args = ["org", "login", "device", "--alias", alias, "--json"]
        if instance_url:
            args.extend(["--instance-url", instance_url])

        logger.info("starting_sf_cli_device_login", alias=alias, instance_url=instance_url)

        result = await self._run_command_async(args, timeout=timeout)

        try:
            output = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            raise SFCLIError(f"Failed to parse SF CLI JSON output: {e}") from e

        if output.get("status") != 0:
            raise SFCLIError(
                f"Device login failed: {output.get('message', 'Unknown error')}"
            )

        result_data = output.get("result", {})
        return self._parse_auth_result(result_data, alias)

    async def get_org_info(self, alias: str = "default") -> SFCLIAuthResult:
        """
        Get org info including access token.

        Args:
            alias: Org alias

        Returns:
            SFCLIAuthResult with current auth details
        """
        args = ["org", "display", "--target-org", alias, "--json"]

        result = await self._run_command_async(args)

        try:
            output = json.loads(result.stdout)
        except json.JSONDecodeError as e:
            raise SFCLIError(f"Failed to parse SF CLI JSON output: {e}") from e

        if output.get("status") != 0:
            raise SFCLIError(f"Failed to get org info: {output.get('message', 'Unknown error')}")

        result_data = output.get("result", {})
        return self._parse_auth_result(result_data, alias)

    async def refresh_token(self, alias: str = "default") -> SFCLIAuthResult:
        """
        Force token refresh by getting org info (SF CLI handles refresh internally).

        Args:
            alias: Org alias

        Returns:
            SFCLIAuthResult with refreshed auth details
        """
        # SF CLI automatically refreshes tokens when running org display
        return await self.get_org_info(alias)

    async def logout(self, alias: str = "default") -> None:
        """
        Logout and remove org from SF CLI.

        Args:
            alias: Org alias to remove
        """
        args = ["org", "logout", "--target-org", alias, "--json", "--no-prompt"]
        await self._run_command_async(args)
        logger.info("sf_cli_logout", alias=alias)

    def _parse_auth_result(self, data: dict[str, Any], alias: str) -> SFCLIAuthResult:
        """Parse SF CLI auth result into SFCLIAuthResult."""
        access_token = data.get("accessToken")
        instance_url = data.get("instanceUrl")
        refresh_token = data.get("refreshToken")
        username = data.get("username")

        # Parse expiration if available. SF CLI's `org display --json` returns
        # the field as `expirationDate`; older sfdx and some custom forks
        # used `tokenExpiration`. Try the real field name first, then the
        # legacy name as a fallback. Without a valid timestamp the
        # StoredToken is saved with expires_at=None, which is treated as
        # "unknown expiry, trust SF CLI" (see StoredToken.is_expired).
        expires_at: datetime | None = None
        for field in ("expirationDate", "tokenExpiration"):
            raw = data.get(field)
            if not raw:
                continue
            try:
                expires_at = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
                break
            except (ValueError, TypeError):
                continue

        if not access_token or not instance_url:
            raise SFCLIError("Missing access token or instance URL in SF CLI response")

        return SFCLIAuthResult(
            access_token=access_token,
            instance_url=instance_url.rstrip("/"),
            refresh_token=refresh_token,
            expires_at=expires_at,
            alias=alias,
            username=username,
        )

    def list_orgs(self) -> list[Any]:
        """List all authorized orgs."""
        result = self._run_command(["org", "list", "--json", "--all"])
        try:
            output = json.loads(result.stdout)
            return output.get("result", {}).get("orgs", [])  # type: ignore[no-any-return]
        except (json.JSONDecodeError, KeyError):
            return []
