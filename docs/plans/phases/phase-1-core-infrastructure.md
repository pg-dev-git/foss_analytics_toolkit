# Phase 1: Core Infrastructure

**Document**: `docs/plans/phases/phase-1-core-infrastructure.md`  
**Duration**: 1 week  
**Branch**: `feature/phase-1-core-infrastructure` (to be created when implementation begins)  
**Depends on**: Phase 0 complete  
**Status**: ✅ Completed

---

## 🎯 Objective

Build the core infrastructure for the Interactive TUI:
- `SessionManager` — wraps `SFCLIAuthService` for persistent multi-org sessions
- `SafetyMonitor` — VPN/Proxy detection with hard-block on critical risk
- `TCRMApp` — Main Textual App with layout (sidebar, content, detail, status bar)
- Login screen with SF CLI web flow
- Org switcher (Ctrl+O)
- Status bar with safety indicator (🟢/🟡/🔴)

---

## 📋 Explicit Requirements

### 1. SessionManager

**File**: `tcrm_toolkit/interactive/session.py`

```python
"""Session management for Interactive TUI - wraps SFCLIAuthService."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Optional

from tcrm_toolkit.core.auth import SFCLIAuthService, SFCLIAuthError
from tcrm_toolkit.core.client import SalesforceClient, create_client
from tcrm_toolkit.core.config import Settings, get_settings
from tcrm_toolkit.core.crypto import CryptoManager, create_crypto_manager
from tcrm_toolkit.interactive.safety import SafetyMonitor, SafetyError


@dataclass
class OrgSession:
    """Represents an authenticated org session."""
    alias: str
    username: Optional[str]
    instance_url: str
    is_default: bool = False


class SessionManager:
    """
    Manages authenticated sessions across the TUI lifecycle.
    
    Responsibilities:
    - Multi-org credential management via SF CLI aliases
    - Auto-refresh tokens before expiry
    - Session persistence across TUI restarts
    - Quick org switching (Ctrl+O)
    - Safety gate: blocks client creation if VPN/Proxy detected
    """
    
    def __init__(
        self,
        settings: Settings | None = None,
        crypto: CryptoManager | None = None,
        safety_monitor: SafetyMonitor | None = None,
    ):
        self.settings = settings or get_settings()
        self.crypto = crypto or create_crypto_manager()
        self.safety = safety_monitor or SafetyMonitor(self.settings)
        self._auth_service: SFCLIAuthService | None = None
        self._current_alias: str = "default"
        self._client: SalesforceClient | None = None
        self._org_sessions: dict[str, OrgSession] = {}
    
    @property
    def auth_service(self) -> SFCLIAuthService:
        """Lazy-initialize SFCLIAuthService."""
        if self._auth_service is None:
            self._auth_service = SFCLIAuthService(
                settings=self.settings,
                crypto_manager=self.crypto,
            )
        return self._auth_service
    
    @property
    def current_alias(self) -> str:
        return self._current_alias
    
    @property
    def current_org(self) -> OrgSession | None:
        return self._org_sessions.get(self._current_alias)
    
    async def initialize(self) -> None:
        """Initialize session - load orgs, check safety, auto-login if needed."""
        # Check connection safety first
        safety_result = await self.safety.check_connection_safety()
        if not safety_result.is_safe and self.settings.safety_block_on_critical:
            raise SafetyError(f"Unsafe connection: {safety_result.details}")
        
        # Load available orgs from SF CLI
        await self.refresh_org_list()
        
        # Try to get valid token for current/default org
        try:
            await self.ensure_valid_token()
        except SFCLIAuthError:
            # No valid token - will need login
            pass
    
    async def refresh_org_list(self) -> list[OrgSession]:
        """Refresh list of authorized orgs from SF CLI."""
        orgs = await self.auth_service.list_orgs()
        self._org_sessions = {}
        
        for org in orgs:
            alias = org.get("alias", "unknown")
            username = org.get("username")
            instance_url = org.get("instanceUrl", "")
            connected = org.get("connectedStatus") == "Connected"
            
            if connected and username and instance_url:
                self._org_sessions[alias] = OrgSession(
                    alias=alias,
                    username=username,
                    instance_url=instance_url.rstrip("/"),
                    is_default=(alias == "default"),
                )
        
        return list(self._org_sessions.values())
    
    async def ensure_valid_token(self, alias: str | None = None) -> str:
        """
        Get valid access token for alias, auto-refresh if needed.
        
        Args:
            alias: Org alias (defaults to current)
            
        Returns:
            Valid access token
            
        Raises:
            SFCLIAuthError: If no valid token available
            SafetyError: If connection safety check fails
        """
        alias = alias or self._current_alias
        
        # Safety check before any API call
        safety_result = await self.safety.check_connection_safety()
        if not safety_result.is_safe and self.settings.safety_block_on_critical:
            raise SafetyError(f"Unsafe connection: {safety_result.details}")
        
        token = await self.auth_service.get_access_token(alias=alias, auto_refresh=True)
        return token
    
    async def get_client(self, alias: str | None = None) -> SalesforceClient:
        """
        Get authenticated SalesforceClient for alias.
        
        Creates new client if alias changed or client expired.
        """
        alias = alias or self._current_alias
        
        # Safety gate
        safety_result = await self.safety.check_connection_safety()
        if not safety_result.is_safe and self.settings.safety_block_on_critical:
            raise SafetyError(f"Unsafe connection: {safety_result.details}")
        
        # Get token and instance URL
        access_token = await self.ensure_valid_token(alias)
        instance_url = await self.auth_service.get_instance_url(alias)
        
        # Create client
        self._client = SalesforceClient(
            access_token=access_token,
            instance_url=instance_url,
            settings=self.settings,
        )
        
        return self._client
    
    @asynccontextmanager
    async def client_context(self, alias: str | None = None):
        """Context manager for SalesforceClient with auto-cleanup."""
        client = await self.get_client(alias)
        try:
            yield client
        finally:
            await client.close()
            self._client = None
    
    async def switch_org(self, alias: str) -> OrgSession:
        """Switch to different org alias."""
        if alias not in self._org_sessions:
            await self.refresh_org_list()
        
        if alias not in self._org_sessions:
            raise SFCLIAuthError(f"Org alias '{alias}' not found. Run 'sf org list' first.")
        
        self._current_alias = alias
        self._client = None  # Force new client on next use
        
        # Verify token works for new org
        await self.ensure_valid_token(alias)
        
        return self._org_sessions[alias]
    
    async def login(self, alias: str = "default", instance_url: str | None = None) -> str:
        """Run SF CLI web login flow."""
        token = await self.auth_service.login(alias=alias, instance_url=instance_url)
        await self.refresh_org_list()
        self._current_alias = alias
        return token
    
    async def logout(self, alias: str = "default") -> bool:
        """Logout and remove stored auth for alias."""
        result = await self.auth_service.logout(alias)
        await self.refresh_org_list()
        
        if alias == self._current_alias:
            self._current_alias = "default"
            self._client = None
        
        return result
    
    async def get_status(self, alias: str | None = None) -> dict:
        """Get authentication status for alias."""
        alias = alias or self._current_alias
        return await self.auth_service.status(alias)
    
    def list_orgs(self) -> list[OrgSession]:
        """List all known org sessions."""
        return list(self._org_sessions.values())
    
    async def close(self) -> None:
        """Cleanup resources."""
        if self._client:
            await self._client.close()
            self._client = None
        if self._auth_service:
            await self._auth_service.close()
            self._auth_service = None
```

---

### 2. SafetyMonitor (VPN/Proxy Detection)

**File**: `tcrm_toolkit/interactive/safety.py`

```python
"""Connection safety monitor - detects VPN/Proxy that trigger Salesforce blocks."""

import asyncio
import json
import os
import platform
import subprocess
import socket
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Literal
from urllib.parse import urlparse

import httpx
import structlog

from tcrm_toolkit.core.config import Settings, get_settings
from tcrm_toolkit.core.platform import is_windows, is_linux, is_macos

logger = structlog.get_logger(__name__)


class RiskLevel(str, Enum):
    SAFE = "safe"
    WARNING = "warning"
    CRITICAL = "critical"


class CheckName(str, Enum):
    IP_REPUTATION = "ip_reputation"
    VPN_INTERFACES = "vpn_interfaces"
    SYSTEM_PROXY = "system_proxy"
    DNS_LEAK = "dns_leak"


@dataclass
class CheckResult:
    name: CheckName
    passed: bool
    details: str
    remediation: str | None = None
    risk_level: RiskLevel = RiskLevel.SAFE


@dataclass
class SafetyResult:
    is_safe: bool
    checks: dict[CheckName, CheckResult] = field(default_factory=dict)
    risk_level: RiskLevel = RiskLevel.SAFE
    details: str = ""
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        # Determine overall risk level
        if any(c.risk_level == RiskLevel.CRITICAL for c in self.checks.values()):
            self.risk_level = RiskLevel.CRITICAL
            self.is_safe = False
        elif any(c.risk_level == RiskLevel.WARNING for c in self.checks.values()):
            self.risk_level = RiskLevel.WARNING
            self.is_safe = True  # Warning doesn't block
        else:
            self.risk_level = RiskLevel.SAFE
            self.is_safe = True
        
        # Build details string
        failed = [c for c in self.checks.values() if not c.passed]
        if failed:
            self.details = "; ".join(f"{c.name.value}: {c.details}" for c in failed)


class SafetyError(Exception):
    """Raised when safety check fails and blocking is enabled."""
    pass


class SafetyMonitor:
    """
    Monitors connection for VPN/Proxy that could trigger Salesforce blocks.
    
    Salesforce now IMMEDIATELY disables users detected on VPN/Proxy.
    This monitor runs on startup and periodically in background.
    
    Detection Methods:
    1. IP Reputation (ipapi.co) - VPN, Proxy, Tor, Hosting detection
    2. VPN Interfaces - Local network interface scanning
    3. System Proxy - OS proxy settings detection
    4. DNS Leak - DNS resolution consistency check
    """
    
    # Known VPN interface prefixes
    VPN_INTERFACE_PREFIXES = (
        "tun", "tap", "wg", "vpn", "wireguard", 
        "ppp", "ipsec", "sslvpn", "openvpn",
        "nordlynx", "proton", "mullvad", "expressvpn",
    )
    
    # IP reputation service
    IP_API_URL = "https://ipapi.co/json/"
    IP_API_TIMEOUT = 10.0
    
    def __init__(self, settings: Settings | None = None):
        self.settings = settings or get_settings()
        self._cache: SafetyResult | None = None
        self._cache_expires: datetime | None = None
        self._monitor_task: asyncio.Task | None = None
        self._http_client: httpx.AsyncClient | None = None
    
    @property
    def http_client(self) -> httpx.AsyncClient:
        if self._http_client is None:
            self._http_client = httpx.AsyncClient(
                timeout=httpx.Timeout(self.IP_API_TIMEOUT),
                follow_redirects=True,
            )
        return self._http_client
    
    async def close(self) -> None:
        if self._http_client:
            await self._http_client.aclose()
            self._http_client = None
        if self._monitor_task:
            self._monitor_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._monitor_task
    
    def _is_cache_valid(self) -> bool:
        if not self._cache or not self._cache_expires:
            return False
        return datetime.utcnow() < self._cache_expires
    
    async def check_connection_safety(self, force: bool = False) -> SafetyResult:
        """
        Run all safety checks and return combined result.
        
        Args:
            force: Skip cache and run fresh checks
            
        Returns:
            SafetyResult with overall safety status
        """
        if not force and self._is_cache_valid():
            return self._cache
        
        logger.info("running_safety_checks")
        
        # Run all checks in parallel
        results = await asyncio.gather(
            self._check_ip_reputation(),
            self._check_vpn_interfaces(),
            self._check_system_proxy(),
            self._check_dns_leak(),
            return_exceptions=True,
        )
        
        checks = {}
        for i, result in enumerate(results):
            check_name = list(CheckName)[i]
            if isinstance(result, Exception):
                logger.error("safety_check_failed", check=check_name.value, error=str(result))
                checks[check_name] = CheckResult(
                    name=check_name,
                    passed=True,  # Fail open for check errors
                    details=f"Check failed: {result}",
                    risk_level=RiskLevel.SAFE,
                )
            else:
                checks[check_name] = result
        
        safety_result = SafetyResult(checks=checks)
        
        # Cache result
        self._cache = safety_result
        self._cache_expires = datetime.utcnow() + timedelta(
            seconds=self.settings.safety_check_interval
        )
        
        logger.info(
            "safety_check_complete",
            is_safe=safety_result.is_safe,
            risk_level=safety_result.risk_level.value,
            details=safety_result.details,
        )
        
        return safety_result
    
    async def _check_ip_reputation(self) -> CheckResult:
        """Check IP reputation via ipapi.co."""
        # Check allowlist first
        if self.settings.safety_allowlist_ips:
            try:
                current_ip = await self._get_current_ip()
                if current_ip in self.settings.safety_allowlist_ips:
                    return CheckResult(
                        name=CheckName.IP_REPUTATION,
                        passed=True,
                        details=f"IP {current_ip} in allowlist",
                    )
            except Exception:
                pass
        
        try:
            response = await self.http_client.get(self.IP_API_URL)
            response.raise_for_status()
            data = response.json()
            
            # Check security fields
            security = data.get("security", {})
            is_vpn = security.get("vpn", False)
            is_proxy = security.get("proxy", False)
            is_tor = security.get("tor", False)
            is_hosting = security.get("hosting", False)
            is_relay = security.get("relay", False)
            
            ip = data.get("ip", "unknown")
            country = data.get("country_name", "unknown")
            
            if is_vpn or is_proxy or is_tor:
                return CheckResult(
                    name=CheckName.IP_REPUTATION,
                    passed=False,
                    details=f"IP {ip} ({country}): VPN={is_vpn}, Proxy={is_proxy}, Tor={is_tor}, Hosting={is_hosting}",
                    remediation="Disconnect VPN/Proxy and retry. Use allowlist for trusted IPs.",
                    risk_level=RiskLevel.CRITICAL,
                )
            
            if is_hosting or is_relay:
                return CheckResult(
                    name=CheckName.IP_REPUTATION,
                    passed=False,
                    details=f"IP {ip} ({country}): Hosting={is_hosting}, Relay={is_relay} (may trigger blocks)",
                    remediation="Consider using residential IP. Add to allowlist if trusted.",
                    risk_level=RiskLevel.WARNING,
                )
            
            return CheckResult(
                name=CheckName.IP_REPUTATION,
                passed=True,
                details=f"IP {ip} ({country}): Clean",
            )
            
        except httpx.TimeoutException:
            return CheckResult(
                name=CheckName.IP_REPUTATION,
                passed=True,  # Fail open on timeout
                details="IP reputation check timed out",
                risk_level=RiskLevel.SAFE,
            )
        except Exception as e:
            return CheckResult(
                name=CheckName.IP_REPUTATION,
                passed=True,  # Fail open on error
                details=f"IP reputation check failed: {e}",
                risk_level=RiskLevel.SAFE,
            )
    
    async def _get_current_ip(self) -> str:
        """Get current public IP."""
        try:
            response = await self.http_client.get("https://api.ipify.org?format=json")
            response.raise_for_status()
            return response.json().get("ip", "unknown")
        except Exception:
            return "unknown"
    
    async def _check_vpn_interfaces(self) -> CheckResult:
        """Scan for VPN network interfaces."""
        vpn_interfaces = []
        
        try:
            if is_windows():
                # Windows: use GetAdaptersAddresses via PowerShell
                cmd = [
                    "powershell", "-Command",
                    "Get-NetAdapter | Where-Object {$_.InterfaceDescription -match 'VPN|TAP|TUN|WireGuard|OpenVPN|NordLynx|Proton|Mullvad|ExpressVPN'} | Select-Object Name, InterfaceDescription | ConvertTo-Json"
                ]
                result = await asyncio.create_subprocess_exec(
                    *cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                )
                stdout, _ = await result.communicate()
                if stdout:
                    adapters = json.loads(stdout.decode())
                    if not isinstance(adapters, list):
                        adapters = [adapters]
                    for adapter in adapters:
                        name = adapter.get("Name", "")
                        desc = adapter.get("InterfaceDescription", "")
                        vpn_interfaces.append(f"{name} ({desc})")
            else:
                # Linux/macOS: scan /sys/class/net
                net_path = Path("/sys/class/net")
                if net_path.exists():
                    for iface in net_path.iterdir():
                        iface_name = iface.name.lower()
                        if any(iface_name.startswith(prefix) for prefix in self.VPN_INTERFACE_PREFIXES):
                            # Get interface details
                            try:
                                operstate = (iface / "operstate").read_text().strip()
                                if operstate == "up":
                                    vpn_interfaces.append(f"{iface.name} (up)")
                            except Exception:
                                vpn_interfaces.append(iface.name)
        except Exception as e:
            logger.debug("vpn_interface_scan_failed", error=str(e))
        
        if vpn_interfaces:
            return CheckResult(
                name=CheckName.VPN_INTERFACES,
                passed=False,
                details=f"VPN interfaces detected: {', '.join(vpn_interfaces)}",
                remediation="Disconnect VPN and retry. Check 'ip link' or 'Get-NetAdapter'.",
                risk_level=RiskLevel.CRITICAL,
            )
        
        return CheckResult(
            name=CheckName.VPN_INTERFACES,
            passed=True,
            details="No VPN interfaces detected",
        )
    
    async def _check_system_proxy(self) -> CheckResult:
        """Check system proxy settings."""
        proxy_vars = [
            "http_proxy", "https_proxy", "ftp_proxy", "all_proxy",
            "HTTP_PROXY", "HTTPS_PROXY", "FTP_PROXY", "ALL_PROXY",
        ]
        
        set_proxies = {var: os.environ.get(var) for var in proxy_vars if os.environ.get(var)}
        
        # Also check OS-specific proxy settings
        os_proxy = ""
        if is_windows():
            try:
                cmd = ["powershell", "-Command", "Get-ItemProperty 'HKCU:\\Software\\Microsoft\\Windows\\CurrentVersion\\Internet Settings' | Select-Object ProxyEnable, ProxyServer | ConvertTo-Json"]
                result = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE)
                stdout, _ = await result.communicate()
                if stdout:
                    data = json.loads(stdout.decode())
                    if data.get("ProxyEnable") == 1:
                        os_proxy = f"Windows Proxy: {data.get('ProxyServer', 'enabled')}"
            except Exception:
                pass
        elif is_macos():
            try:
                result = await asyncio.create_subprocess_exec(
                    "networksetup", "-getwebproxy", "Wi-Fi",
                    stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                stdout, _ = await result.communicate()
                output = stdout.decode()
                if "Enabled: Yes" in output:
                    os_proxy = f"macOS Proxy: {output}"
            except Exception:
                pass
        elif is_linux():
            # Check GNOME/KDE proxy settings
            for cmd in [
                ["gsettings", "get", "org.gnome.system.proxy", "mode"],
                ["kreadconfig5", "--group", "Proxy Settings", "--key", "ProxyType"],
            ]:
                try:
                    result = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE)
                    stdout, _ = await result.communicate()
                    if stdout and b"none" not in stdout.lower() and b"0" not in stdout:
                        os_proxy = f"Linux Proxy: {stdout.decode().strip()}"
                        break
                except Exception:
                    pass
        
        all_proxies = {**set_proxies}
        if os_proxy:
            all_proxies["os_proxy"] = os_proxy
        
        if all_proxies:
            details = "; ".join(f"{k}={v}" for k, v in all_proxies.items())
            return CheckResult(
                name=CheckName.SYSTEM_PROXY,
                passed=False,
                details=f"System proxy configured: {details}",
                remediation="Disable system proxy or use allowlist. Check environment variables and OS network settings.",
                risk_level=RiskLevel.WARNING,  # Proxy env vars don't always mean active proxy
            )
        
        return CheckResult(
            name=CheckName.SYSTEM_PROXY,
            passed=True,
            details="No system proxy detected",
        )
    
    async def _check_dns_leak(self) -> CheckResult:
        """Check for DNS leaks by comparing system DNS vs known clean DNS."""
        try:
            # Resolve via system DNS
            system_ips = await asyncio.get_event_loop().getaddrinfo(
                "whoami.akamai.net", None, family=socket.AF_INET
            )
            system_ip = system_ips[0][4][0] if system_ips else None
            
            # Resolve via clean DNS (1.1.1.1) using httpx
            async with httpx.AsyncClient(timeout=5.0) as client:
                response = await client.get("https://1.1.1.1/dns-query?name=whoami.akamai.net&type=A", 
                                           headers={"Accept": "application/dns-json"})
                response.raise_for_status()
                data = response.json()
                clean_ip = data.get("Answer", [{}])[0].get("data") if data.get("Answer") else None
            
            if system_ip and clean_ip and system_ip != clean_ip:
                return CheckResult(
                    name=CheckName.DNS_LEAK,
                    passed=False,
                    details=f"DNS leak detected: System={system_ip}, Clean={clean_ip}",
                    remediation="Check DNS settings. VPN may be leaking DNS. Use VPN with DNS leak protection.",
                    risk_level=RiskLevel.WARNING,
                )
            
            return CheckResult(
                name=CheckName.DNS_LEAK,
                passed=True,
                details=f"DNS consistent: {system_ip or clean_ip}",
            )
        except Exception as e:
            logger.debug("dns_leak_check_failed", error=str(e))
            return CheckResult(
                name=CheckName.DNS_LEAK,
                passed=True,  # Fail open
                details=f"DNS leak check failed: {e}",
                risk_level=RiskLevel.SAFE,
            )
    
    async def start_monitoring(self, callback=None, interval: int | None = None) -> None:
        """Start background monitoring task."""
        interval = interval or self.settings.safety_check_interval
        
        async def monitor_loop():
            while True:
                try:
                    result = await self.check_connection_safety(force=True)
                    if callback:
                        await callback(result)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("safety_monitor_error", error=str(e))
                
                await asyncio.sleep(interval)
        
        self._monitor_task = asyncio.create_task(monitor_loop())
        logger.info("safety_monitor_started", interval=interval)
    
    def stop_monitoring(self) -> None:
        """Stop background monitoring."""
        if self._monitor_task:
            self._monitor_task.cancel()
            self._monitor_task = None
            logger.info("safety_monitor_stopped")
```

---

### 3. Main Textual App (TCRMApp)

**File**: `tcrm_toolkit/interactive/app.py`

```python
"""Main Textual App for CRMA Toolkit Interactive TUI."""

import asyncio
from pathlib import Path
from typing import Optional

from textual import on, work
from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Footer, Header, Static

from tcrm_toolkit.core.config import Settings, get_settings
from tcrm_toolkit.core.crypto import create_crypto_manager
from tcrm_toolkit.interactive.session import SessionManager
from tcrm_toolkit.interactive.safety import SafetyMonitor, SafetyResult, RiskLevel
from tcrm_toolkit.interactive.screens.login_screen import LoginScreen
from tcrm_toolkit.interactive.screens.main_screen import MainScreen
from tcrm_toolkit.interactive.screens.org_picker import OrgPickerScreen
from tcrm_toolkit.interactive.screens.safety_modal import SafetyModalScreen
from tcrm_toolkit.interactive.widgets.status_bar import StatusBar


class TCRMApp(App):
    """
    Main Interactive TUI Application for CRMA Toolkit.
    
    Features:
    - Persistent session with SF CLI auth
    - Multi-org support with quick switching
    - VPN/Proxy safety monitoring with hard-block
    - Background task execution with progress
    - Command palette (Ctrl+P)
    - Keyboard-driven navigation
    """
    
    CSS_PATH = "styles/dark.css"
    
    BINDINGS = [
        Binding("ctrl+q", "quit", "Quit", show=True),
        Binding("ctrl+p", "command_palette", "Command Palette", show=True),
        Binding("ctrl+o", "org_picker", "Switch Org", show=True),
        Binding("ctrl+r", "refresh", "Refresh", show=True),
        Binding("f1", "help", "Help", show=False),
        Binding("escape", "escape", "Back/Cancel", show=False),
    ]
    
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.settings = get_settings()
        self.crypto = create_crypto_manager()
        self.safety = SafetyMonitor(self.settings)
        self.session = SessionManager(
            settings=self.settings,
            crypto=self.crypto,
            safety_monitor=self.safety,
        )
        self._main_screen: MainScreen | None = None
        self._safety_check_interval = self.settings.safety_check_interval
    
    def compose(self) -> ComposeResult:
        """Compose the app layout."""
        yield Header(show_clock=True)
        yield Container(id="main-container")
        yield StatusBar(id="status-bar")
        yield Footer()
    
    async def on_mount(self) -> None:
        """Initialize app on mount."""
        # Set up safety monitoring callback
        self.safety.start_monitoring(callback=self._on_safety_update)
        
        # Initialize session
        try:
            await self.session.initialize()
        except Exception as e:
            self.notify(f"Session init failed: {e}", severity="error")
        
        # Check safety - show modal if critical
        safety_result = await self.safety.check_connection_safety()
        if safety_result.risk_level == RiskLevel.CRITICAL and self.settings.safety_block_on_critical:
            self.push_screen(SafetyModalScreen(safety_result), self._on_safety_modal_dismiss)
        else:
            await self._show_main_screen()
    
    async def _on_safety_update(self, result: SafetyResult) -> None:
        """Handle safety monitor updates."""
        # Update status bar
        status_bar = self.query_one("#status-bar", StatusBar)
        status_bar.update_safety(result)
        
        # If critical risk appeared during session, show modal
        if result.risk_level == RiskLevel.CRITICAL and self.settings.safety_block_on_critical:
            if not self.screen_stack or not isinstance(self.screen_stack[-1], SafetyModalScreen):
                self.push_screen(SafetyModalScreen(result), self._on_safety_modal_dismiss)
    
    def _on_safety_modal_dismiss(self, action: str) -> None:
        """Handle safety modal dismissal."""
        if action == "retry":
            # Re-check safety
            self.run_worker(self._recheck_safety_and_continue())
        elif action == "continue":
            # User acknowledged risk - allow but warn
            self.notify("⚠️ Proceeding at your own risk!", severity="warning", timeout=10)
            self.run_worker(self._show_main_screen())
        elif action == "quit":
            self.exit()
    
    async def _recheck_safety_and_continue(self) -> None:
        """Re-check safety after user action."""
        result = await self.safety.check_connection_safety(force=True)
        if result.risk_level == RiskLevel.CRITICAL:
            self.push_screen(SafetyModalScreen(result), self._on_safety_modal_dismiss)
        else:
            await self._show_main_screen()
    
    async def _show_main_screen(self) -> None:
        """Show main screen after auth/safety checks."""
        # Pop any modal screens
        while len(self.screen_stack) > 1:
            self.pop_screen()
        
        # Check if we need login
        if not self.session.current_org:
            self.push_screen(LoginScreen(self.session), self._on_login_complete)
        else:
            await self._mount_main_screen()
    
    def _on_login_complete(self, success: bool) -> None:
        """Handle login screen completion."""
        if success:
            self.run_worker(self._mount_main_screen())
        else:
            self.notify("Login failed", severity="error")
            self.push_screen(LoginScreen(self.session), self._on_login_complete)
    
    async def _mount_main_screen(self) -> None:
        """Mount the main screen."""
        if self._main_screen is None:
            self._main_screen = MainScreen(self.session, self.safety)
        
        container = self.query_one("#main-container", Container)
        await container.mount(self._main_screen)
        self._main_screen.focus()
    
    # =========================================================================
    # Actions
    # =========================================================================
    
    async def action_command_palette(self) -> None:
        """Show command palette."""
        if self._main_screen:
            await self._main_screen.action_command_palette()
    
    async def action_org_picker(self) -> None:
        """Show org picker."""
        orgs = self.session.list_orgs()
        if not orgs:
            self.notify("No orgs configured. Run 'sf org login web' first.", severity="warning")
            return
        
        def on_org_selected(alias: str) -> None:
            self.run_worker(self._switch_org(alias))
        
        self.push_screen(OrgPickerScreen(orgs, self.session.current_alias), on_org_selected)
    
    async def _switch_org(self, alias: str) -> None:
        """Switch to selected org."""
        try:
            await self.session.switch_org(alias)
            self.notify(f"Switched to org: {alias}", severity="information")
            if self._main_screen:
                await self._main_screen.refresh_data()
            # Update status bar
            status_bar = self.query_one("#status-bar", StatusBar)
            status_bar.update_org(self.session.current_org)
        except Exception as e:
            self.notify(f"Failed to switch org: {e}", severity="error")
    
    async def action_refresh(self) -> None:
        """Refresh current view."""
        if self._main_screen:
            await self._main_screen.refresh_data()
            self.notify("Refreshed", severity="information", timeout=2)
    
    async def action_escape(self) -> None:
        """Handle escape key - context dependent."""
        if self._main_screen:
            await self._main_screen.action_escape()
    
    async def on_unmount(self) -> None:
        """Cleanup on app exit."""
        self.safety.stop_monitoring()
        await self.session.close()
        await self.safety.close()


# Entry point for `tcrm` command
def main() -> None:
    """Main entry point for interactive TUI."""
    app = TCRMApp()
    app.run()


if __name__ == "__main__":
    main()
```

---

### 4. Login Screen

**File**: `tcrm_toolkit/interactive/screens/login_screen.py`

```python
"""Login screen for initial authentication."""

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label, Static
from textual.widgets import Input

from tcrm_toolkit.interactive.session import SessionManager


class LoginScreen(ModalScreen[bool]):
    """Modal screen for SF CLI web login."""
    
    BINDINGS = [
        ("escape", "cancel", "Cancel"),
    ]
    
    def __init__(self, session: SessionManager):
        super().__init__()
        self.session = session
        self._alias = "default"
        self._instance_url = ""
    
    def compose(self) -> ComposeResult:
        yield Container(
            Vertical(
                Static("🔐 Salesforce Authentication", id="login-title"),
                Static(
                    "This will open a browser window for SF CLI web authentication.\n"
                    "Make sure SF CLI is installed: https://developer.salesforce.com/tools/sfdxcli",
                    id="login-info"
                ),
                Input(placeholder="Org alias (default)", id="alias-input", value="default"),
                Input(placeholder="Custom instance URL (optional)", id="instance-input"),
                Static("", id="login-status"),
                Button("Login with SF CLI", id="login-btn", variant="primary"),
                Button("Cancel", id="cancel-btn", variant="default"),
                id="login-form"
            ),
            id="login-container"
        )
    
    @on(Button.Pressed, "#login-btn")
    async def on_login_pressed(self) -> None:
        """Handle login button press."""
        alias_input = self.query_one("#alias-input", Input)
        instance_input = self.query_one("#instance-input", Input)
        status = self.query_one("#login-status", Static)
        login_btn = self.query_one("#login-btn", Button)
        
        self._alias = alias_input.value or "default"
        self._instance_url = instance_input.value or None
        
        login_btn.disabled = True
        status.update("🔄 Opening browser for authentication...")
        
        try:
            await self._run_login()
            self.dismiss(True)
        except Exception as e:
            status.update(f"❌ Login failed: {e}")
            login_btn.disabled = False
    
    @work(exclusive=True)
    async def _run_login(self) -> None:
        """Run SF CLI login in background."""
        status = self.query_one("#login-status", Static)
        
        token = await self.session.login(
            alias=self._alias,
            instance_url=self._instance_url,
        )
        
        status.update(f"✅ Authenticated as {self.session.current_org.username}")
    
    @on(Button.Pressed, "#cancel-btn")
    def on_cancel_pressed(self) -> None:
        self.dismiss(False)
    
    def action_cancel(self) -> None:
        self.dismiss(False)
```

---

### 5. Org Picker Screen

**File**: `tcrm_toolkit/interactive/screens/org_picker.py`

```python
"""Org picker screen for quick org switching."""

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Label, ListItem, ListView, Static

from tcrm_toolkit.interactive.session import OrgSession


class OrgPickerScreen(ModalScreen[str]):
    """Modal screen for picking org alias."""
    
    BINDINGS = [
        ("escape", "cancel", "Cancel"),
        ("enter", "select", "Select"),
    ]
    
    def __init__(self, orgs: list[OrgSession], current_alias: str):
        super().__init__()
        self.orgs = orgs
        self.current_alias = current_alias
    
    def compose(self) -> ComposeResult:
        yield Container(
            Vertical(
                Static("🔐 Switch Organization", id="picker-title"),
                ListView(
                    *[
                        ListItem(
                            Label(
                                f"{'● ' if org.alias == self.current_alias else '  '}"
                                f"{org.alias} — {org.username} ({org.instance_url})"
                            ),
                            id=f"org-{org.alias}",
                        )
                        for org in self.orgs
                    ],
                    id="org-list"
                ),
                Button("Cancel", id="cancel-btn"),
                id="picker-container"
            ),
            id="picker-dialog"
        )
    
    @on(ListView.Selected, "#org-list")
    def on_org_selected(self, event: ListView.Selected) -> None:
        item = event.item
        if item.id and item.id.startswith("org-"):
            alias = item.id[4:]
            self.dismiss(alias)
    
    @on(Button.Pressed, "#cancel-btn")
    def on_cancel(self) -> None:
        self.dismiss(None)
    
    def action_cancel(self) -> None:
        self.dismiss(None)
    
    def action_select(self) -> None:
        list_view = self.query_one("#org-list", ListView)
        if list_view.highlighted_child:
            item = list_view.highlighted_child
            if item.id and item.id.startswith("org-"):
                alias = item.id[4:]
                self.dismiss(alias)
```

---

### 6. Safety Modal Screen

**File**: `tcrm_toolkit/interactive/screens/safety_modal.py`

```python
"""Safety modal for critical VPN/Proxy detection."""

from textual import on
from textual.app import ComposeResult
from textual.containers import Container, Vertical
from textual.screen import ModalScreen
from textual.widgets import Button, Checkbox, Label, Static

from tcrm_toolkit.interactive.safety import SafetyResult, RiskLevel


class SafetyModalScreen(ModalScreen[str]):
    """Modal dialog for critical safety alerts."""
    
    BINDINGS = [
        ("escape", "cancel", "Cancel"),
    ]
    
    def __init__(self, safety_result: SafetyResult):
        super().__init__()
        self.safety_result = safety_result
        self._dont_show_again = False
    
    def compose(self) -> ComposeResult:
        # Build details text
        details_lines = []
        for check in self.safety_result.checks.values():
            if not check.passed:
                icon = "🔴" if check.risk_level == RiskLevel.CRITICAL else "🟡"
                details_lines.append(f"{icon} {check.name.value}: {check.details}")
                if check.remediation:
                    details_lines.append(f"   → {check.remediation}")
        
        details_text = "\n".join(details_lines) if details_lines else "Unknown risk detected"
        
        yield Container(
            Vertical(
                Static("⚠️ CONNECTION SAFETY ALERT", id="safety-title"),
                Static(
                    "Salesforce detects VPN/Proxy connections and will IMMEDIATELY disable your user.\n"
                    "Continuing risks permanent org lockout.",
                    id="safety-warning"
                ),
                Static(details_text, id="safety-details"),
                Checkbox("Don't show again this session", id="dont-show-checkbox"),
                Container(
                    Button("Disconnect VPN & Retry", id="retry-btn", variant="primary"),
                    Button("I Understand Risks - Continue", id="continue-btn", variant="warning"),
                    Button("Quit", id="quit-btn", variant="error"),
                    id="safety-buttons"
                ),
                id="safety-container"
            ),
            id="safety-dialog"
        )
    
    @on(Button.Pressed, "#retry-btn")
    def on_retry(self) -> None:
        self.dismiss("retry")
    
    @on(Button.Pressed, "#continue-btn")
    def on_continue(self) -> None:
        checkbox = self.query_one("#dont-show-checkbox", Checkbox)
        self._dont_show_again = checkbox.value
        self.dismiss("continue")
    
    @on(Button.Pressed, "#quit-btn")
    def on_quit(self) -> None:
        self.dismiss("quit")
    
    def action_cancel(self) -> None:
        self.dismiss("quit")
```

---

### 7. Status Bar Widget

**File**: `tcrm_toolkit/interactive/widgets/status_bar.py`

```python
"""Status bar widget for bottom of TUI."""

from textual.widgets import Static
from textual.containers import Horizontal

from tcrm_toolkit.interactive.safety import SafetyResult, RiskLevel
from tcrm_toolkit.interactive.session import OrgSession


class StatusBar(Static):
    """Bottom status bar showing org, safety, API usage, background tasks."""
    
    def __init__(self):
        super().__init__("", id="status-bar")
        self._org: OrgSession | None = None
        self._safety: SafetyResult | None = None
        self._api_usage = "0/15,000"
        self._bg_tasks = 0
    
    def update_org(self, org: OrgSession | None) -> None:
        self._org = org
        self._render()
    
    def update_safety(self, safety: SafetyResult) -> None:
        self._safety = safety
        self._render()
    
    def update_api_usage(self, used: int, limit: int) -> None:
        self._api_usage = f"{used:,}/{limit:,}"
        self._render()
    
    def update_bg_tasks(self, count: int) -> None:
        self._bg_tasks = count
        self._render()
    
    def _render(self) -> None:
        parts = []
        
        # Org info
        if self._org:
            parts.append(f"Org: {self._org.alias} ({self._org.username})")
        else:
            parts.append("Org: Not connected")
        
        # Safety indicator
        if self._safety:
            if self._safety.risk_level == RiskLevel.CRITICAL:
                parts.append("🔴 UNSAFE")
            elif self._safety.risk_level == RiskLevel.WARNING:
                parts.append("🟡 WARNING")
            else:
                parts.append("🟢 SAFE")
        else:
            parts.append("🟢 SAFE")
        
        # API usage
        parts.append(f"API: {self._api_usage}")
        
        # Background tasks
        if self._bg_tasks > 0:
            parts.append(f"BG: {self._bg_tasks} running")
        
        self.update("  •  ".join(parts))
```

---

### 8. Main Screen Skeleton

**File**: `tcrm_toolkit/interactive/screens/main_screen.py`

```python
"""Main screen with sidebar navigation and content area."""

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Container, Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, DataTable, Label, ListItem, ListView, Static, TabbedContent, TabPane

from tcrm_toolkit.interactive.session import SessionManager
from tcrm_toolkit.interactive.safety import SafetyMonitor
from tcrm_toolkit.interactive.widgets.detail_panel import DetailPanel


class MainScreen(Screen):
    """Main screen with navigation sidebar and content area."""
    
    BINDINGS = [
        ("ctrl+p", "command_palette", "Command Palette"),
        ("ctrl+o", "org_picker", "Switch Org"),
        ("ctrl+r", "refresh", "Refresh"),
        ("escape", "escape", "Back"),
    ]
    
    def __init__(self, session: SessionManager, safety: SafetyMonitor):
        super().__init__()
        self.session = session
        self.safety = safety
        self._current_view = "datasets"
    
    def compose(self) -> ComposeResult:
        yield Horizontal(
            # Sidebar Navigation
            Vertical(
                Static("📊 TCRM Toolkit", id="sidebar-title"),
                ListView(
                    ListItem(Label("📊 Datasets"), id="nav-datasets"),
                    ListItem(Label("📈 Dashboards"), id="nav-dashboards"),
                    ListItem(Label("🔄 Dataflows"), id="nav-dataflows"),
                    ListItem(Label("📋 Jobs"), id="nav-jobs"),
                    ListItem(Label("🔐 Orgs"), id="nav-orgs"),
                    ListItem(Label("⚙️ Config"), id="nav-config"),
                    id="nav-list"
                ),
                Static("[dim]Ctrl+P: Commands  Ctrl+O: Orgs  Ctrl+R: Refresh[/dim]", id="sidebar-hints"),
                id="sidebar"
            ),
            # Main Content Area
            Vertical(
                Static("Select a navigation item", id="content-title"),
                Container(id="content-area"),
                id="content"
            ),
            # Detail Panel (right side)
            DetailPanel(id="detail-panel"),
            id="main-layout"
        )
    
    async def on_mount(self) -> None:
        """Initialize main screen."""
        # Select first nav item
        nav_list = self.query_one("#nav-list", ListView)
        nav_list.index = 0
        await self._switch_view("datasets")
    
    @on(ListView.Selected, "#nav-list")
    async def on_nav_selected(self, event: ListView.Selected) -> None:
        item = event.item
        if item.id and item.id.startswith("nav-"):
            view = item.id[4:]
            await self._switch_view(view)
    
    async def _switch_view(self, view: str) -> None:
        """Switch to different view."""
        self._current_view = view
        
        # Update title
        titles = {
            "datasets": "📊 Datasets",
            "dashboards": "📈 Dashboards",
            "dataflows": "🔄 Dataflows",
            "jobs": "📋 Dataflow Jobs",
            "orgs": "🔐 Organizations",
            "config": "⚙️ Configuration",
        }
        self.query_one("#content-title", Static).update(titles.get(view, view))
        
        # Load view content
        await self._load_view(view)
    
    async def _load_view(self, view: str) -> None:
        """Load content for view."""
        container = self.query_one("#content-area", Container)
        await container.remove_children()
        
        if view == "datasets":
            await self._load_datasets_view(container)
        elif view == "dashboards":
            await self._load_dashboards_view(container)
        elif view == "dataflows":
            await self._load_dataflows_view(container)
        elif view == "jobs":
            await self._load_jobs_view(container)
        elif view == "orgs":
            await self._load_orgs_view(container)
        elif view == "config":
            await self._load_config_view(container)
    
    async def _load_datasets_view(self, container: Container) -> None:
        """Load datasets table."""
        table = DataTable(id="datasets-table", cursor_type="row")
        table.add_columns("#", "ID", "Name", "Label", "Rows", "Status")
        table.zebra_stripes = True
        await container.mount(table)
        
        # Load data in background
        self.run_worker(self._populate_datasets(table), exclusive=True)
    
    @work(exclusive=True)
    async def _populate_datasets(self, table: DataTable) -> None:
        """Populate datasets table."""
        try:
            async with self.session.client_context() as client:
                from tcrm_toolkit.core.services.dataset_service import DatasetService
                service = DatasetService(client, self.session.settings)
                datasets = await service.list_datasets(page_size=100)
                
                for i, ds in enumerate(datasets, 1):
                    rows = f"{ds.row_count:,}" if ds.row_count else "N/A"
                    table.add_row(str(i), ds.id, ds.name, ds.label, rows, ds.status)
        except Exception as e:
            self.notify(f"Failed to load datasets: {e}", severity="error")
    
    async def _load_dashboards_view(self, container: Container) -> None:
        """Load dashboards table."""
        table = DataTable(id="dashboards-table", cursor_type="row")
        table.add_columns("#", "ID", "Name", "Label", "Folder")
        table.zebra_stripes = True
        await container.mount(table)
        
        self.run_worker(self._populate_dashboards(table), exclusive=True)
    
    @work(exclusive=True)
    async def _populate_dashboards(self, table: DataTable) -> None:
        try:
            async with self.session.client_context() as client:
                from tcrm_toolkit.core.services.dashboard_service import DashboardService
                service = DashboardService(client, self.session.settings)
                dashboards = await service.list_dashboards()
                
                for i, db in enumerate(dashboards, 1):
                    folder = db.folder_name or "N/A"
                    table.add_row(str(i), db.id, db.name, db.label, folder)
        except Exception as e:
            self.notify(f"Failed to load dashboards: {e}", severity="error")
    
    async def _load_dataflows_view(self, container: Container) -> None:
        """Load dataflows table."""
        table = DataTable(id="dataflows-table", cursor_type="row")
        table.add_columns("#", "ID", "Name", "Label", "Status")
        table.zebra_stripes = True
        await container.mount(table)
        
        self.run_worker(self._populate_dataflows(table), exclusive=True)
    
    @work(exclusive=True)
    async def _populate_dataflows(self, table: DataTable) -> None:
        try:
            async with self.session.client_context() as client:
                from tcrm_toolkit.core.services.dataflow_service import DataflowService
                service = DataflowService(client, self.session.settings)
                dataflows = await service.list_dataflows()
                
                for i, df in enumerate(dataflows, 1):
                    table.add_row(str(i), df.id, df.name, df.label, df.status)
        except Exception as e:
            self.notify(f"Failed to load dataflows: {e}", severity="error")
    
    async def _load_jobs_view(self, container: Container) -> None:
        """Load dataflow jobs table."""
        table = DataTable(id="jobs-table", cursor_type="row")
        table.add_columns("#", "ID", "Dataflow", "Command", "Status", "Start Time", "End Time")
        table.zebra_stripes = True
        await container.mount(table)
        
        self.run_worker(self._populate_jobs(table), exclusive=True)
    
    @work(exclusive=True)
    async def _populate_jobs(self, table: DataTable) -> None:
        try:
            async with self.session.client_context() as client:
                from tcrm_toolkit.core.services.dataflow_service import DataflowService
                service = DataflowService(client, self.session.settings)
                jobs = await service.list_dataflow_jobs()
                
                for i, job in enumerate(jobs, 1):
                    start = job.start_time.strftime("%Y-%m-%d %H:%M") if job.start_time else "N/A"
                    end = job.end_time.strftime("%Y-%m-%d %H:%M") if job.end_time else "N/A"
                    table.add_row(str(i), job.id, job.dataflow_name, job.command, job.status, start, end)
        except Exception as e:
            self.notify(f"Failed to load jobs: {e}", severity="error")
    
    async def _load_orgs_view(self, container: Container) -> None:
        """Load orgs list."""
        orgs = self.session.list_orgs()
        
        table = DataTable(id="orgs-table", cursor_type="row")
        table.add_columns("#", "Alias", "Username", "Instance URL", "Current")
        table.zebra_stripes = True
        await container.mount(table)
        
        for i, org in enumerate(orgs, 1):
            current = "●" if org.alias == self.session.current_alias else ""
            table.add_row(str(i), org.alias, org.username or "N/A", org.instance_url, current)
    
    async def _load_config_view(self, container: Container) -> None:
        """Load configuration view."""
        await container.mount(Static("Configuration view - TODO"))
    
    async def refresh_data() -> None:
        """Refresh current view."""
        await self._load_view(self._current_view)
    
    async def action_escape(self) -> None:
        """Handle escape - clear detail panel."""
        detail = self.query_one("#detail-panel", DetailPanel)
        detail.clear()
    
    async def action_command_palette(self) -> None:
        """Show command palette - TODO in Phase 4."""
        self.notify("Command palette coming in Phase 4", severity="information")
```

---

### 9. Detail Panel Widget

**File**: `tcrm_toolkit/interactive/widgets/detail_panel.py`

```python
"""Detail panel widget for showing entity details."""

from textual.containers import Vertical
from textual.widgets import Static, DataTable, Label
from textual.widget import Widget


class DetailPanel(Widget):
    """Right-side detail panel for showing selected item details."""
    
    def __init__(self):
        super().__init__(id="detail-panel")
        self._content = Static("Select an item to view details", id="detail-content")
    
    def compose(self) -> ComposeResult:
        yield Vertical(
            Label("Details", id="detail-title"),
            self._content,
            id="detail-container"
        )
    
    def show_dataset(self, dataset) -> None:
        """Show dataset details."""
        from tcrm_toolkit.core.models import Dataset
        if not isinstance(dataset, Dataset):
            return
        
        content = f"""[bold]Dataset Details[/bold]

[cyan]ID:[/cyan] {dataset.id}
[cyan]Name:[/cyan] {dataset.name}
[cyan]Label:[/cyan] {dataset.label}
[cyan]Description:[/cyan] {dataset.description or 'N/A'}
[cyan]Status:[/cyan] {dataset.status}
[cyan]Type:[/cyan] {dataset.type}
[cyan]Row Count:[/cyan] {dataset.row_count:, if dataset.row_count else 'N/A'}
[cyan]Created:[/cyan] {dataset.created_date.strftime('%Y-%m-%d %H:%M') if dataset.created_date else 'N/A'}
[cyan]Last Modified:[/cyan] {dataset.last_modified_date.strftime('%Y-%m-%d %H:%M') if dataset.last_modified_date else 'N/A'}
[cyan]Current Version:[/cyan] {dataset.current_version_id or 'N/A'}
"""
        self._content.update(content)
    
    def show_dashboard(self, dashboard) -> None:
        """Show dashboard details."""
        from tcrm_toolkit.core.models import Dashboard
        if not isinstance(dashboard, Dashboard):
            return
        
        content = f"""[bold]Dashboard Details[/bold]

[cyan]ID:[/cyan] {dashboard.id}
[cyan]Name:[/cyan] {dashboard.name}
[cyan]Label:[/cyan] {dashboard.label}
[cyan]Description:[/cyan] {dashboard.description or 'N/A'}
[cyan]Folder:[/cyan] {dashboard.folder_name or 'N/A'}
[cyan]Created:[/cyan] {dashboard.created_date.strftime('%Y-%m-%d %H:%M') if dashboard.created_date else 'N/A'}
[cyan]Last Modified:[/cyan] {dashboard.last_modified_date.strftime('%Y-%m-%d %H:%M') if dashboard.last_modified_date else 'N/A'}
"""
        self._content.update(content)
    
    def show_dataflow(self, dataflow) -> None:
        """Show dataflow details."""
        from tcrm_toolkit.core.models import Dataflow
        if not isinstance(dataflow, Dataflow):
            return
        
        content = f"""[bold]Dataflow Details[/bold]

[cyan]ID:[/cyan] {dataflow.id}
[cyan]Name:[/cyan] {dataflow.name}
[cyan]Label:[/cyan] {dataflow.label}
[cyan]Description:[/cyan] {dataflow.description or 'N/A'}
[cyan]Status:[/cyan] {dataflow.status}
[cyan]Created:[/cyan] {dataflow.created_date.strftime('%Y-%m-%d %H:%M') if dataflow.created_date else 'N/A'}
[cyan]Last Modified:[/cyan] {dataflow.last_modified_date.strftime('%Y-%m-%d %H:%M') if dataflow.last_modified_date else 'N/A'}
"""
        self._content.update(content)
    
    def clear(self) -> None:
        """Clear detail panel."""
        self._content.update("Select an item to view details")
```

---

### 10. Update CLI Entry Point

**File**: `tcrm_toolkit/cli/main.py` (MODIFY)

Add interactive command:

```python
# Add to imports
from tcrm_toolkit.interactive import TCRMApp

# Add new command
@app.command()
def interactive() -> None:
    """Launch interactive TUI mode."""
    TCRMApp().run()

# Modify callback to default to interactive if no args
@app.callback(invoke_without_command=True)
def callback(
    ctx: typer.Context,
    verbose: bool = typer.Option(False, "--verbose", "-v"),
    version: bool = typer.Option(False, "--version"),
    interactive: bool = typer.Option(False, "--interactive", "-i", help="Launch interactive TUI"),
) -> None:
    if version:
        from tcrm_toolkit import __version__
        console.print(f"tcrm-toolkit version {__version__}")
        raise typer.Exit()
    
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    
    # Default to interactive if no subcommand
    if ctx.invoked_subcommand is None and not interactive:
        # Check if running in interactive terminal
        import sys
        if sys.stdin.isatty() and sys.stdout.isatty():
            TCRMApp().run()
        else:
            ctx.invoke(app["--help"])
```

---

## ✅ Acceptance Criteria

| Feature | Verification |
|---------|--------------|
| SessionManager initializes | `tcrm` starts, loads orgs from SF CLI |
| Multi-org switching | Ctrl+O shows org list, switches successfully |
| Safety monitor runs | Startup shows 🟢/🟡/🔴 in status bar |
| Critical risk blocks | VPN detected → modal appears, API calls blocked |
| Login screen works | First run → SF CLI web login → persistent session |
| Main screen loads | Sidebar navigation works, tables populate |
| Detail panel updates | Click row → details show on right |
| Cross-platform | Runs on Windows, Linux, macOS |
| Docker works | `docker compose run prod` launches TUI |

---

## 🔧 Coding Agent Instructions

### Implementation Order
1. **platform.py** - Cross-platform utilities (Phase 0, but needed here)
2. **safety.py** - SafetyMonitor with all 4 checks
3. **session.py** - SessionManager wrapping SFCLIAuthService
4. **widgets/status_bar.py** - Status bar with safety indicator
5. **screens/login_screen.py** - Modal login
6. **screens/org_picker.py** - Org switcher
7. **screens/safety_modal.py** - Critical risk modal
8. **widgets/detail_panel.py** - Detail panel
9. **screens/main_screen.py** - Main layout with navigation
10. **app.py** - TCRMApp tying everything together
11. **cli/main.py** - Add interactive command

### Key Patterns
- **Async throughout**: All I/O uses `async/await`
- **Error handling**: Try/except with user-friendly notifications
- **Background workers**: Use `@work(exclusive=True)` for data loading
- **Context managers**: `async with session.client_context()` for clients
- **Safety first**: Every API call goes through `session.get_client()` which checks safety

### Cross-Platform Notes
- SafetyMonitor uses platform-specific commands (PowerShell on Windows, `/sys/class/net` on Linux)
- All paths via `tcrm_toolkit.core.platform` utilities
- Textual handles terminal differences automatically

### Testing
```bash
# Unit tests
pytest tests/unit/test_safety_monitor.py -v
pytest tests/unit/test_session_manager.py -v

# Integration test
tcrm  # Should launch TUI
# Ctrl+O to test org picker
# Check status bar safety indicator
```

---

## 📝 Architecture Decisions (Log in `architecture-decisions.md`)

- [ ] Decision: SessionManager wraps SFCLIAuthService (not replace)
- [ ] Decision: SafetyMonitor fails open on check errors (don't block on false positives)
- [ ] Decision: Hard block on CRITICAL risk (career-ending consequence)
- [ ] Decision: Textual ModalScreen for login/org picker/safety modal
- [ ] Decision: Background monitoring via asyncio.Task with callback
- [ ] Decision: DataTable for all list views (sortable, keyboard nav)

---

*End of Phase 1 Document*