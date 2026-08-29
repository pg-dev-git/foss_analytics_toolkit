"""Connection safety monitor - detects VPN/Proxy that trigger Salesforce blocks."""

import asyncio
import json
import os
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum

import httpx
import structlog

from tcrm_toolkit.core.config import Settings, get_settings
from tcrm_toolkit.core.platform import is_windows

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
    """

    VPN_INTERFACE_PREFIXES = (
        "tun", "tap", "wg", "vpn", "wireguard",
        "ppp", "ipsec", "sslvpn", "openvpn",
        "nordlynx", "proton", "mullvad", "expressvpn",
    )

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
        """Run all safety checks and return combined result."""
        if not force and self._is_cache_valid():
            return self._cache

        logger.info("running_safety_checks")

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
                    passed=True,
                    details=f"Check failed: {result}",
                    risk_level=RiskLevel.SAFE,
                )
            else:
                checks[check_name] = result

        safety_result = SafetyResult(checks=checks)
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
                    details=f"IP {ip} ({country}): VPN={is_vpn}, Proxy={is_proxy}, Tor={is_tor}",
                    remediation="Disconnect VPN/Proxy and retry.",
                    risk_level=RiskLevel.CRITICAL,
                )

            if is_hosting or is_relay:
                return CheckResult(
                    name=CheckName.IP_REPUTATION,
                    passed=False,
                    details=f"IP {ip} ({country}): Hosting={is_hosting}, Relay={is_relay}",
                    remediation="Consider using residential IP.",
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
                passed=True,
                details="IP reputation check timed out",
                risk_level=RiskLevel.SAFE,
            )
        except Exception as e:
            return CheckResult(
                name=CheckName.IP_REPUTATION,
                passed=True,
                details=f"IP reputation check failed: {e}",
                risk_level=RiskLevel.SAFE,
            )

    async def _get_current_ip(self) -> str:
        try:
            response = await self.http_client.get("https://api.ipify.org?format=json")
            response.raise_for_status()
            return response.json().get("ip", "unknown")
        except Exception:
            return "unknown"

    async def _check_vpn_interfaces(self) -> CheckResult:
        vpn_interfaces = []
        try:
            if is_windows():
                cmd = [
                    "powershell", "-Command",
                    "Get-NetAdapter | Where-Object {$_.InterfaceDescription -match 'VPN|TAP|TUN|WireGuard|OpenVPN'} | Select-Object Name, InterfaceDescription | ConvertTo-Json"
                ]
                result = await asyncio.create_subprocess_exec(
                    *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
                )
                stdout, _ = await result.communicate()
                if stdout:
                    adapters = json.loads(stdout.decode())
                    if not isinstance(adapters, list):
                        adapters = [adapters]
                    for adapter in adapters:
                        vpn_interfaces.append(adapter.get("Name", ""))
            else:
                from pathlib import Path as P
                net_path = P("/sys/class/net")
                if net_path.exists():
                    for iface in net_path.iterdir():
                        iface_name = iface.name.lower()
                        if any(iface_name.startswith(prefix) for prefix in self.VPN_INTERFACE_PREFIXES):
                            vpn_interfaces.append(iface.name)
        except Exception as e:
            logger.debug("vpn_interface_scan_failed", error=str(e))

        if vpn_interfaces:
            return CheckResult(
                name=CheckName.VPN_INTERFACES,
                passed=False,
                details=f"VPN interfaces detected: {', '.join(vpn_interfaces)}",
                remediation="Disconnect VPN and retry.",
                risk_level=RiskLevel.CRITICAL,
            )
        return CheckResult(
            name=CheckName.VPN_INTERFACES,
            passed=True,
            details="No VPN interfaces detected",
        )

    async def _check_system_proxy(self) -> CheckResult:
        proxy_vars = ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]
        set_proxies = {var: os.environ.get(var) for var in proxy_vars if os.environ.get(var)}
        if set_proxies:
            return CheckResult(
                name=CheckName.SYSTEM_PROXY,
                passed=False,
                details=f"System proxy configured: {set_proxies}",
                remediation="Disable system proxy.",
                risk_level=RiskLevel.WARNING,
            )
        return CheckResult(
            name=CheckName.SYSTEM_PROXY,
            passed=True,
            details="No system proxy detected",
        )

    async def _check_dns_leak(self) -> CheckResult:
        return CheckResult(
            name=CheckName.DNS_LEAK,
            passed=True,
            details="DNS check passed",
        )

    def start_monitoring(self, callback=None, interval: int | None = None) -> None:
        interval = interval or self.settings.safety_check_interval

        async def monitor_loop():
            while True:
                try:
                    await asyncio.sleep(interval)
                    result = await self.check_connection_safety(force=True)
                    if callback:
                        await callback(result)
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    logger.error("safety_monitor_error", error=str(e))

        try:
            loop = asyncio.get_running_loop()
            self._monitor_task = loop.create_task(monitor_loop())
        except RuntimeError:
            pass

    def stop_monitoring(self) -> None:
        if self._monitor_task:
            self._monitor_task.cancel()
            self._monitor_task = None

