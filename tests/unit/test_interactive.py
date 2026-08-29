"""Unit tests for Phase 1 Interactive TUI components (SafetyMonitor, SessionManager)."""

import base64
from datetime import datetime

import pytest

from tcrm_toolkit.core.config import Settings
from tcrm_toolkit.interactive.safety import (
    CheckName,
    CheckResult,
    RiskLevel,
    SafetyMonitor,
    SafetyResult,
)
from tcrm_toolkit.interactive.session import SessionManager


@pytest.fixture
def settings():
    encryption_key = base64.urlsafe_b64encode(b"x" * 32).decode()
    jwt_secret = base64.urlsafe_b64encode(b"y" * 32).decode()
    return Settings.model_construct(
        encryption_key=encryption_key,
        jwt_secret_key=jwt_secret,
        safety_allowlist_ips=["192.168.1.1"],
    )


@pytest.mark.asyncio
async def test_safety_result_post_init():
    # Test safe result
    res = SafetyResult(is_safe=True, checks={
        CheckName.DNS_LEAK: CheckResult(name=CheckName.DNS_LEAK, passed=True, details="OK", risk_level=RiskLevel.SAFE)
    })
    assert res.is_safe is True
    assert res.risk_level == RiskLevel.SAFE

    # Test critical risk
    res_crit = SafetyResult(is_safe=True, checks={
        CheckName.IP_REPUTATION: CheckResult(name=CheckName.IP_REPUTATION, passed=False, details="VPN detected", risk_level=RiskLevel.CRITICAL)
    })
    assert res_crit.is_safe is False
    assert res_crit.risk_level == RiskLevel.CRITICAL
    assert "ip_reputation" in res_crit.details


@pytest.mark.asyncio
async def test_safety_monitor_allowlist(settings):
    monitor = SafetyMonitor(settings)
    # Mock _get_current_ip to return allowlisted IP
    monitor._get_current_ip = async_return_value("192.168.1.1")

    result = await monitor._check_ip_reputation()
    assert result.passed is True
    assert "allowlist" in result.details
    await monitor.close()


def async_return_value(val):
    async def factory(*args, **kwargs):
        return val
    return factory


@pytest.mark.asyncio
async def test_session_manager_init(settings):
    session = SessionManager(settings=settings)
    assert session.current_alias == "default"
    assert session.current_org is None
    orgs = session.list_orgs()
    assert isinstance(orgs, list)
    await session.close()


def test_data_browser_and_context_menu():
    from tcrm_toolkit.core.models import DataflowJob
    from tcrm_toolkit.interactive.widgets.context_menu import ContextMenu
    from tcrm_toolkit.interactive.widgets.data_table import ColumnConfig, DataBrowser
    from tcrm_toolkit.interactive.widgets.detail_panel import DetailPanel

    cols = [ColumnConfig(key="id", title="ID"), ColumnConfig(key="name", title="Name")]
    async def dummy_load(offset, limit, search, sort):
        return [{"id": "1", "name": "Test"}], 1

    browser = DataBrowser(
        columns=cols,
        load_data=dummy_load,
        get_row_id=lambda r: r["id"],
        get_row_data=lambda r: r,
    )
    assert browser.title == "Data Browser"

    menu = ContextMenu([("View", "view"), ("Delete", "del")], 10, 10)
    assert len(menu.actions) == 2

    detail = DetailPanel()
    job = DataflowJob(
        id="job_1",
        dataflow_id="df_1",
        dataflow_name="df_1",
        command="start",
        status="Success",
        start_time=datetime(2026, 7, 23, 10, 0),
        end_time=datetime(2026, 7, 23, 10, 5),
    )
    detail.show_dataflow_job(job)
    assert "job_1" in str(detail._content.render())


@pytest.mark.asyncio
async def test_task_runner():
    import asyncio
    import tempfile
    from pathlib import Path

    import pandas as pd

    from tcrm_toolkit.interactive.tasks import TaskRunner, TaskStatus, merge_csv_chunks

    runner = TaskRunner(max_concurrent=2)

    async def dummy_coro():
        await asyncio.sleep(0.01)
        return "success"

    result = await runner.run_task(dummy_coro, name="Dummy")
    assert result.status == TaskStatus.COMPLETED
    assert result.result == "success"

    history = runner.get_history()
    assert len(history) == 1

    # Test process pool with merge_csv_chunks
    with tempfile.TemporaryDirectory() as tmpdir:
        p1 = Path(tmpdir) / "c1.csv"
        p2 = Path(tmpdir) / "c2.csv"
        out = Path(tmpdir) / "out.csv"
        pd.DataFrame({"a": [1, 2]}).to_csv(p1, index=False)
        pd.DataFrame({"a": [3, 4]}).to_csv(p2, index=False)

        res = await runner.run_in_process_pool(merge_csv_chunks, [str(p1), str(p2)], str(out))
        assert res["total_rows"] == 4
        assert out.exists()

    await runner.close()


def test_phase_4_polish_components():
    import tempfile
    from pathlib import Path
    from tcrm_toolkit.interactive.config import TUIConfig
    from tcrm_toolkit.interactive.config_manager import ConfigManager
    from tcrm_toolkit.interactive.window_manager import WindowManager
    from tcrm_toolkit.interactive.notifications import NotificationManager
    from tcrm_toolkit.interactive.widgets.command_palette import CommandPaletteScreen
    from tcrm_toolkit.interactive.screens.help_screen import HelpScreen

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp_path = Path(tmpdir)
        
        # Test Config & ConfigManager
        cfg_mgr = ConfigManager(tmp_path)
        cfg = cfg_mgr.load()
        assert cfg.theme == "dark"
        cfg.theme = "light"
        cfg_mgr.save(cfg)
        
        cfg_mgr2 = ConfigManager(tmp_path)
        cfg2 = cfg_mgr2.load()
        assert cfg2.theme == "light"

        # Test WindowManager
        win_mgr = WindowManager(tmp_path)
        win_mgr.set("sidebar_width", 30)
        assert win_mgr.get("sidebar_width") == 30

        # Test NotificationManager
        notif_mgr = NotificationManager(max_history=5)
        rec = notif_mgr.record("Test alert", severity="warning")
        assert rec.message == "Test alert"
        assert rec.severity == "warning"
        assert len(notif_mgr.get_history()) == 1

        # Test CommandPaletteScreen init
        palette = CommandPaletteScreen([("Extract Dataset", "extract"), ("Upload Dataset", "upload")])
        assert len(palette.commands) == 2

        # Test HelpScreen init
        help_screen = HelpScreen()
        assert help_screen is not None



