"""Regression: a dashboard's details must survive a failing sub-request.

The platform answers 404 for ``/wave/dashboards/{id}/datasets`` on some
dashboards. That sub-resource is supplementary -- the dashboard itself
loaded fine -- so a 404 on it must not abort the whole "show dashboard"
view and hide the details the user asked for.

Found by driving the interactive menu against a live org, where every
dashboard's "Show details" failed with "Resource not found".
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
def dashboard():
    """A minimal dashboard as the service returns it."""
    db = MagicMock()
    db.id = "0FKak0000018zdgGAA"
    db.name = "example_dashboard"
    db.label = "Example Dashboard"
    db.folder_name = "folder"
    db.created_date = "2026-07-15 20:12:55+00:00"
    db.last_modified_date = "2026-10-21 14:01:56+00:00"
    return db


class TestShowDashboardSurvivesAFailingDatasetsLookup:
    """The dashboard's own details must still be shown."""

    async def test_a_404_on_the_datasets_does_not_hide_the_details(
        self, dashboard, capsys
    ):
        from asftool.cli.commands.dashboards import show_dashboard_async
        from asftool.core.exceptions import SalesforceNotFoundError

        service = MagicMock()
        service.get_dashboard = AsyncMock(return_value=dashboard)
        service.get_dashboard_datasets = AsyncMock(
            side_effect=SalesforceNotFoundError("The requested resource does not exist")
        )

        session = MagicMock()

        class _Ctx:
            async def __aenter__(self):
                return MagicMock()

            async def __aexit__(self, *args):
                return False

        session.client_context = lambda: _Ctx()
        session.settings = MagicMock()
        session.close = AsyncMock()

        with (
            patch("asftool.cli.commands.dashboards.Session", return_value=session),
            patch(
                "asftool.cli.commands.dashboards.DashboardService",
                return_value=service,
            ),
        ):
            await show_dashboard_async(dashboard_id=dashboard.id)

        out = capsys.readouterr().out
        assert "example_dashboard" in out
        assert "0FKak0000018zdgGAA" in out

    async def test_the_datasets_line_says_they_are_unavailable(
        self, dashboard, capsys
    ):
        """The user is told the datasets are unavailable, not that they are empty."""
        from asftool.cli.commands.dashboards import show_dashboard_async
        from asftool.core.exceptions import SalesforceNotFoundError

        service = MagicMock()
        service.get_dashboard = AsyncMock(return_value=dashboard)
        service.get_dashboard_datasets = AsyncMock(
            side_effect=SalesforceNotFoundError("nope")
        )

        session = MagicMock()

        class _Ctx:
            async def __aenter__(self):
                return MagicMock()

            async def __aexit__(self, *args):
                return False

        session.client_context = lambda: _Ctx()
        session.settings = MagicMock()
        session.close = AsyncMock()

        with (
            patch("asftool.cli.commands.dashboards.Session", return_value=session),
            patch(
                "asftool.cli.commands.dashboards.DashboardService",
                return_value=service,
            ),
        ):
            await show_dashboard_async(dashboard_id=dashboard.id)

        out = capsys.readouterr().out
        assert "not available" in out
