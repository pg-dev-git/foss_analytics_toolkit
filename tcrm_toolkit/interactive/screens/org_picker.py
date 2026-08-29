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

