"""Command Palette modal for fuzzy searching and executing actions."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Container
from textual.screen import ModalScreen
from textual.widgets import Input, ListItem, ListView, Static


class CommandPaletteScreen(ModalScreen[str | None]):
    """Modal screen for command palette search and execution."""

    def __init__(self, commands: list[tuple[str, str]]):
        """
        commands: list of (display_name, action_id)
        """
        super().__init__()
        self.commands = commands
        self.filtered_commands = list(commands)

    def compose(self) -> ComposeResult:
        with Container(id="palette-dialog"):
            yield Static("Command Palette", id="palette-title")
            yield Input(placeholder="Type to search commands...", id="search-input")
            yield ListView(id="nav-list")

    def on_mount(self) -> None:
        self.query_one("#search-input", Input).focus()
        self._populate_list(self.commands)

    def _populate_list(self, cmds: list[tuple[str, str]]) -> None:
        list_view = self.query_one("#nav-list", ListView)
        list_view.clear()
        for display_name, action_id in cmds:
            list_view.append(ListItem(Static(display_name), id=f"cmd-{action_id}"))

    def on_input_changed(self, event: Input.Changed) -> None:
        query = event.value.lower()
        if not query:
            self.filtered_commands = list(self.commands)
        else:
            self.filtered_commands = [
                (name, act) for name, act in self.commands if query in name.lower() or query in act.lower()
            ]
        self._populate_list(self.filtered_commands)

    def on_list_view_selected(self, event: ListView.Selected) -> None:
        item_id = event.item.id
        if item_id and item_id.startswith("cmd-"):
            action_id = item_id[4:]
            self.dismiss(action_id)

    def on_key(self, event) -> None:
        if event.key == "escape":
            self.dismiss(None)

