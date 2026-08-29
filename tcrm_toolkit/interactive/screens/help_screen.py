"""Help screen modal with tabbed content and keyboard shortcuts."""

from __future__ import annotations

from textual.app import ComposeResult
from textual.containers import Container
from textual.screen import ModalScreen
from textual.widgets import Button, Static, TabbedContent, TabPane


class HelpScreen(ModalScreen[None]):
    """Modal screen displaying keyboard shortcuts and help."""

    def compose(self) -> ComposeResult:
        with Container(id="help-dialog"):
            yield Static("CRM Toolkit - Keyboard Shortcuts & Help", id="help-title")
            with TabbedContent():
                with TabPane("Global"):
                    yield Static(
                        "\n".join([
                            "Ctrl+Q : Quit application",
                            "Ctrl+P : Open command palette",
                            "Ctrl+O : Organization picker (switch org)",
                            "Ctrl+R : Refresh current view",
                            "F1     : Help screen",
                            "Escape : Back / Cancel / Clear search",
                        ])
                    )
                with TabPane("Navigation"):
                    yield Static(
                        "\n".join([
                            "Tab / Shift+Tab : Move between panels",
                            "Arrow Up / Down / j / k : Navigate lists",
                            "Page Up / Page Down : Scroll pages",
                            "Home / End : Start/End of list",
                            "Enter : Select / Activate item",
                        ])
                    )
                with TabPane("Data Browsers"):
                    yield Static(
                        "\n".join([
                            "/ : Focus search input",
                            "Escape : Clear search",
                            "Enter : Apply search filter",
                            "Click Header : Sort column",
                        ])
                    )
                with TabPane("Actions"):
                    yield Static(
                        "\n".join([
                            "E : Extract dataset",
                            "U : Upload dataset",
                            "B : Backup dashboard",
                            "R : Restore dashboard",
                            "S : Start dataflow",
                            "T : Stop dataflow",
                            "Y : Show dependencies",
                            "D : Delete item (with confirmation)",
                            "C : Copy ID to clipboard",
                        ])
                    )
            yield Button("Close", variant="primary", id="close-btn")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "close-btn":
            self.dismiss(None)

    def on_key(self, event) -> None:
        if event.key == "escape":
            self.dismiss(None)
