"""Context menu for row actions."""

from textual import on
from textual.app import ComposeResult
from textual.containers import Container
from textual.screen import ModalScreen
from textual.widgets import Button


class ContextMenu(ModalScreen[str]):
    """Context menu for row actions."""

    def __init__(self, actions: list[tuple[str, str]], x: int, y: int):
        super().__init__()
        self.actions = actions  # List of (label, action_id)
        self._x = x
        self._y = y

    def compose(self) -> ComposeResult:
        yield Container(
            Container(
                *[Button(label, id=f"action-{i}", variant="default") for i, (label, _) in enumerate(self.actions)],
                id="context-menu-items"
            ),
            id="context-menu"
        )

    def on_mount(self) -> None:
        # Position menu at cursor
        try:
            menu = self.query_one("#context-menu", Container)
            menu.styles.offset = (self._x, self._y)
        except Exception:
            pass

    @on(Button.Pressed)
    def on_action_selected(self, event: Button.Pressed) -> None:
        if event.button.id and event.button.id.startswith("action-"):
            try:
                idx = int(event.button.id.split("-")[1])
                if idx < len(self.actions):
                    _, action_id = self.actions[idx]
                    self.dismiss(action_id)
            except Exception:
                self.dismiss(None)

    def on_click(self, event) -> None:
        # Click outside closes menu
        self.dismiss(None)
