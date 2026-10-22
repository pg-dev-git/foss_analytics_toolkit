"""Tests for the interactive prompt helpers.

The `prompt_*` helpers used to disagree about what `default` meant:
`prompt_text` treated it as the value used when the user pressed Enter,
while `prompt_select` displayed a numbered list but parsed the answer as an
index. Pressing Enter at the API-version prompt therefore fed "v68.0" into
`int()`, raised `ValueError`, and looped forever -- the only escape was
killing the process.

These tests pin one contract across all three helpers:

* `default` is the value used when the user presses Enter;
* `prompt_select` returns a choice value or None;
* `prompt_text` returns a string;
* `prompt_confirm` returns a bool.
"""

import pytest

from asftool.cli import ui
from asftool.cli.ui import prompt_confirm, prompt_select, prompt_text

CHOICES = ["v60.0", "v67.0", "v68.0"]


@pytest.fixture
def answers(monkeypatch):
    """Feed a scripted list of answers to Prompt.ask."""
    queue: list[str] = []

    def _ask(prompt, default=None, **kwargs):
        if queue:
            return queue.pop(0)
        return default

    monkeypatch.setattr(ui.Prompt, "ask", staticmethod(_ask))
    return queue


class TestPromptSelect:
    def test_accepts_a_one_based_index(self, answers):
        answers.append("2")
        assert prompt_select("Pick", CHOICES) == "v67.0"

    def test_accepts_the_choice_value(self, answers):
        answers.append("v68.0")
        assert prompt_select("Pick", CHOICES) == "v68.0"

    def test_enter_returns_the_default(self, answers):
        """Pressing Enter must not loop -- this was the reported bug."""
        answers.append("")
        assert prompt_select("Pick", CHOICES, default="v68.0") == "v68.0"

    def test_enter_returns_the_default_when_ask_returns_it(self, answers):
        """Prompt.ask returns the default when the user presses Enter."""
        answers.append("v67.0")
        assert prompt_select("Pick", CHOICES, default="v67.0") == "v67.0"

    def test_default_not_in_choices_is_ignored(self, answers):
        """A default that is not a choice cannot be offered."""
        answers.append("")
        answers.append("1")
        assert prompt_select("Pick", CHOICES, default="v99.0") == "v60.0"

    def test_rejects_an_out_of_range_index_and_asks_again(self, answers):
        answers.append("99")
        answers.append("1")
        assert prompt_select("Pick", CHOICES) == "v60.0"

    def test_rejects_garbage_and_asks_again(self, answers):
        answers.append("banana")
        answers.append("3")
        assert prompt_select("Pick", CHOICES) == "v68.0"

    def test_gives_up_after_too_many_invalid_answers(self, answers):
        """The prompt must terminate rather than loop forever."""
        for _ in range(ui.MAX_SELECT_ATTEMPTS + 5):
            answers.append("banana")
        assert prompt_select("Pick", CHOICES) is None

    def test_stops_asking_at_the_attempt_cap(self, answers):
        extra = 5
        for _ in range(ui.MAX_SELECT_ATTEMPTS + extra):
            answers.append("banana")
        prompt_select("Pick", CHOICES)
        # The prompt consumed exactly the cap and left the surplus alone.
        assert len(answers) == extra

    def test_ctrl_c_cancels(self, monkeypatch):
        def _ask(prompt, default=None, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(ui.Prompt, "ask", staticmethod(_ask))
        assert prompt_select("Pick", CHOICES) is None

    def test_eof_cancels(self, monkeypatch):
        def _ask(prompt, default=None, **kwargs):
            raise EOFError

        monkeypatch.setattr(ui.Prompt, "ask", staticmethod(_ask))
        assert prompt_select("Pick", CHOICES) is None

    def test_no_choices_returns_none(self, answers):
        assert prompt_select("Pick", []) is None


class TestPromptText:
    def test_returns_the_typed_text(self, answers):
        answers.append("revenue")
        assert prompt_text("Field") == "revenue"

    def test_empty_answer_returns_empty_string(self, answers):
        answers.append("")
        assert prompt_text("Field") == ""

    def test_enter_returns_the_default(self, answers):
        answers.append("revenue")
        assert prompt_text("Field", default="revenue") == "revenue"

    def test_ctrl_c_returns_empty_string(self, monkeypatch):
        def _ask(prompt, default=None, **kwargs):
            raise KeyboardInterrupt

        monkeypatch.setattr(ui.Prompt, "ask", staticmethod(_ask))
        assert prompt_text("Field") == ""


class TestPromptConfirm:
    def test_returns_a_bool(self, monkeypatch):
        monkeypatch.setattr(ui, "Confirm", type("C", (), {"ask": staticmethod(lambda *a, **k: True)}))
        assert prompt_confirm("Sure?") is True
