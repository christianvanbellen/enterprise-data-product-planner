"""Tests for SpecRenderer — uses mock Anthropic client."""

import sys
import types
import pytest
from unittest.mock import MagicMock, patch

from graph.spec.assembler import SpecDocument
from graph.spec.renderer import SpecRenderer, _MODEL


def _make_anthropic_mock(text="Rendered text"):
    """Build a fake anthropic module + client that returns the given text."""
    mock_content = MagicMock()
    mock_content.text = text

    mock_message = MagicMock()
    mock_message.content = [mock_content]

    mock_client = MagicMock()
    mock_client.messages.create.return_value = mock_message

    mock_anthropic_mod = types.ModuleType("anthropic")
    mock_anthropic_mod.Anthropic = MagicMock(return_value=mock_client)

    return mock_anthropic_mod, mock_client


def _make_spec(**kwargs):
    defaults = dict(
        spec_id="abc123def456abcd",
        spec_type="full_spec",
        initiative_id="pricing_adequacy_monitoring",
        initiative_name="Pricing Adequacy Monitoring",
        archetype="monitoring",
        readiness="ready_now",
        composite_score=0.85,
        business_value_score=0.85,
        implementation_effort_score=0.35,
        business_objective="Detect drifting portfolios",
        output_type="monitoring_dashboard",
        target_users=["pricing_team"],
        composes_with=[],
        available_primitives=[],
        missing_primitives=[],
        blockers=[],
        grain_join_paths=[],
        graph_build_id="build_001",
        assembled_at_utc="2026-04-15T10:00:00+00:00",
    )
    defaults.update(kwargs)
    return SpecDocument(**defaults)


# ------------------------------------------------------------------ #
# Happy path — mocked Anthropic client                                #
# ------------------------------------------------------------------ #

def test_render_returns_markdown_on_success():
    spec = _make_spec(spec_type="full_spec")
    mock_mod, _ = _make_anthropic_mock("## Pricing Adequacy Monitoring\n\nSome rendered text.")

    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"}):
        with patch.dict(sys.modules, {"anthropic": mock_mod}):
            rendered, error = SpecRenderer().render(spec)

    assert error is None
    assert "Pricing Adequacy Monitoring" in rendered


def test_render_uses_correct_model():
    spec = _make_spec(spec_type="full_spec")
    mock_mod, mock_client = _make_anthropic_mock("Some text")

    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"}):
        with patch.dict(sys.modules, {"anthropic": mock_mod}):
            SpecRenderer().render(spec)

    call_kwargs = mock_client.messages.create.call_args[1]
    assert call_kwargs["model"] == _MODEL


def test_full_spec_uses_1200_max_tokens():
    spec = _make_spec(spec_type="full_spec")
    mock_mod, mock_client = _make_anthropic_mock("Rendered")

    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"}):
        with patch.dict(sys.modules, {"anthropic": mock_mod}):
            SpecRenderer().render(spec)

    call_kwargs = mock_client.messages.create.call_args[1]
    assert call_kwargs["max_tokens"] == 1600


def test_gap_brief_uses_600_max_tokens():
    spec = _make_spec(spec_type="gap_brief")
    mock_mod, mock_client = _make_anthropic_mock("Rendered")

    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"}):
        with patch.dict(sys.modules, {"anthropic": mock_mod}):
            SpecRenderer().render(spec)

    call_kwargs = mock_client.messages.create.call_args[1]
    assert call_kwargs["max_tokens"] == 600


# ------------------------------------------------------------------ #
# Error handling — never raises                                        #
# ------------------------------------------------------------------ #

def test_render_returns_empty_when_api_key_missing():
    spec = _make_spec()
    mock_mod, _ = _make_anthropic_mock()
    with patch.dict("os.environ", {}, clear=True):
        with patch.dict(sys.modules, {"anthropic": mock_mod}):
            rendered, error = SpecRenderer().render(spec)
    assert rendered == ""
    assert error is not None
    assert "ANTHROPIC_API_KEY" in error


def test_render_returns_empty_when_anthropic_not_installed():
    spec = _make_spec()
    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test"}):
        with patch.dict(sys.modules, {"anthropic": None}):
            rendered, error = SpecRenderer().render(spec)
    assert rendered == ""
    assert error is not None


def test_render_returns_empty_on_api_exception():
    spec = _make_spec()
    mock_mod, mock_client = _make_anthropic_mock()
    mock_client.messages.create.side_effect = RuntimeError("Rate limit exceeded")

    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"}):
        with patch.dict(sys.modules, {"anthropic": mock_mod}):
            rendered, error = SpecRenderer().render(spec)

    assert rendered == ""
    assert "Rate limit" in error


def test_render_handles_empty_content_gracefully():
    spec = _make_spec()
    mock_mod, mock_client = _make_anthropic_mock()
    # Override to return empty content list
    mock_message = MagicMock()
    mock_message.content = []
    mock_client.messages.create.return_value = mock_message

    with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "sk-test-key"}):
        with patch.dict(sys.modules, {"anthropic": mock_mod}):
            rendered, error = SpecRenderer().render(spec)

    assert rendered == ""
    assert error is None   # not an error — just empty response
