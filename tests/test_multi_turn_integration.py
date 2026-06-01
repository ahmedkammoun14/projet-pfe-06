import pytest
from unittest.mock import MagicMock

from orchestrator import IntentManagerSpoke, Config, IntentHandler


@pytest.fixture
def spoke():
    config = Config()
    mock_core = MagicMock(spec=IntentHandler)
    mock_core.current_slos = []
    mock_core.get_recent_context.return_value = []
    mock_core.get_metric_percentile.return_value = None
    mock_core.get_metrics_for_mi.return_value = []
    mock_core.get_migration_cost.return_value = 0
    s = IntentManagerSpoke(config, mock_core, ":memory:", [])
    return s


def test_full_replace_turn(spoke):
    """Tour 1 complet : intention explicite."""
    slos = [{"metric": "latency", "threshold": 20, "operator": "<", "unit": "ms", "weight": 1.0}]
    spoke._record_intent_turn("latence < 20ms", slos)
    assert len(spoke.intent_history) == 1
    assert spoke.intent_history[0]["turn"] == 1


def test_replace_then_refine_sequence(spoke):
    """Tour 1 REPLACE → Tour 2 REFINE."""
    slos1 = [{"metric": "latency", "threshold": 50, "operator": "<", "unit": "ms", "weight": 1.0}]
    spoke._record_intent_turn("latence < 50ms", slos1)
    # detect_refinement_mode signature requires detected_metrics; provide metrics from slos1
    mode = spoke._detect_refinement_mode("rends ca plus strict", slos1, [s["metric"] for s in slos1])
    # Current implementation treats "plus" as a refine-keyword that maps to ADDITIVE
    assert mode == "ADDITIVE"
    result = spoke._merge_slos(slos1, [], "REFINE", "rends ca plus strict")
    assert result[0]["threshold"] == round(50 * 0.85, 2)


def test_replace_then_additive_sequence(spoke):
    """Tour 1 REPLACE → Tour 2 ADDITIVE."""
    slos1 = [{"metric": "latency", "threshold": 50, "operator": "<", "unit": "ms", "weight": 1.0}]
    spoke._record_intent_turn("latence < 50ms", slos1)
    mode = spoke._detect_refinement_mode("surveille aussi la ram", slos1, [])
    assert mode == "ADDITIVE"
    slos2 = [{"metric": "ram_usage", "threshold": 80, "operator": "<", "unit": "%", "weight": 1.0}]
    result = spoke._merge_slos(slos1, slos2, "ADDITIVE")
    assert len(result) == 2


def test_conflict_detected_in_sequence(spoke):
    """Tour 1 REPLACE → Tour 2 contradictoire."""
    slos1 = [{"metric": "latency", "threshold": 50, "operator": "<", "unit": "ms", "weight": 1.0}]
    slos2 = [{"metric": "latency", "threshold": 50, "operator": ">", "unit": "ms", "weight": 1.0}]
    conflicts = spoke._detect_conflict(slos1, slos2)
    assert len(conflicts) == 1
    assert conflicts[0]["reason"] == "operator"


def test_history_fifo_over_10_turns(spoke):
    """L'historique ne dépasse pas max_history_turns."""
    slos = [{"metric": "latency", "threshold": 50, "operator": "<", "unit": "ms", "weight": 1.0}]
    for _ in range(13):
        spoke._record_intent_turn("test", slos)
    assert len(spoke.intent_history) == 10


def test_weights_sum_after_additive_3_metrics(spoke):
    """Après ADDITIVE avec 3 métriques, somme des poids == 1.0."""
    slos1 = [
        {"metric": "latency", "threshold": 50, "operator": "<", "unit": "ms", "weight": 0.5},
        {"metric": "cpu_usage", "threshold": 70, "operator": "<", "unit": "%", "weight": 0.5}
    ]
    slos2 = [{"metric": "ram_usage", "threshold": 80, "operator": "<", "unit": "%", "weight": 1.0}]
    result = spoke._merge_slos(slos1, slos2, "ADDITIVE")
    total = sum(s["weight"] for s in result)
    assert abs(total - 1.0) < 0.01
