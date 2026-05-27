import unittest
from unittest.mock import MagicMock, patch, patch
import pytest
import time
import json
from datetime import datetime, timezone
from typing import List, Dict

# Import des composants à tester
from orchestrator import (
    Config, 
    calculate_weighted_mean, 
    DecisionIntelligenceSpoke, 
    MLPredictorSpoke, 
    IntentManagerSpoke, 
    CollectorSpoke, 
    OrchestratorCore,
    DatabaseSpoke,
    ObservabilitySpoke
)

# =============================================================================
# 1. TESTS DES UTILITAIRES (Unit Tests)
# =============================================================================

def test_calculate_weighted_mean_empty():
    """Vérifie qu'une liste vide retourne 0.0."""
    assert calculate_weighted_mean([]) == 0.0

def test_calculate_weighted_mean_single_value():
    """Vérifie qu'une seule valeur retourne elle-même."""
    assert calculate_weighted_mean([10.0]) == 10.0

def test_calculate_weighted_mean_seven_values():
    """Vérifie le calcul exact pour 7 valeurs (poids 7 à 1, total 28)."""
    # (10*7 + 10*6 + 10*5 + 10*4 + 10*3 + 10*2 + 10*1) / 28 = 280 / 28 = 10.0
    values = [10.0] * 7
    assert calculate_weighted_mean(values) == 10.0
    
    # Cas avec variation : [20, 10, 10, 10, 10, 10, 10]
    # (20*7 + 10*6 + 10*5 + 10*4 + 10*3 + 10*2 + 10*1) / 28 = 350 / 28 = 12.5
    values2 = [20.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0]
    assert calculate_weighted_mean(values2) == 12.5

# =============================================================================
# 2. TESTS DE LA LOGIQUE DE DÉCISION (Logic Tests)
# =============================================================================

class TestDecisionLogic:
    
    @pytest.fixture
    def config(self):
        return Config(DEFAULT_LATENCY_THRESHOLD=50.0)

    @pytest.fixture
    def decision_engine(self, config):
        return DecisionIntelligenceSpoke(config)

    def test_classic_decision_nominal(self, decision_engine):
        """Arrange: Toutes les VMs sous le seuil. Act: Evaluate. Assert: Stay."""
        current_data = [{"vm_id": "vm1", "rtt_ms": 10.0}, {"vm_id": "vm2", "rtt_ms": 15.0}]
        preds = {"vm1": [10.0]*5, "vm2": [15.0]*5}
        
        result = decision_engine.evaluate_classic_decision(current_data, preds)
        
        assert result["decision"] == "stay"
        assert "Nominal" in result["reason"]

    def test_classic_decision_threshold_breach(self, decision_engine):
        """Arrange: vm1 dépasse 50ms. Act: Evaluate. Assert: Migrate."""
        current_data = [{"vm_id": "vm1", "rtt_ms": 60.0}, {"vm_id": "vm2", "rtt_ms": 10.0}]
        preds = {"vm1": [60.0]*5, "vm2": [10.0]*5}
        
        result = decision_engine.evaluate_classic_decision(current_data, preds)
        
        assert result["decision"] == "migrate"
        assert result["from_vm"] == "vm1"
        assert result["to_vm"] == "vm2"

    def test_enhanced_decision_cpu_breach(self, decision_engine):
        """Arrange: SLO CPU < 70, vm1 est à 80. Act: Evaluate. Assert: Migrate."""
        slos = [{"metric": "cpu_usage", "threshold": 70.0, "weight": 1.0}]
        current_data = [
            {"vm_id": "vm1", "cpu_usage": 80.0, "rtt_ms": 20.0},
            {"vm_id": "vm2", "cpu_usage": 40.0, "rtt_ms": 20.0}
        ]
        # Prédictions stables
        preds = {
            "vm1": {"cpu_usage": [80.0]*7},
            "vm2": {"cpu_usage": [40.0]*7}
        }
        
        result = decision_engine.evaluate_enhanced_decision(current_data, preds, slos)
        
        assert result["decision"] == "migrate"
        assert result["from_vm"] == "vm1"
        assert result["to_vm"] == "vm2"

    def test_enhanced_decision_missing_metric(self, decision_engine):
        """Vérifie que si une métrique est absente (None), le SLO est ignoré."""
        slos = [{"metric": "ram_usage", "threshold": 80.0, "weight": 1.0}]
        current_data = [{"vm_id": "vm1", "rtt_ms": 20.0}] # ram_usage manquant
        preds = {"vm1": {"ram_usage": [50.0]*7}}
        
        result = decision_engine.evaluate_enhanced_decision(current_data, preds, slos)
        
        assert result["decision"] == "stay" # Pas de crash, stay car pas de violation ram_usage connue

# =============================================================================
# 3. TESTS D'EXTRACTION D'INTENTION (LLM & Regex)
# =============================================================================

class TestIntentExtraction:

    @pytest.fixture
    def intent_mgr(self):
        return IntentManagerSpoke(Config(), MagicMock())

    def test_parse_slos_valid_json(self, intent_mgr):
        """Vérifie l'extraction d'un JSON valide retourné par le LLM."""
        llm_output = '[{"metric": "latency", "operator": "<", "threshold": 25, "unit": "ms", "weight": 1.0}]'
        slos, success = intent_mgr._parse_slos_from_text(llm_output)
        
        assert success is True
        assert len(slos) == 1
        assert slos[0]["metric"] == "latency"
        assert slos[0]["threshold"] == 25

    def test_parse_slos_regex_fallback(self, intent_mgr):
        """Vérifie le fallback regex si le JSON est malformé."""
        bad_output = "I want latency < 30 and cpu < 60 please."
        slos, success = intent_mgr._parse_slos_from_text(bad_output)
        
        assert success is True
        metrics = [s["metric"] for s in slos]
        assert "latency" in metrics
        assert "cpu_usage" in metrics
        # Vérifie la somme des poids
        assert sum(s["weight"] for s in slos) == pytest.approx(1.0)

# =============================================================================
# 4. TESTS DU COOLDOWN (Core State Machine)
# =============================================================================

class TestOrchestratorCore:

    @patch('orchestrator.DatabaseSpoke')
    @patch('orchestrator.ObservabilitySpoke')
    @patch('orchestrator.MLPredictorSpoke')
    def test_cooldown_active(self, mock_ml, mock_viz, mock_db):
        """Vérifie que la décision est 'stay' si une migration a eu lieu il y a < 60s."""
        core = OrchestratorCore(Config())
        core.last_migration_ts = time.time() - 10 # Migration il y a 10s
        
        # Simuler des données qui devraient normalement déclencher une migration
        core.decision_engine.evaluate_classic_decision = MagicMock()
        measurements = [{"vm_id": "vm1", "rtt_ms": 999.0}] 
        
        result = core.run_classic_flow(measurements)
        
        assert result["decision"] == "stay"
        assert "Cooldown actif" in result["reason"]
        # Vérifie que l'intelligence n'a même pas été appelée
        core.decision_engine.evaluate_classic_decision.assert_not_called()

# =============================================================================
# 5. TESTS DES SPOKES DE DONNÉES (Collectors & Predictors)
# =============================================================================

class TestSpokesData:

    @patch('requests.get')
    def test_ml_predictor_api_fallback(self, mock_get):
        """Vérifie le fallback local si l'API ML est injoignable."""
        mock_get.side_effect = Exception("Connection Error")
        predictor = MLPredictorSpoke(Config())
        
        preds = predictor.get_prediction(20.0)
        
        assert len(preds) == 5 # simulation locale génère 5 points
        assert preds[0] > 20.0 # simulation locale fait croître la valeur

    @patch('requests.get')
    def test_collector_api_success(self, mock_get):
        """Vérifie la collecte réelle via l'API de simulation de VM."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"cpu_usage": 45.5, "ram_usage": 60.0}
        mock_get.return_value = mock_resp
        
        collector = CollectorSpoke(Config())
        data = collector.collect_vm_metrics("vm1")
        
        assert data["cpu_usage"] == 45.5
        assert data["ram_usage"] == 60.0

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    # Permet de lancer les tests directement via 'python test_orchestrator.py'
    pytest.main([__file__, "-v"])
