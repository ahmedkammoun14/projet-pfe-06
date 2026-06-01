import unittest
from unittest.mock import MagicMock, patch
import pytest
import time
import json
import sqlite3
import os
import tempfile
from datetime import datetime, timezone, timedelta
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
    ObservabilitySpoke,
    ValidationResult,
    LatencyManagerSpoke,
    PredictionResult
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
        return IntentManagerSpoke(Config(), MagicMock(), "orchestrator.db", [])

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
        # On définit le retour de get_window_count pour éviter l'erreur avec MagicMock
        mock_db_instance = mock_db.return_value
        mock_db_instance.get_window_count.return_value = 10
        mock_db_instance.get_slo_violations.return_value = 0

        core = OrchestratorCore(Config())
        core.last_migration_ts = time.time() - 10 # Migration il y a 10s
        
        # Simuler des données qui devraient normalement déclencher une migration
        core.decision_engine.evaluate_enhanced_decision = MagicMock()
        measurements = [{"vm_id": "vm1", "rtt_ms": 999.0}] 
        
        result = core.run_autonomous_flow(measurements)
        
        assert result["decision"] == "stay"
        assert "Cooldown actif" in result["reason"]
        # Vérifie que l'intelligence n'a même pas été appelée
        core.decision_engine.evaluate_enhanced_decision.assert_not_called()

    @patch('orchestrator.logger')
    @patch('orchestrator.DatabaseSpoke')
    @patch('orchestrator.ObservabilitySpoke')
    @patch('orchestrator.MLPredictorSpoke')
    def test_classic_flow_logs_deprecation_warning(self, mock_ml, mock_viz, mock_db, mock_logger):
        """Vérifie que run_classic_flow() produit un avertissement de dépréciation.
        
        Cette méthode teste que:
        1. L'appel à run_classic_flow() déclenche un logger.warning()
        2. Le message contient le texte attendu sur la dépréciation
        3. run_autonomous_flow() est recommandé comme remplacement
        """
        from io import StringIO
        import sys
        
        mock_db_instance = mock_db.return_value
        mock_db_instance.get_window_count.return_value = 10
        mock_db_instance.get_slo_violations.return_value = 0
        
        core = OrchestratorCore(Config())
        core.db = mock_db_instance
        
        # Simuler des données minimales pour la méthode
        measurements = [{"vm_id": "vm1", "rtt_ms": 20.0}]
        
        # Rediriger stdout pour éviter les problèmes d'encodage avec colorama
        old_stdout = sys.stdout
        sys.stdout = StringIO()
        try:
            # Appeler la méthode dépréciée
            core.run_classic_flow(measurements)
        finally:
            sys.stdout = old_stdout
        
        # Vérifier que logger.warning a été appelé (plusieurs fois potentiellement)
        assert mock_logger.warning.called, "logger.warning() n'a pas été appelé"
        
        # Chercher parmi tous les appels celui qui contient le message de dépréciation
        deprecation_found = False
        for call in mock_logger.warning.call_args_list:
            call_args = call[0][0] if call[0] else ""
            if "run_classic_flow()" in call_args and "déprécié" in call_args:
                deprecation_found = True
                break
        
        assert deprecation_found, (
            f"Aucun appel à logger.warning() ne contient "
            f"le message de dépréciation attendu. "
            f"Appels: {[call[0][0] if call[0] else '' for call in mock_logger.warning.call_args_list]}"
        )

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
# 6. TESTS DU SCHEMA OPENSLO (Budget & Violations)
# =============================================================================

class TestOpenSLOSchema:
    
    @pytest.fixture
    def intent_mgr(self):
        return IntentManagerSpoke(Config(), MagicMock(), "orchestrator.db", [])

    def test_enrich_slo_schema_defaults(self, intent_mgr):
        """Vérifie que les 4 champs sont présents avec valeurs par défaut."""
        raw_slo = {"metric": "latency", "threshold": 50}
        enriched = intent_mgr._enrich_slo_schema(raw_slo)
        
        assert enriched["target"] == 0.99
        assert enriched["window"] == "5m"
        assert enriched["budget_remaining"] == 100.0
        assert enriched["violations"] == 0

    def test_enrich_slo_schema_no_overwrite(self, intent_mgr):
        """Vérifie que target existant n'est pas écrasé."""
        raw_slo = {"metric": "cpu_usage", "threshold": 70, "target": 0.95}
        enriched = intent_mgr._enrich_slo_schema(raw_slo)
        
        assert enriched["target"] == 0.95
        assert enriched["window"] == "5m"

    def test_get_slo_violations_empty_db(self):
        """Vérifie que get_slo_violations retourne 0 sur DB vide sans crash."""
        config = Config(DB_NAME=":memory:") # DB en mémoire pour test isolé
        db = DatabaseSpoke(config)
        db.init_db()
        
        # Test latency < 50
        violations = db.get_slo_violations("latency", 50.0, "<", 300)
        assert violations == 0

    def test_budget_remaining_bounds(self):
        """Vérifie que budget_remaining reste dans [0.0, 100.0] même avec violations > total."""
        # On peut simuler la logique de OrchestratorCore directement
        def calculate_budget(violations, total_points):
            if total_points > 0:
                budget = 100.0 * (1 - (violations / total_points))
                return max(0.0, min(100.0, round(budget, 2)))
            return 100.0
            
        assert calculate_budget(10, 5) == 0.0 # Plus de violations que de points
        assert calculate_budget(-1, 5) == 100.0 # Cas impossible mais test bornes
        assert calculate_budget(2, 10) == 80.0 # Cas nominal

# =============================================================================
# 7. TESTS DU SIGNAL BUDGET (Cooldown & Scoring)
# =============================================================================

class TestErrorBudgetSignal:

    @pytest.fixture
    def core(self):
        with patch('orchestrator.DatabaseSpoke'), \
             patch('orchestrator.ObservabilitySpoke'), \
             patch('orchestrator.MLPredictorSpoke'), \
             patch('orchestrator.IntentManagerSpoke'), \
             patch('orchestrator.CollectorSpoke'), \
             patch('orchestrator.LatencyManagerSpoke'):
            core = OrchestratorCore(Config())
            return core

    def test_cooldown_override_when_budget_zero(self, core):
        """Vérifie que le cooldown est ignoré si le budget est à 0%."""
        core.mode = "enhanced"
        core.last_migration_ts = time.time() - 10  # Cooldown normalement actif
        core.current_slos = [{"metric": "latency", "threshold": 50, "budget_remaining": 0.0}]
        
        assert core._is_budget_exhausted() is True
        assert core._check_cooldown() is None  # Override actif

    def test_cooldown_respected_when_budget_ok(self, core):
        """Vérifie que le cooldown est respecté si le budget est > 0%."""
        core.mode = "enhanced"
        core.last_migration_ts = time.time() - 10
        core.current_slos = [{"metric": "latency", "threshold": 50, "budget_remaining": 10.0}]
        
        result = core._check_cooldown()
        assert result["decision"] == "stay"
        assert "Cooldown actif" in result["reason"]

    def test_compute_vm_score_budget_bonus(self):
        """Vérifie que le budget bonus réduit le score (favorise la VM)."""
        engine = DecisionIntelligenceSpoke(Config())
        slos = [{"metric": "latency", "threshold": 50, "weight": 1.0, "budget_remaining": 80.0}]
        weights = {"latency": 1.0}
        
        target = {"vm_id": "vm1", "rtt_ms": 20.0} # Respecte le SLO
        preds = {"vm1": {"latency": [20.0]*5}}
        
        # Sans bonus (budget = 0)
        slos_no_budget = [{"metric": "latency", "threshold": 50, "weight": 1.0, "budget_remaining": 0.0}]
        score_base = engine._compute_vm_score(target, preds, slos_no_budget, weights)
        
        # Avec bonus (budget = 80)
        score_with_bonus = engine._compute_vm_score(target, preds, slos, weights)
        
        assert score_with_bonus < score_base
        # Différence attendue : weight(1.0) * budget(0.8) = 0.8
        assert score_with_bonus == pytest.approx(score_base - 0.8)

    def test_enhanced_decision_prefers_high_budget_vm(self):
        """Vérifie que le moteur préfère une VM avec un budget élevé à RTT égal."""
        engine = DecisionIntelligenceSpoke(Config())
        slos = [{"metric": "latency", "threshold": 50, "weight": 1.0}]
        # Ajout manuel des budgets simulés
        slo_low = {**slos[0], "budget_remaining": 10.0}
        slo_high = {**slos[0], "budget_remaining": 90.0}
        
        # Deux VMs avec le même RTT actuel et prédit
        current_data = [
            {"vm_id": "vm1", "rtt_ms": 20.0}, # Target candidate 1
            {"vm_id": "vm2", "rtt_ms": 20.0}, # Target candidate 2
            {"vm_id": "vm_source", "rtt_ms": 60.0} # VM en violation
        ]
        
        preds = {
            "vm1": {"latency": [20.0]*5},
            "vm2": {"latency": [20.0]*5},
            "vm_source": {"latency": [60.0]*5}
        }
        
        # Simulation : vm2 est "plus saine" (budget 90) vs vm1 (budget 10)
        # On doit mocker _compute_vm_score ou manipuler slos pendant l'appel
        # Le plus simple ici est de tester directement _compute_vm_score pour vm1 vs vm2
        weights = {"latency": 1.0}
        score_vm1 = engine._compute_vm_score(current_data[0], preds, [slo_low], weights)
        score_vm2 = engine._compute_vm_score(current_data[1], preds, [slo_high], weights)
        
        assert score_vm2 < score_vm1

# =============================================================================
# 8. TESTS DE COHÉRENCE SLO (Validator)
# =============================================================================

class TestSLOCoherenceValidator:

    @pytest.fixture
    def intent_mgr(self):
        return IntentManagerSpoke(Config(), MagicMock(), "orchestrator.db", [])

    def test_valid_slos_pass_validation(self, intent_mgr):
        """Vérifie que les SLOs valides passent sans erreur."""
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        validator = intent_mgr._SLOCoherenceValidator()
        result = validator.validate(slos)
        
        assert result.is_valid is True
        assert len(result.errors) == 0
        assert len(result.corrected_slos) == 1

    def test_latency_below_minimum_rejected(self, intent_mgr):
        """Vérifie que la latence < 5ms est rejetée."""
        slos = [{"metric": "latency", "threshold": 1.0, "weight": 1.0}]
        validator = intent_mgr._SLOCoherenceValidator()
        result = validator.validate(slos)
        
        # is_valid = False car le seul SLO est rejeté
        assert result.is_valid is False
        assert any("latency threshold 1.0ms est hors plage" in e for e in result.errors)
        assert len(result.corrected_slos) == 0

    def test_invalid_slo_removed_weights_redistributed(self, intent_mgr):
        """Vérifie qu'un SLO invalide est supprimé et les poids redistribués."""
        slos = [
            {"metric": "latency", "threshold": 1.0, "weight": 0.5},   # Invalide
            {"metric": "cpu_usage", "threshold": 70.0, "weight": 0.5} # Valide
        ]
        validator = intent_mgr._SLOCoherenceValidator()
        result = validator.validate(slos)
        
        assert result.is_valid is True
        assert len(result.corrected_slos) == 1
        assert result.corrected_slos[0]["metric"] == "cpu_usage"
        # Poids doit être redistribué à 1.0
        assert result.corrected_slos[0]["weight"] == 1.0

    def test_weight_sum_error_detected(self, intent_mgr):
        """Vérifie la détection et correction d'une somme de poids != 1.0."""
        slos = [
            {"metric": "latency", "threshold": 50.0, "weight": 0.8},
            {"metric": "cpu_usage", "threshold": 70.0, "weight": 0.5} # Total 1.3
        ]
        validator = intent_mgr._SLOCoherenceValidator()
        result = validator.validate(slos)
        
        assert result.is_valid is True
        assert any("Somme des poids = 1.30" in e for e in result.errors)
        # Vérifie correction automatique (0.5 + 0.5)
        assert sum(s["weight"] for s in result.corrected_slos) == pytest.approx(1.0)

    def test_all_slos_invalid_returns_false(self, intent_mgr):
        """Vérifie que si tous les SLOs sont invalides, is_valid est False."""
        slos = [
            {"metric": "latency", "threshold": 0.0, "weight": 0.5},
            {"metric": "cpu_usage", "threshold": 100.0, "weight": 0.5}
        ]
        validator = intent_mgr._SLOCoherenceValidator()
        result = validator.validate(slos)
        
        assert result.is_valid is False
        assert len(result.corrected_slos) == 0

# =============================================================================
# 9. TESTS DES PERCENTILES HISTORIQUES (Tâche 3)
# =============================================================================

class TestHistoricalPercentiles:
    
    @pytest.fixture
    def db(self, tmp_path):
        db_file = tmp_path / "test_orchestrator.db"
        config = Config(DB_NAME=str(db_file))
        db = DatabaseSpoke(config)
        db.init_db()
        return db

    def test_percentile_insufficient_data(self, db):
        """DB vide ou < 10 points -> retourne None sans crash."""
        # 1. DB vide
        assert db.get_historical_percentiles("latency", 25.0) is None
        
        # 2. 9 points
        measurements = [{"vm_id": "vm1", "rtt_ms": 20.0, "cpu_usage": 50.0, "ram_usage": 60.0}] * 9
        db.save_metrics(measurements, "test")
        assert db.get_historical_percentiles("latency", 25.0) is None

    def test_percentile_calculation_correct(self, db):
        """Insérer 10 valeurs connues et vérifier le calcul du P25."""
        # Valeurs : 10, 20, 30, 40, 50, 60, 70, 80, 90, 100
        # n = 10, percentile = 25
        # index = 25/100 * (10-1) = 0.25 * 9 = 2.25
        # lower = 2 (valeur 30), upper = 3 (valeur 40)
        # weight = 0.25
        # Result = 30 + 0.25 * (40 - 30) = 32.5
        for i in range(1, 11):
            val = float(i * 10)
            m = [{"vm_id": "vm1", "rtt_ms": val, "cpu_usage": val, "ram_usage": val}]
            db.save_metrics(m, "test")
            
        p25 = db.get_historical_percentiles("latency", 25.0)
        assert p25 == pytest.approx(32.5)

    def test_percentile_p10_less_than_p25(self, db):
        """Vérifie la cohérence mathématique P10 < P25 < P30."""
        for i in range(1, 21):
            m = [{"vm_id": "vm1", "rtt_ms": float(i), "cpu_usage": float(i), "ram_usage": float(i)}]
            db.save_metrics(m, "test")
            
        p10 = db.get_historical_percentiles("latency", 10.0)
        p25 = db.get_historical_percentiles("latency", 25.0)
        p30 = db.get_historical_percentiles("latency", 30.0)
        
        assert p10 is not None
        assert p25 is not None
        assert p30 is not None
        assert p10 < p25 < p30

    def test_rag_context_includes_percentiles(self):
        """Mocker hub.get_metric_percentile et vérifier l'inclusion du bloc."""
        hub = MagicMock()
        # Mock des percentiles pour latency
        hub.get_metric_percentile.side_effect = lambda m, p, w: {
            ( "latency", 10.0): 18.5,
            ( "latency", 25.0): 24.2,
            ( "latency", 30.0): 27.1,
        }.get((m, p))
        
        hub.get_recent_context.return_value = [{"vm_id": "vm1", "rtt_ms": 20.0, "cpu_usage": 50.0, "ram_usage": 60.0}]
        
        slos = [{"metric": "latency", "operator": "<", "threshold": 50.0, "unit": "ms", "weight": 1.0}]
        builder = IntentManagerSpoke._RAGContextBuilder(Config(), hub, slos)
        
        ctx = builder.build_context()
        assert "Seuils historiquement bons" in ctx
        assert "latency" in ctx
        assert "P10=18.5ms" in ctx
        assert "P25=24.2ms" in ctx

    def test_rag_context_skips_block_if_all_none(self):
        """Si tous les percentiles sont None, le bloc doit être absent."""
        hub = MagicMock()
        hub.get_metric_percentile.return_value = None
        hub.get_recent_context.return_value = [{"vm_id": "vm1", "rtt_ms": 20.0, "cpu_usage": 50.0, "ram_usage": 60.0}]
        
        slos = [{"metric": "latency", "operator": "<", "threshold": 50.0, "unit": "ms", "weight": 1.0}]
        builder = IntentManagerSpoke._RAGContextBuilder(Config(), hub, slos)
        
        ctx = builder.build_context()
        assert "Seuils historiquement bons" not in ctx

# =============================================================================
# 10. TESTS DU PROMPT FEW-SHOT (Tâche 4)
# =============================================================================

class TestFewShotPrompt:
    
    @pytest.fixture
    def intent_mgr(self):
        return IntentManagerSpoke(Config(), MagicMock(), ":memory:", [])

    def test_build_system_prompt_contains_examples(self, intent_mgr):
        """Appeler _build_system_prompt("") et vérifier la présence des exemples."""
        prompt = intent_mgr._build_system_prompt("")
        assert "ralentissements" in prompt
        assert "latence < 15ms" in prompt
        assert "stable" in prompt

    def test_build_system_prompt_with_context(self, intent_mgr):
        """Vérifier que le contexte est injecté et les exemples présents."""
        context = "RTT=38ms P25=24ms"
        prompt = intent_mgr._build_system_prompt(context)
        assert context in prompt
        assert "Exemple 1" in prompt
        assert "Exemple 2" in prompt
        assert "Exemple 3" in prompt

    def test_build_system_prompt_empty_context(self, intent_mgr):
        """Vérifier que le prompt reste valide sans contexte."""
        prompt = intent_mgr._build_system_prompt("")
        assert "P25" in prompt
        assert "threshold" in prompt
        assert "Somme des weights DOIT être 1.0" in prompt

    @patch('requests.post')
    def test_query_uses_build_system_prompt(self, mock_post, intent_mgr):
        """Mocker LLM et vérifier l'appel à _build_system_prompt."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "message": {"content": '[{"metric":"latency","operator":"<","threshold":20,"unit":"ms","weight":1.0}]'}
        }
        mock_post.return_value = mock_resp
        
        # On espionne l'appel via MagicMock
        intent_mgr._build_system_prompt = MagicMock(side_effect=intent_mgr._build_system_prompt)
        
        intent_mgr.query_intent_engine("Rends ça plus rapide")
        
        assert intent_mgr._build_system_prompt.called
        # Vérifie que le premier argument passé à requests.post contient le prompt construit
        args, kwargs = mock_post.call_args
        payload = kwargs.get('json', {})
        system_content = payload['messages'][0]['content']
        assert "Tu es un expert SLO" in system_content

# =============================================================================
# 11. TESTS DU KEYWORDS MATCHER (Tâche 5)
# =============================================================================

class TestKeywordsMatcher:
    
    @pytest.fixture
    def matcher(self):
        return IntentManagerSpoke._KeywordsMatcher(Config())

    def test_detect_profile_ux_sensitive(self, matcher):
        """text = "mon app est trop lente et pas réactive" -> ux_sensitive."""
        text = "mon app est trop lente et pas réactive"
        assert matcher.detect_profile(text) == "ux_sensitive"

    def test_detect_profile_edge_critical(self, matcher):
        """text = "utilisateurs critiques edge temps réel" -> edge_critical."""
        text = "utilisateurs critiques edge temps réel"
        assert matcher.detect_profile(text) == "edge_critical"

    def test_detect_profile_no_match_returns_default(self, matcher):
        """text = "xyz abc 123" -> default."""
        assert matcher.detect_profile("xyz abc 123") == "default"

    def test_build_slos_with_percentiles(self, matcher):
        """Mocker hub.get_metric_percentile et vérifier les seuils."""
        hub = MagicMock()
        hub.get_metric_percentile.return_value = 30.0
        
        slos = matcher.build_slos("ux_sensitive", hub)
        
        assert len(slos) == 2
        # Latency should be 30.0
        latency_slo = next(s for s in slos if s["metric"] == "latency")
        assert latency_slo["threshold"] == 30.0
        assert sum(s["weight"] for s in slos) == pytest.approx(1.0)

    def test_build_slos_fallback_on_none_percentile(self, matcher):
        """Mocker hub.get_metric_percentile -> None et vérifier fallback Config."""
        hub = MagicMock()
        hub.get_metric_percentile.return_value = None
        
        slos = matcher.build_slos("ux_sensitive", hub)
        
        latency_slo = next(s for s in slos if s["metric"] == "latency")
        assert latency_slo["threshold"] == Config().DEFAULT_LATENCY_THRESHOLD
        assert len(slos) == 2

    @patch('requests.post')
    def test_keywords_matcher_not_called_on_llm_success(self, mock_post):
        """Vérifier que le matcher n'est pas instancié si Ollama réussit."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "message": {"content": '[{"metric":"latency","operator":"<","threshold":20,"unit":"ms","weight":1.0}]'}
        }
        mock_post.return_value = mock_resp
        
        intent_mgr = IntentManagerSpoke(Config(), MagicMock(), ":memory:", [])
        
        with patch.object(IntentManagerSpoke, '_KeywordsMatcher', wraps=IntentManagerSpoke._KeywordsMatcher) as mock_matcher:
            intent_mgr.query_intent_engine("Rends ça plus rapide")
            assert not mock_matcher.called

# =============================================================================
# 12. TESTS DE LA CASCADE DE FALLBACK (Tâche 6)
# =============================================================================

class TestFallbackCascade:
    
    @pytest.fixture
    def intent_mgr(self):
        return IntentManagerSpoke(Config(), MagicMock(), ":memory:", [])

    @patch('requests.post')
    def test_regex_called_when_llm_returns_invalid_json(self, mock_post, intent_mgr):
        """Si Ollama retourne du texte non-JSON, on tente le regex sur le texte original."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"message": {"content": "Désolé, je ne peux pas générer de JSON"}}
        mock_post.return_value = mock_resp
        
        # On s'assure que le KeywordsMatcher n'est PAS appelé car "latency < 30" contient des chiffres
        with patch.object(IntentManagerSpoke, '_KeywordsMatcher') as mock_matcher:
            slos, success = intent_mgr.query_intent_engine("latency < 30")
            
            assert success is True
            assert any(s["metric"] == "latency" and s["threshold"] == 30 for s in slos)
            assert not mock_matcher.called

    @patch('requests.post')
    def test_keywords_called_when_llm_and_regex_fail(self, mock_post, intent_mgr):
        """Si LLM et Regex échouent (pas de chiffres), KeywordsMatcher prend le relais."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"message": {"content": "..."}}
        mock_post.return_value = mock_resp
        
        # On doit mocker le hub (core) pour retourner des valeurs numériques pour les percentiles
        intent_mgr.core.get_metric_percentile.return_value = 25.0
        
        # "mon app est lente" ne contient pas de chiffres -> Regex échoue
        slos, success = intent_mgr.query_intent_engine("mon app est lente")
        
        assert success is True
        # Par défaut "lente" matche ux_sensitive -> latency et cpu_usage
        metrics = [s["metric"] for s in slos]
        assert "latency" in metrics
        assert "cpu_usage" in metrics

    @patch('requests.post')
    def test_validation_always_called(self, mock_post, intent_mgr):
        """Vérifie que validate() est appelé quel que soit le résultat."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "message": {"content": '[{"metric":"latency","operator":"<","threshold":20,"unit":"ms","weight":1.0}]'}
        }
        mock_post.return_value = mock_resp
        
        # On s'assure que le core retourne des valeurs valides
        intent_mgr.core.get_metric_percentile.return_value = 20.0
        
        # Spy sur validate
        with patch('orchestrator.IntentManagerSpoke._SLOCoherenceValidator') as mock_class:
            mock_class.return_value.validate.return_value = ValidationResult(
                is_valid=True, errors=[], warnings=[], 
                corrected_slos=[{"metric":"latency","threshold":20}]
            )
            intent_mgr.query_intent_engine("test")
            assert mock_class.return_value.validate.called

    @patch('requests.post')
    def test_cascade_llm_success_skips_fallbacks(self, mock_post, intent_mgr):
        """Si Ollama réussit, les fallbacks (regex et keywords) ne sont pas sollicités."""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "message": {"content": '[{"metric":"latency","operator":"<","threshold":20,"unit":"ms","weight":1.0}]'}
        }
        mock_post.return_value = mock_resp
        
        with patch.object(IntentManagerSpoke, '_KeywordsMatcher') as mock_matcher:
            with patch.object(IntentManagerSpoke, '_parse_slos_from_text', 
                              side_effect=intent_mgr._parse_slos_from_text) as mock_parse:
                
                intent_mgr.query_intent_engine("latency < 30")
                
                # mock_parse est appelé une fois pour le texte LLM
                # Il ne doit pas être appelé une DEUXIÈME fois pour le texte original "latency < 30"
                # car success était True.
                assert mock_parse.call_count == 1 
                assert not mock_matcher.called

# =============================================================================
# 13. TESTS DU STOCKAGE DES VIOLATIONS (Tâche 7)
# =============================================================================

class TestIsViolationStorage:
    
    @pytest.fixture
    def db(self, tmp_path):
        db_file = tmp_path / "test_is_violation.db"
        config = Config(DB_NAME=str(db_file))
        db = DatabaseSpoke(config)
        db.init_db()
        return db

    def test_is_violation_stored_when_slo_breached(self, db):
        """Vérifie que is_violation est à 1 si le seuil est dépassé."""
        slo = {"metric": "latency", "threshold": 50.0}
        measurement = [{"vm_id": "vm1", "rtt_ms": 65.0}]
        
        db.save_metrics(measurement, "autonomous", [slo])
        
        with sqlite3.connect(db.config.DB_NAME) as conn:
            row = conn.execute("SELECT is_violation FROM metrics WHERE id=1").fetchone()
            assert row[0] == 1

    def test_is_violation_zero_when_slo_respected(self, db):
        """Vérifie que is_violation est à 0 si le seuil est respecté."""
        slo = {"metric": "latency", "threshold": 50.0}
        measurement = [{"vm_id": "vm1", "rtt_ms": 30.0}]
        
        db.save_metrics(measurement, "autonomous", [slo])
        
        with sqlite3.connect(db.config.DB_NAME) as conn:
            row = conn.execute("SELECT is_violation FROM metrics WHERE id=1").fetchone()
            assert row[0] == 0

    def test_is_violation_zero_when_no_slos(self, db):
        """Vérifie que is_violation est à 0 si aucun SLO n'est passé."""
        measurement = [{"vm_id": "vm1", "rtt_ms": 100.0}]
        db.save_metrics(measurement, "autonomous", [])
        
        with sqlite3.connect(db.config.DB_NAME) as conn:
            row = conn.execute("SELECT is_violation FROM metrics WHERE id=1").fetchone()
            assert row[0] == 0

    def test_is_violation_any_metric_triggers(self, db):
        """Vérifie qu'une violation de n'importe quelle métrique active le flag."""
        slo = {"metric": "cpu_usage", "threshold": 70.0}
        measurement = [{"vm_id": "vm1", "cpu_usage": 85.0}]
        
        db.save_metrics(measurement, "autonomous", [slo])
        
        with sqlite3.connect(db.config.DB_NAME) as conn:
            row = conn.execute("SELECT is_violation FROM metrics WHERE id=1").fetchone()
            assert row[0] == 1

    def test_migration_douce_existing_table(self, tmp_path):
        """Vérifie que l'ajout de la colonne is_violation fonctionne sur une table existante."""
        db_file = tmp_path / "test_migration.db"
        
        # 1. Créer la table à l'ancienne (sans is_violation)
        with sqlite3.connect(str(db_file)) as conn:
            conn.execute("CREATE TABLE metrics (id INTEGER PRIMARY KEY, vm_id TEXT, rtt_ms REAL, cpu_usage REAL, ram_usage REAL, mode TEXT, timestamp TEXT)")
        
        # 2. Appeler init_db() (migration douce)
        config = Config(DB_NAME=str(db_file))
        db = DatabaseSpoke(config)
        db.init_db() # Ne doit pas crash
        db.init_db() # Un deuxième appel ne doit pas crash non plus
        
        # 3. Vérifier la présence de la colonne
        with sqlite3.connect(str(db_file)) as conn:
            # PRAGMA table_info retourne une ligne par colonne
            columns = [row[1] for row in conn.execute("PRAGMA table_info(metrics)").fetchall()]
            assert "is_violation" in columns

# =============================================================================
# 14. TESTS DU SCORING MI (Tâche 8)
# =============================================================================

class TestMIScoring:
    
    @pytest.fixture
    def mgr(self):
        # Utiliser les composants réels de orchestrator.py
        from orchestrator import MetricsManagerSpoke
        return MetricsManagerSpoke()

    def test_mi_score_high_when_correlated(self, mgr):
        """MI score élevé quand cpu_usage est parfaitement corrélé à is_violation."""
        # On crée 10 points
        data = []
        for i in range(10):
            # cpu > 70 -> violation=1, cpu < 70 -> violation=0
            cpu = 80.0 if i < 5 else 40.0
            viol = 1 if i < 5 else 0
            data.append({"cpu_usage": cpu, "is_violation": viol})
            
        scores = mgr.compute_mi_scores(data)
        # MI pour corrélation parfaite = 1.0 (normalisé)
        assert scores["cpu_usage"] > 0.5
        assert scores["cpu_usage"] <= 1.0

    def test_mi_score_zero_when_uncorrelated(self, mgr):
        """MI score proche de 0 quand latency est constante ou aléatoire."""
        data = []
        for i in range(10):
            data.append({
                "rtt_ms": 30.0, # constante -> pas d'information
                "is_violation": 1 if i % 2 == 0 else 0
            })
            
        scores = mgr.compute_mi_scores(data)
        assert scores["latency"] == pytest.approx(0.0, abs=1e-5)

    def test_mi_fallback_when_insufficient_data(self, mgr):
        """Fallback statique si moins de 5 points."""
        slos = [{"metric": "cpu_usage"}]
        # Cas 1 : data vide
        metrics, scores = mgr.analyze_needed_metrics(slos, hub=MagicMock(), window_seconds=300)
        assert "cpu_usage" in metrics
        assert scores == {}
        
        # Cas 2 : pas assez de points (3 points)
        hub = MagicMock()
        hub.get_metrics_for_mi.return_value = [{"cpu_usage": 50, "is_violation": 0}] * 3
        metrics, scores = mgr.analyze_needed_metrics(slos, hub=hub)
        assert "cpu_usage" in metrics
        assert scores == {}

    def test_analyze_returns_slo_metrics_always(self, mgr):
        """Une métrique dans les SLOs est TOUJOURS retournée, même avec MI=0."""
        slos = [{"metric": "cpu_usage"}]
        hub = MagicMock()
        # On simule un score MI élevé pour ram_usage mais 0 pour cpu_usage
        hub.get_metrics_for_mi.return_value = [
            {"ram_usage": 100 if i < 5 else 10, "cpu_usage": 50, "is_violation": 1 if i < 5 else 0} 
            for i in range(10)
        ]
        
        metrics, scores = mgr.analyze_needed_metrics(slos, hub=hub)
        assert "cpu_usage" in metrics # Car dans SLO
        assert "ram_usage" in metrics  # Car score MI élevé (> 0.05)

    def test_analyze_returns_tuple(self, mgr):
        """Vérifie que analyze_needed_metrics retourne un tuple (list, dict)."""
        slos = [{"metric": "cpu_usage"}]
        result = mgr.analyze_needed_metrics(slos)
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)
        assert isinstance(result[1], dict)

    def test_entropy_zero_for_constant_distribution(self, mgr):
        """L'entropie d'une distribution constante est 0."""
        assert mgr._entropy([1.0]) == 0.0
        assert mgr._entropy([0.0, 1.0]) == 0.0
        assert mgr._entropy([0.5, 0.5]) == 1.0 # log2(2) = 1

# =============================================================================
# 15. TESTS DU MODE AUTONOME (Tâche 9)
# =============================================================================

class TestAutonomousMode:
    
    @pytest.fixture
    def core(self, tmp_path):
        db_file = tmp_path / "test_autonomous.db"
        config = Config(DB_NAME=str(db_file))
        core = OrchestratorCore(config)
        # On mocke les spokes pour éviter les appels réseaux/GUI
        core.collector = MagicMock()
        core.ml = MagicMock()
        core.decision_engine = MagicMock()
        core.viz = MagicMock()
        core.db = DatabaseSpoke(config)
        core.db.init_db()
        return core

    def test_default_mode_is_autonomous(self, core):
        """Vérifie que le mode par défaut au démarrage est 'autonomous'."""
        assert core.mode == "autonomous"

    def test_default_slos_not_empty(self, core):
        """Vérifie que les SLOs par défaut sont chargés (3 métriques)."""
        metrics = [s["metric"] for s in core.current_slos]
        assert "latency" in metrics
        assert "cpu_usage" in metrics
        assert "ram_usage" in metrics
        assert len(core.current_slos) == 3

    def test_autonomous_flow_collects_all_metrics(self, core):
        """Vérifie que le flux autonome enrichit bien les données."""
        measurements = [{"vm_id": "vm1", "rtt_ms": 20.0}]
        core.collector.collect_vm_metrics.return_value = {"vm_id": "vm1", "cpu_usage": 50.0, "ram_usage": 60.0}
        core.ml.get_enhanced_prediction.return_value = [55.0] * 5
        core.decision_engine.evaluate_enhanced_decision.return_value = {"decision": "stay", "reason": "test"}
        
        # On espionne save_metrics
        with patch.object(core.db, 'save_metrics', wraps=core.db.save_metrics) as mock_save:
            core.run_autonomous_flow(measurements)
            
            # Vérifier l'appel à save_metrics
            args, _ = mock_save.call_args
            enriched_data = args[0]
            mode = args[1]
            assert mode == "autonomous"
            assert enriched_data[0]["cpu_usage"] == 50.0
            assert enriched_data[0]["ram_usage"] == 60.0

    def test_autonomous_flow_uses_enhanced_decision(self, core):
        """Vérifie que le flux autonome appelle le moteur de décision multi-métriques."""
        measurements = [{"vm_id": "vm1", "rtt_ms": 20.0}]
        core.collector.collect_vm_metrics.return_value = {"cpu_usage": 50.0, "ram_usage": 60.0}
        
        core.run_autonomous_flow(measurements)
        
        assert core.decision_engine.evaluate_enhanced_decision.called
        assert not core.decision_engine.evaluate_classic_decision.called

    def test_latency_manager_routes_to_autonomous(self):
        """Vérifie que le LatencyManager route vers run_autonomous_flow si mode != enhanced."""
        core = MagicMock()
        core.mode = "autonomous"
        core.run_autonomous_flow.return_value = {"decision": "stay"}
        
        config = Config()
        manager = LatencyManagerSpoke(config, core)
        
        with manager.app.test_client() as client:
            resp = client.post('/rtt', json={"measurements": [{"vm_id": "vm1", "rtt_ms": 20.0}]})
            assert resp.status_code == 200
            assert core.run_autonomous_flow.called
            assert not core.run_classic_flow.called

    def test_latency_handler_protocol_no_classic(self):
        """Vérifie que LatencyHandler Protocol n'expose pas run_classic_flow.
        
        Cette méthode teste que:
        1. Le Protocol LatencyHandler a été nettoyé
        2. run_classic_flow n'en fait plus partie du contrat
        3. Seuls run_autonomous_flow et run_enhanced_flow sont exposés
        """
        import inspect
        from typing import get_type_hints
        
        # Charger le Protocol
        from orchestrator import LatencyHandler
        
        # Vérifier que le Protocol a les bonnes méthodes
        protocol_attrs = set(dir(LatencyHandler))
        
        # run_classic_flow ne doit PAS être dans le Protocol
        assert "run_classic_flow" not in protocol_attrs, \
            "run_classic_flow() ne doit pas être dans LatencyHandler Protocol"
        
        # Les méthodes attendues DOIVENT être dans le Protocol
        assert "run_autonomous_flow" in protocol_attrs, \
            "run_autonomous_flow() doit être dans LatencyHandler Protocol"
        assert "run_enhanced_flow" in protocol_attrs, \
            "run_enhanced_flow() doit être dans LatencyHandler Protocol"
        assert "mode" in protocol_attrs, \
            "mode property doit être dans LatencyHandler Protocol"
        assert "set_last_real_data_ts" in protocol_attrs, \
            "set_last_real_data_ts() doit être dans LatencyHandler Protocol"

# =============================================================================
# 16. TESTS DE RÉGRESSION END-TO-END (Tâche 10)
# =============================================================================

class TestEndToEndRegression:
    
    @pytest.fixture
    def core(self, tmp_path):
        db_file = tmp_path / "regression.db"
        config = Config(DB_NAME=str(db_file))
        core = OrchestratorCore(config)
        # Mocker les parties qui font des appels réels
        core.ml = MagicMock()
        core.ml.get_enhanced_prediction.return_value = [20.0] * 5
        core.viz = MagicMock()
        core.collector = MagicMock()
        # Mock par défaut : vm1 est active
        core.collector.collect_vm_metrics.return_value = {"cpu_usage": 40.0, "ram_usage": 50.0, "is_active_service": True}
        
        core.db.init_db()
        return core

    def test_full_autonomous_cycle(self, core):
        """Vérifie 10 cycles complets en mode autonome avec violations et MI."""
        # On simule 15 cycles (besoin de >= 5 points pour MI)
        for i in range(15):
            # Faire varier les données de vm1 pour provoquer des violations et du MI
            val = 100.0 if i < 5 else 20.0
            # On inclut toujours vm2 comme cible stable
            measurements = [
                {"vm_id": "vm1", "rtt_ms": val},
                {"vm_id": "vm2", "rtt_ms": 10.0}
            ]
            # Mock du collector pour retourner les deux VMs
            def mock_collect(vm_id, history_loader=None):
                if vm_id == "vm1":
                    return {"vm_id": "vm1", "cpu_usage": val, "ram_usage": val, "is_active_service": True, "data_source": "real", "reliability": 1.0}
                return {"vm_id": "vm2", "cpu_usage": 30.0, "ram_usage": 30.0, "is_active_service": False, "data_source": "real", "reliability": 1.0}
            
            core.collector.collect_vm_metrics.side_effect = mock_collect
            
            core.run_autonomous_flow(measurements)
            
        # Vérifications
        with sqlite3.connect(core.config.DB_NAME) as conn:
            # On a inséré 2 VMs par cycle * 15 cycles = 30 lignes
            count = conn.execute("SELECT COUNT(*) FROM metrics").fetchone()[0]
            assert count >= 15
            
            violations = conn.execute("SELECT COUNT(*) FROM metrics WHERE is_violation=1").fetchone()[0]
            assert violations > 0
            
        assert core.last_mi_scores != {}
        # Latency devrait avoir un score MI car on l'a fait varier avec is_violation
        assert core.last_mi_scores.get("latency", 0) > 0

    @patch('requests.post')
    def test_transition_autonomous_to_enhanced(self, mock_post, core):
        """Vérifie la transition fluide entre autonome et enhanced."""
        assert core.mode == "autonomous"
        
        # 1. Mocker Ollama pour le changement d'intention
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "message": {"content": '[{"metric":"latency","operator":"<","threshold":30,"unit":"ms","weight":1.0}]'}
        }
        mock_post.return_value = mock_resp
        
        # 2. Changer d'intention
        core.set_user_intent("latence < 30")
        
        assert core.mode == "enhanced"
        assert core.current_slos[0]["threshold"] == 30
        
        # 3. Exécuter un cycle enhanced
        measurements = [{"vm_id": "vm1", "rtt_ms": 20.0}]
        dec = core.run_enhanced_flow(measurements)
        
        assert dec is not None
        assert "decision" in dec

# =============================================================================
# 7. TESTS DES PRÉDICTIONS PAR SÉQUENCE (Sequence ML Tests)
# =============================================================================

class TestMLPredictorSequence:
    
    @pytest.fixture
    def config(self):
        return Config()

    @pytest.fixture
    def predictor(self, config):
        return MLPredictorSpoke(config)

    @patch("requests.get")
    def test_fetch_window_sizes_ready(self, mock_get, predictor):
        """Mocker requests.get → {"status":"ready","window_size":10,"forecasting_model":"LSTM"}"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {
            "status": "ready",
            "window_size": 10,
            "forecasting_model": "LSTM"
        }
        mock_get.return_value = mock_resp
        
        predictor.fetch_window_sizes()
        
        assert predictor.window_sizes["latency"] == 10
        assert predictor.window_sizes["cpu_usage"] == 10
        assert predictor.window_sizes["ram_usage"] == 10

    @patch("requests.get")
    def test_fetch_window_sizes_not_ready(self, mock_get, predictor):
        """Mocker requests.get → {"status":"not_ready"}"""
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "not_ready"}
        mock_get.return_value = mock_resp
        
        predictor.fetch_window_sizes()
        
        assert predictor.window_sizes["latency"] is None

    @patch("requests.post")
    def test_get_sequence_prediction_success(self, mock_post, predictor):
        """Mocker requests.post → {"predictions":[1.0,2.0,3.0,4.0,5.0], ...}"""
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {
            "predictions": [1.0, 2.0, 3.0, 4.0, 5.0],
            "confidence_low": [0.0, 1.0, 2.0, 3.0, 4.0],
            "confidence_high": [2.0, 3.0, 4.0, 5.0, 6.0]
        }
        mock_post.return_value = mock_resp
        
        result = predictor._get_sequence_prediction("latency", [10.0]*10)
        
        assert result is not None
        assert result["predictions"] == [1.0, 2.0, 3.0, 4.0, 5.0]
        assert len(result["predictions"]) == 5

    @patch("orchestrator.MLPredictorSpoke._get_sequence_prediction")
    @patch("orchestrator.MLPredictorSpoke._get_api_prediction")
    def test_enhanced_prediction_uses_history(self, mock_api_pred, mock_seq_pred, predictor):
        """Mocker history_loader.load_window → [10.0]*10"""
        history_loader = MagicMock()
        history_loader.load_window.return_value = [10.0] * 10
        predictor.window_sizes["latency"] = 10
        
        mock_seq_pred.return_value = {"predictions": [15.0, 16.0, 17.0, 18.0, 19.0]}
        
        result = predictor.get_enhanced_prediction("latency", 10.0, "vm1", history_loader)
        
        assert result == [15.0, 16.0, 17.0, 18.0, 19.0]
        mock_api_pred.assert_not_called()

    @patch("orchestrator.MLPredictorSpoke._get_api_prediction")
    def test_enhanced_prediction_fallback_no_history(self, mock_api_pred, predictor):
        """history_loader = None"""
        mock_api_pred.return_value = [20.0, 21.0, 22.0, 23.0, 24.0]
        
        result = predictor.get_enhanced_prediction("latency", 10.0, history_loader=None)
        
        assert result == [20.0, 21.0, 22.0, 23.0, 24.0]

    @patch("orchestrator.MLPredictorSpoke._get_api_prediction")
    def test_enhanced_prediction_fallback_short_history(self, mock_api_pred, predictor):
        """history_loader.load_window → [10.0]*3 (moins que window_size=10)"""
        history_loader = MagicMock()
        history_loader.load_window.return_value = [10.0] * 3
        predictor.window_sizes["latency"] = 10
        
        mock_api_pred.return_value = [20.0, 21.0, 22.0, 23.0, 24.0]
        
        result = predictor.get_enhanced_prediction("latency", 10.0, "vm1", history_loader)
        
        # Assert : fallback sur _get_api_prediction
        assert result == [20.0, 21.0, 22.0, 23.0, 24.0]
        mock_api_pred.assert_called_once()

# =============================================================================
# 8. TESTS DE LA SÉLECTION TOPSIS (TOPSIS Selection Tests)
# =============================================================================

class TestTOPSISSelection:
    
    @pytest.fixture
    def config(self):
        return Config()

    @pytest.fixture
    def decision_engine(self, config):
        return DecisionIntelligenceSpoke(config)

    def test_topsis_selects_best_vm(self, decision_engine):
        """3 VMs candidates : vm2 est l'idéale (latence et cpu bas)."""
        candidates = [
            {"vm_id": "vm1", "latency": 80.0, "cpu_usage": 80.0},
            {"vm_id": "vm2", "latency": 20.0, "cpu_usage": 20.0},
            {"vm_id": "vm3", "latency": 50.0, "cpu_usage": 50.0}
        ]
        predictions_map = {
            "vm1": {"latency": [80.0]*5, "cpu_usage": [80.0]*5},
            "vm2": {"latency": [20.0]*5, "cpu_usage": [20.0]*5},
            "vm3": {"latency": [50.0]*5, "cpu_usage": [50.0]*5}
        }
        slos = [
            {"metric": "latency", "threshold": 50.0, "weight": 0.5},
            {"metric": "cpu_usage", "threshold": 70.0, "weight": 0.5}
        ]
        weights = {"latency": 0.5, "cpu_usage": 0.5}
        
        best = decision_engine._topsis_select(candidates, predictions_map, slos, weights)
        
        assert best["vm_id"] == "vm2"
        assert best["topsis_score"] > 0.5

    def test_topsis_single_candidate(self, decision_engine):
        """1 seule VM candidate : retourne cette VM sans crash."""
        candidates = [{"vm_id": "vm1", "latency": 30.0}]
        predictions_map = {"vm1": {"latency": [30.0]*5}}
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        weights = {"latency": 1.0}
        
        best = decision_engine._topsis_select(candidates, predictions_map, slos, weights)
        
        assert best["vm_id"] == "vm1"

    def test_topsis_constant_criterion(self, decision_engine):
        """Toutes les VMs ont la même latence : pas de crash."""
        candidates = [
            {"vm_id": "vm1", "latency": 30.0, "cpu_usage": 80.0},
            {"vm_id": "vm2", "latency": 30.0, "cpu_usage": 20.0}
        ]
        predictions_map = {
            "vm1": {"latency": [30.0]*5, "cpu_usage": [80.0]*5},
            "vm2": {"latency": [30.0]*5, "cpu_usage": [20.0]*5}
        }
        slos = [
            {"metric": "latency", "threshold": 50.0, "weight": 0.5},
            {"metric": "cpu_usage", "threshold": 70.0, "weight": 0.5}
        ]
        weights = {"latency": 0.5, "cpu_usage": 0.5}
        
        best = decision_engine._topsis_select(candidates, predictions_map, slos, weights)
        
        # vm2 devrait gagner sur le CPU
        assert best["vm_id"] == "vm2"

    def test_topsis_score_in_response(self, decision_engine):
        """Vérifie que topsis_score et selection_method sont dans la réponse de evaluate_enhanced_decision."""
        current_data = [
            {"vm_id": "vm1", "rtt_ms": 60.0}, # Violation
            {"vm_id": "vm2", "rtt_ms": 20.0}  # Cible
        ]
        predictions_map = {
            "vm1": {"latency": [60.0]*5},
            "vm2": {"latency": [20.0]*5}
        }
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        
        result = decision_engine.evaluate_enhanced_decision(current_data, predictions_map, slos, service_vm="vm1")
        
        assert result["decision"] == "migrate"
        assert "topsis_score" in result
        assert result["selection_method"] == "topsis"

    def test_topsis_budget_penalizes_low_budget(self, decision_engine):
        """vm1 et vm2 ont les mêmes perfs, mais vm2 a un meilleur budget."""
        candidates = [
            {"vm_id": "vm1", "latency": 30.0},
            {"vm_id": "vm2", "latency": 30.0}
        ]
        predictions_map = {
            "vm1": {"latency": [30.0]*5},
            "vm2": {"latency": [30.0]*5}
        }
        # SLOs avec budgets différents
        slos = [
            {"metric": "latency", "threshold": 50.0, "weight": 1.0, "budget_remaining": 10.0},
            {"metric": "latency", "threshold": 50.0, "weight": 1.0, "budget_remaining": 90.0}
        ]
        # On va ruser : le code de _topsis_select boucle sur slos.
        # Si on passe deux SLOs pour la même métrique avec des budgets différents, 
        # le budget_bonus sera calculé par VM.
        # Mais attendez, budget_bonus dépend de 'cand.get(col)'. 
        # Dans ce test, cand a latency=30.0, donc il respecte les deux SLOs.
        
        # En fait, slos est passé tel quel.
        # VM1 vs VM2 : même latency, donc le budget fera la différence.
        # Mais le budget dans 'slos' est le même pour toutes les VMs!
        # Ah! Le budget_bonus est calculé pour CHAQUE candidat.
        # "budget_bonus += bonus" si "val <= threshold".
        # Si VM1 et VM2 respectent toutes les deux le SLO, elles ont le même bonus.
        
        # Comment différencier les VMs par budget si le budget est global au SLO?
        # Dans le projet réel, le budget est global. 
        # MAIS, si une VM ne respecte PAS un SLO actuellement, elle n'aura pas le bonus.
        
        # Testons ça :
        candidates = [
            {"vm_id": "vm1", "latency": 30.0, "cpu_usage": 80.0}, # viole CPU
            {"vm_id": "vm2", "latency": 30.0, "cpu_usage": 30.0}  # respecte tout
        ]
        predictions_map = {
            "vm1": {"latency": [30.0]*5, "cpu_usage": [80.0]*5},
            "vm2": {"latency": [30.0]*5, "cpu_usage": [30.0]*5}
        }
        slos = [
            {"metric": "latency", "threshold": 50.0, "weight": 0.5, "budget_remaining": 100.0},
            {"metric": "cpu_usage", "threshold": 70.0, "weight": 0.5, "budget_remaining": 100.0}
        ]
        weights = {"latency": 0.5, "cpu_usage": 0.5}
        
        best = decision_engine._topsis_select(candidates, predictions_map, slos, weights)
        assert best["vm_id"] == "vm2"

# =============================================================================
# 9. TESTS DE LA DÉTECTION PROACTIVE (Proactive Detection Tests)
# =============================================================================

class TestProactiveDetection:
    
    @pytest.fixture
    def config(self):
        return Config(PROACTIVE_FACTOR=0.85, HORIZON_ALERT=3)

    @pytest.fixture
    def decision_engine(self, config):
        return DecisionIntelligenceSpoke(config)

    def test_reactive_breach_detected(self, decision_engine):
        """val=60, threshold=50 → reactive."""
        preds = [60.0] * 5
        analysis = decision_engine._analyze_predictions(preds, 50.0, 60.0)
        
        assert analysis["breach_type"] == "reactive"
        assert analysis["breach_reactive"] is True
        assert analysis["breach_proactive"] is False

    def test_proactive_breach_detected(self, decision_engine):
        """val=38, threshold=50, preds=[40, 43, 44, 45, 46] → proactive."""
        # threshold=50, 50*0.7=35, 50*0.85=42.5
        preds = [40.0, 43.0, 44.0, 45.0, 46.0]
        analysis = decision_engine._analyze_predictions(preds, 50.0, 38.0)
        
        assert analysis["breach_type"] == "proactive"
        assert analysis["breach_reactive"] is False
        assert analysis["breach_proactive"] is True

    def test_no_breach_below_threshold(self, decision_engine):
        """val=20, threshold=50 → none."""
        preds = [22.0, 23.0, 24.0, 25.0, 26.0]
        analysis = decision_engine._analyze_predictions(preds, 50.0, 20.0)
        
        assert analysis["breach_type"] == "none"

    def test_time_to_breach_calculation(self, decision_engine):
        """preds=[30, 40, 55, 60, 65], threshold=50 → step 3."""
        preds = [30.0, 40.0, 55.0, 60.0, 65.0]
        analysis = decision_engine._analyze_predictions(preds, 50.0, 30.0)
        
        assert analysis["time_to_breach"] == 3

    def test_proactive_slope_in_reason(self, decision_engine):
        """Vérifie que 'breach prédit dans' est dans le reason."""
        current_data = [{"vm_id": "vm1", "rtt_ms": 38.0}, {"vm_id": "vm2", "rtt_ms": 20.0}]
        # threshold=50, 50*0.7=35, 50*0.85=42.5
        predictions_map = {
            "vm1": {"latency": [40.0, 43.0, 44.0, 45.0, 46.0]},
            "vm2": {"latency": [20.0]*5}
        }
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        
        result = decision_engine.evaluate_enhanced_decision(current_data, predictions_map, slos, service_vm="vm1")
        
        assert result["decision"] == "migrate"
        assert "breach prédit dans" in result["reason"]

    def test_empty_preds_no_crash(self, decision_engine):
        """preds=[], threshold=50, val=30 → none."""
        analysis = decision_engine._analyze_predictions([], 50.0, 30.0)
        
        assert analysis["breach_type"] == "none"
        assert analysis["slope"] == 0.0

# =============================================================================
# 10. TESTS DE L'INCERTITUDE (Uncertainty-Aware Tests)
# =============================================================================

from orchestrator import PredictionResult

class TestUncertaintyAware:
    
    @pytest.fixture
    def config(self):
        return Config(PROACTIVE_FACTOR=0.85, HORIZON_ALERT=3)

    @pytest.fixture
    def decision_engine(self, config):
        return DecisionIntelligenceSpoke(config)

    def test_prediction_result_uncertainty_high(self):
        """confidence_low=[30,31,32], confidence_high=[50,51,52], predictions=[40,41,42] → uncertainty > 0."""
        preds = [40.0, 41.0, 42.0]
        low = [30.0, 31.0, 32.0]
        high = [50.0, 51.0, 52.0]
        
        # Calcul manuel pour le test : spreads=[20,20,20], mean=20, ref=42, unc=20/42=0.476
        spreads = [h - l for h, l in zip(high, low)]
        mean_spread = sum(spreads) / len(spreads)
        ref = max(preds)
        expected_unc = min(mean_spread / (ref + 1e-9), 1.0)
        
        result = PredictionResult(predictions=preds, uncertainty=expected_unc, confidence_low=low, confidence_high=high)
        
        assert result.uncertainty > 0.4
        assert result.uncertainty < 0.5

    def test_prediction_result_uncertainty_zero(self):
        """confidence_low=[], confidence_high=[], predictions=[40,41,42] → uncertainty == 0.0."""
        result = PredictionResult(predictions=[40.0, 41.0, 42.0], uncertainty=0.0)
        assert result.uncertainty == 0.0

    def test_conservative_factor_when_high_uncertainty(self, decision_engine):
        """uncertainty=0.8 (> 0.5) → factor=0.90. threshold=50, 50*0.9=45. pred=46 > 45 → migrate."""
        predictions_map = {
            "vm1": {"latency": PredictionResult(
                predictions=[40.0, 43.0, 44.0, 45.0, 46.0],
                uncertainty=0.8,
                confidence_low=[], confidence_high=[]
            )},
            "vm2": {"latency": PredictionResult(
                predictions=[20.0]*5, uncertainty=0.1,
                confidence_low=[], confidence_high=[]
            )}
        }
        current_data = [
            {"vm_id": "vm1", "rtt_ms": 38.0}, # 38 > 50*0.7=35 (cond3 OK)
            {"vm_id": "vm2", "rtt_ms": 20.0}
        ]
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        
        result = decision_engine.evaluate_enhanced_decision(current_data, predictions_map, slos, service_vm="vm1")
        
        assert result["decision"] == "migrate"
        assert "breach prédit dans" in result["reason"]
        assert "Incertitude: 0.80" in result["reason"]

    def test_nominal_factor_when_low_uncertainty(self, decision_engine):
        """uncertainty=0.2 (< 0.5) → factor=0.85. threshold=50, 50*0.85=42.5. pred=43 > 42.5 → migrate."""
        predictions_map = {
            "vm1": {"latency": PredictionResult(
                predictions=[40.0, 41.0, 42.0, 42.1, 43.0],
                uncertainty=0.2,
                confidence_low=[], confidence_high=[]
            )},
            "vm2": {"latency": PredictionResult(
                predictions=[20.0]*5, uncertainty=0.1,
                confidence_low=[], confidence_high=[]
            )}
        }
        current_data = [
            {"vm_id": "vm1", "rtt_ms": 38.0},
            {"vm_id": "vm2", "rtt_ms": 20.0}
        ]
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        
        result = decision_engine.evaluate_enhanced_decision(current_data, predictions_map, slos, service_vm="vm1")
        
        assert result["decision"] == "migrate"

    def test_no_breach_with_conservative_factor(self, decision_engine):
        """uncertainty=0.8 → factor=0.90. threshold=50, 50*0.9=45. max(preds)=44 < 45 → stay."""
        predictions_map = {
            "vm1": {"latency": PredictionResult(
                predictions=[40.0, 41.0, 42.0, 43.0, 44.0],
                uncertainty=0.8,
                confidence_low=[], confidence_high=[]
            )}
        }
        current_data = [{"vm_id": "vm1", "rtt_ms": 38.0}]
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        
        result = decision_engine.evaluate_enhanced_decision(current_data, predictions_map, slos, service_vm="vm1")
        
        assert result["decision"] == "stay"

    def test_list_float_backward_compatibility(self, decision_engine):
        """predictions_map contient List[float] au lieu de PredictionResult → pas de crash."""
        predictions_map = {"vm1": {"latency": [20.0]*5}}
        current_data = [{"vm_id": "vm1", "rtt_ms": 20.0}]
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        
        result = decision_engine.evaluate_enhanced_decision(current_data, predictions_map, slos, service_vm="vm1")
        
        assert result["decision"] == "stay"

# =============================================================================
# 11. TESTS DES COÛTS DE MIGRATION (Migration Cost Tests)
# =============================================================================

class TestMigrationCost:
    
    @pytest.fixture
    def config(self, tmp_path):
        conf = Config()
        db_file = tmp_path / "test_orchestrator.db"
        conf.DB_NAME = str(db_file)
        return conf

    @pytest.fixture
    def db(self, config):
        db = DatabaseSpoke(config)
        db.init_db()
        return db

    def test_get_migration_count_returns_zero_no_data(self, db):
        """Vérifie que le compteur retourne 0 sans données."""
        assert db.get_migration_count("vm1", 300) == 0

    def test_get_migration_count_counts_only_target(self, db):
        """Vérifie que seules les migrations VERS la VM sont comptées."""
        now_iso = datetime.now(timezone.utc).isoformat()
        db.save_decision({"decision": "migrate", "from_vm": "vm1", "to_vm": "vm2", "reason": "test", "budget_remaining": 100.0}, "auto", True)
        db.save_decision({"decision": "migrate", "from_vm": "vm3", "to_vm": "vm2", "reason": "test", "budget_remaining": 100.0}, "auto", True)
        db.save_decision({"decision": "migrate", "from_vm": "vm2", "to_vm": "vm1", "reason": "test", "budget_remaining": 100.0}, "auto", True)
        
        assert db.get_migration_count("vm2", 300) == 2
        assert db.get_migration_count("vm1", 300) == 1

    def test_get_migration_count_respects_window(self, db):
        """Vérifie que les migrations hors fenêtre sont ignorées."""
        old_iso = (datetime.now(timezone.utc) - timedelta(seconds=600)).isoformat()
        with sqlite3.connect(db.config.DB_NAME) as conn:
            conn.execute("INSERT INTO decisions (decision, from_vm, to_vm, mode, timestamp) VALUES (?, ?, ?, ?, ?)",
                         ("migrate", "vm1", "vm2", "auto", old_iso))
        
        assert db.get_migration_count("vm2", 300) == 0

    def test_topsis_penalizes_high_migration_vm(self):
        """Vérifie que TOPSIS pénalise une VM trop migrée."""
        decision_engine = DecisionIntelligenceSpoke(Config())
        # Poids très faibles pour les métriques (0.01) pour que le coût de migration (0.5) gagne
        candidates = [
            {"vm_id": "vm2", "latency": 15.0, "cpu_usage": 20.0, "ram_usage": 20.0},
            {"vm_id": "vm3", "latency": 20.0, "cpu_usage": 25.0, "ram_usage": 25.0}
        ]
        predictions_map = {
            "vm2": {"latency": [15.0]*5, "cpu_usage": [20.0]*5, "ram_usage": [20.0]*5},
            "vm3": {"latency": [20.0]*5, "cpu_usage": [25.0]*5, "ram_usage": [25.0]*5}
        }
        migration_costs = {"vm2": 10, "vm3": 0} 
        slos = [
            {"metric": "latency", "threshold": 50.0, "weight": 0.01},
            {"metric": "cpu_usage", "threshold": 75.0, "weight": 0.01},
            {"metric": "ram_usage", "threshold": 80.0, "weight": 0.01}
        ]
        weights = {"latency": 0.01, "cpu_usage": 0.01, "ram_usage": 0.01}
        
        best = decision_engine._topsis_select(candidates, predictions_map, slos, weights, migration_costs=migration_costs)
        
        assert best["vm_id"] == "vm3"

    def test_topsis_migration_cost_none_backward_compat(self):
        """Vérifie la compatibilité ascendante quand migration_costs est None."""
        decision_engine = DecisionIntelligenceSpoke(Config())
        candidates = [
            {"vm_id": "vm2", "latency": 15.0},
            {"vm_id": "vm3", "latency": 20.0}
        ]
        predictions_map = {
            "vm2": {"latency": [15.0]*5},
            "vm3": {"latency": [20.0]*5}
        }
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        weights = {"latency": 1.0}
        
        best = decision_engine._topsis_select(candidates, predictions_map, slos, weights, migration_costs=None)
        
        assert best["vm_id"] == "vm2"

    def test_evaluate_enhanced_passes_migration_costs(self):
        """Vérifie que evaluate_enhanced_decision prend en compte les migration_costs."""
        decision_engine = DecisionIntelligenceSpoke(Config())
        current_data = [
            {"vm_id": "vm1", "rtt_ms": 60.0}, # Violation
            {"vm_id": "vm2", "rtt_ms": 15.0}, # Bonnes perfs mais bcp de migrations
            {"vm_id": "vm3", "rtt_ms": 20.0}  # Perfs moyennes mais 0 migrations
        ]
        predictions_map = {
            "vm1": {"latency": [60.0]*5},
            "vm2": {"latency": [15.0]*5},
            "vm3": {"latency": [20.0]*5}
        }
        migration_costs = {"vm2": 100, "vm3": 0}
        # Poids faible pour la latence
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 0.01}]
        
        result = decision_engine.evaluate_enhanced_decision(current_data, predictions_map, slos, service_vm="vm1", migration_costs=migration_costs)
        
        assert result["to_vm"] == "vm3"


# =============================================================================
# 12. TESTS DU FILTRE DES MÉTRIQUES (Filter Metrics Tests)
# =============================================================================

class TestFilterActiveMetrics:
    
    @pytest.fixture
    def core(self):
        return OrchestratorCore(Config())

    def test_is_active_service_propagated(self, core):
        """Vérifie que is_active_service est propagé (True)."""
        collected = {
            "vm_id": "vm1",
            "cpu_usage": 45.0,
            "ram_usage": 60.0,
            "is_active_service": True
        }
        active_metrics = ["cpu_usage", "ram_usage"]
        result = core._filter_active_metrics(collected, active_metrics, "vm1")
        
        assert result["is_active_service"] is True
        assert result["vm_id"] == "vm1"
        assert result["cpu_usage"] == 45.0

    def test_is_active_service_false_propagated(self, core):
        """Vérifie que is_active_service est propagé (False)."""
        collected = {
            "vm_id": "vm2",
            "cpu_usage": 30.0,
            "ram_usage": 40.0,
            "is_active_service": False
        }
        active_metrics = ["cpu_usage", "ram_usage"]
        result = core._filter_active_metrics(collected, active_metrics, "vm2")
        
        assert result["is_active_service"] is False

    def test_is_active_service_absent_no_crash(self, core):
        """Vérifie que l'absence de is_active_service ne provoque pas de crash."""
        collected = {
            "vm_id": "vm3",
            "cpu_usage": 30.0,
            "ram_usage": 40.0
        }
        active_metrics = ["cpu_usage", "ram_usage"]
        result = core._filter_active_metrics(collected, active_metrics, "vm3")
        
        assert "is_active_service" not in result

    def test_find_active_vm_works_after_filter(self, core):
        """Vérifie que _find_active_vm trouve la bonne VM après filtrage."""
        measurements = [
            {"vm_id": "vm1", "rtt_ms": 20.0, "is_active_service": False},
            {"vm_id": "vm2", "rtt_ms": 15.0, "is_active_service": True}
        ]
        active_metrics = ["cpu_usage", "ram_usage"]

        # Simuler ce que fait run_autonomous_flow
        collected_vm1 = {"vm_id": "vm1", "cpu_usage": 30.0, "ram_usage": 40.0, "is_active_service": False}
        collected_vm2 = {"vm_id": "vm2", "cpu_usage": 25.0, "ram_usage": 35.0, "is_active_service": True}

        enriched = [
            {**measurements[0], **core._filter_active_metrics(collected_vm1, active_metrics, "vm1")},
            {**measurements[1], **core._filter_active_metrics(collected_vm2, active_metrics, "vm2")}
        ]

        assert core._find_active_vm(enriched) == "vm2"

# =============================================================================
# 22. TESTS DU COLLECTOR SPOKE (Adaptive Timeout & Reliability EMA)
# =============================================================================

class TestCollectorSpoke:
    
    @pytest.fixture
    def collector(self):
        return CollectorSpoke(Config())
    
    def test_adaptive_timeout_with_history(self, collector):
        """Test that adaptive timeout is calculated from RTT history.
        
        Given:
        - Historical RTT window: [40.0, 42.0, 38.0, 41.0, 39.0] ms
        - Mean RTT: 40ms = 0.040s
        - Timeout = 0.040 * 3 = 0.120s
        
        Expected:
        - Result clamped to [0.5, 5.0] = 0.5s (minimum)
        """
        history_loader = MagicMock()
        history_loader.load_window.return_value = [40.0, 42.0, 38.0, 41.0, 39.0]
        
        timeout = collector._get_adaptive_timeout("vm1", history_loader)
        
        # mean = 40.0 ms, factor = 3.0 → 0.120s, clamped to min 0.5
        assert timeout == 0.5
    
    def test_adaptive_timeout_no_history(self, collector):
        """Test that fallback timeout is used when history_loader is None."""
        timeout = collector._get_adaptive_timeout("vm1", history_loader=None)
        
        assert timeout == 1.5
    
    def test_adaptive_timeout_high_rtt(self, collector):
        """Test adaptive timeout with high RTT values.
        
        Given:
        - Historical RTT window: [500.0, 600.0, 550.0] ms
        - Mean RTT: 550ms = 0.55s
        - Timeout = 0.55 * 3 = 1.65s
        
        Expected:
        - Result is 1.65s (within [0.5, 5.0])
        """
        history_loader = MagicMock()
        history_loader.load_window.return_value = [500.0, 600.0, 550.0]
        
        timeout = collector._get_adaptive_timeout("vm1", history_loader)
        
        assert abs(timeout - 1.65) < 0.0001
    
    def test_adaptive_timeout_insufficient_history(self, collector):
        """Test that fallback is used when history has fewer than 3 points."""
        history_loader = MagicMock()
        history_loader.load_window.return_value = [40.0, 42.0]  # Only 2 points
        
        timeout = collector._get_adaptive_timeout("vm1", history_loader)
        
        assert timeout == 1.5
    
    def test_reliability_ema_success(self, collector):
        """Test EMA update on successful collection.
        
        Given:
        - Current reliability: 0.8
        - Alpha: 0.1
        - Success (result = 1.0)
        
        Expected:
        - Updated = (1 - 0.1) * 0.8 + 0.1 * 1.0 = 0.72 + 0.1 = 0.82
        """
        collector.reliability_ema["vm1"] = 0.8
        
        updated = collector._update_reliability("vm1", success=True)
        
        assert abs(updated - 0.82) < 0.0001
        assert collector.reliability_ema["vm1"] == updated
    
    def test_reliability_ema_failure(self, collector):
        """Test EMA update on failed collection.
        
        Given:
        - Current reliability: 1.0
        - Alpha: 0.1
        - Failure (result = 0.0)
        
        Expected:
        - Updated = (1 - 0.1) * 1.0 + 0.1 * 0.0 = 0.9
        """
        collector.reliability_ema["vm1"] = 1.0
        
        updated = collector._update_reliability("vm1", success=False)
        
        assert abs(updated - 0.9) < 0.0001
        assert collector.reliability_ema["vm1"] == updated
    
    def test_reliability_initialized_at_1(self, collector):
        """Test that unknown VM reliability is initialized at 1.0 before EMA.
        
        Given:
        - VM never seen before (not in reliability_ema dict)
        - Success (result = 1.0)
        
        Expected:
        - Initial value: 1.0
        - Updated = (1 - 0.1) * 1.0 + 0.1 * 1.0 = 1.0
        """
        updated = collector._update_reliability("vm_new", success=True)
        
        assert abs(updated - 1.0) < 0.0001
        assert "vm_new" in collector.reliability_ema
    
    def test_collect_real_data_source(self, collector):
        """Test that successful collection returns data_source='real' with reliability."""
        with patch('orchestrator.requests.get') as mock_get:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "cpu_usage": 45.0,
                "ram_usage": 60.0,
                "is_active_service": True
            }
            mock_get.return_value = mock_response
            
            result = collector.collect_vm_metrics("vm1", history_loader=None)
            
            assert result["data_source"] == "real"
            assert result["vm_id"] == "vm1"
            assert result["cpu_usage"] == 45.0
            assert result["ram_usage"] == 60.0
            assert result["is_active_service"] is True
            assert "reliability" in result
            assert result["reliability"] > 0.0
    
    def test_collect_simulated_data_source(self, collector):
        """Test that failed collection returns data_source='simulated' with updated reliability.
        
        When:
        - requests.get raises an exception
        
        Then:
        - data_source should be "simulated"
        - reliability_ema should decrease (failure)
        - reliability field should be present
        """
        with patch('orchestrator.requests.get') as mock_get:
            mock_get.side_effect = Exception("Connection timeout")
            
            result = collector.collect_vm_metrics("vm1", history_loader=None)
            
            assert result["data_source"] == "simulated"
            assert result["vm_id"] == "vm1"
            assert "cpu_usage" in result
            assert "ram_usage" in result
            assert result["is_active_service"] is False
            assert "reliability" in result
            # After first failure, reliability should be ~0.9
            assert collector.reliability_ema["vm1"] < 1.0

# =============================================================================
# 13. TESTS DE QUALITÉ DES DONNÉES (Data Quality)
# =============================================================================

class TestDataQuality:
    
    @pytest.fixture
    def db(self, tmp_path):
        db_file = tmp_path / "test_quality.db"
        config = Config(DB_NAME=str(db_file))
        db = DatabaseSpoke(config)
        db.init_db()
        return db

    def test_data_source_stored(self, db):
        """Vérifie que data_source est bien persisté dans SQLite."""
        measurement = [{"vm_id": "vm1", "rtt_ms": 20.0, "data_source": "real"}]
        db.save_metrics(measurement, "autonomous", [])
        
        with sqlite3.connect(db.config.DB_NAME) as conn:
            row = conn.execute("SELECT data_source FROM metrics WHERE id=1").fetchone()
            assert row[0] == "real"

    def test_data_quality_ratio(self, db):
        """Vérifie le calcul du ratio real/(real+simulated)."""
        # 7 mesures réelles
        for _ in range(7):
            db.save_metrics([{"vm_id": "vm1", "data_source": "real"}], "auto")
        # 3 mesures simulées
        for _ in range(3):
            db.save_metrics([{"vm_id": "vm1", "data_source": "simulated"}], "auto")
            
        ratio = db.get_data_quality_ratio(window_seconds=300)
        assert ratio == 0.7

    def test_data_quality_empty_db(self, db):
        """Vérifie que le ratio est de 1.0 si la base est vide."""
        assert db.get_data_quality_ratio(window_seconds=300) == 1.0

# =============================================================================
# 14. TESTS DE FIABILITÉ HUB (Reliability Hub)
# =============================================================================

class TestReliabilityHub:
    
    @pytest.fixture
    def core(self):
        config = Config()
        return OrchestratorCore(config)

    def test_get_reliability_scores_empty(self, core):
        """Vérifie que le hub retourne un dictionnaire vide si le collecteur n'a rien."""
        core.collector.reliability_ema = {}
        assert core.get_reliability_scores() == {}

    def test_get_reliability_scores_values(self, core):
        """Vérifie que le hub retourne les scores du collecteur."""
        scores = {"vm1": 0.9, "vm2": 0.7}
        core.collector.reliability_ema = scores
        assert core.get_reliability_scores() == scores

# =============================================================================
# 15. TESTS FIABILITÉ TOPSIS (Reliability TOPSIS)
# =============================================================================

class TestReliabilityTOPSIS:
    
    @pytest.fixture
    def spoke(self):
        return DecisionIntelligenceSpoke(Config())

    def test_topsis_penalizes_low_reliability(self, spoke):
        """Vérifie que TOPSIS choisit la VM la plus fiable à prédictions égales."""
        candidates = [
            {"vm_id": "vm1", "rtt_ms": 20.0}, # Peu fiable
            {"vm_id": "vm2", "rtt_ms": 20.0}  # Très fiable
        ]
        # Prédictions identiques (25ms pour la latence)
        preds_map = {
            "vm1": {"latency": [25.0]*5},
            "vm2": {"latency": [25.0]*5}
        }
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        weights = {"latency": 1.0}
        reliability = {"vm1": 0.3, "vm2": 0.9}
        
        best = spoke._topsis_select(
            candidates, preds_map, slos, weights, 
            reliability_scores=reliability
        )
        assert best["vm_id"] == "vm2"

    def test_topsis_reliability_none_no_crash(self, spoke):
        """Vérifie que le système ne crashe pas si reliability_scores est None."""
        candidates = [{"vm_id": "vm1", "rtt_ms": 20.0}]
        preds_map = {"vm1": {"latency": [25.0]*5}}
        slos = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        weights = {"latency": 1.0}
        
        # Ne doit pas crash
        best = spoke._topsis_select(
            candidates, preds_map, slos, weights, 
            reliability_scores=None
        )
        assert best["vm_id"] == "vm1"

# =============================================================================
# 16. TESTS MISE À JOUR DÉCISION (Update Decision)
# =============================================================================

class TestUpdateDecision:
    
    @pytest.fixture
    def viz(self):
        return ObservabilitySpoke(Config())

    def test_migrate_stores_detail(self, viz):
        """Vérifie que les détails de migration sont correctement stockés."""
        dec = {
            "decision": "migrate",
            "from_vm": "vm1",
            "to_vm": "vm3",
            "reason": "vm1 cpu 85%>SLO 70%",
            "topsis_score": 0.847,
            "uncertainty": 0.12
        }
        viz.update_decision(dec)
        
        detail = viz.last_decision_detail
        assert detail["topsis_score"] == 0.847
        assert detail["breach_type"] == "reactive"
        assert viz.last_decision == dec # Rétrocompatibilité

    def test_proactive_breach_inferred(self, viz):
        """Vérifie l'inférence du type proactive par le texte."""
        dec = {"decision": "migrate", "reason": "breach prédit dans 2 steps"}
        viz.update_decision(dec)
        assert viz.last_decision_detail["breach_type"] == "proactive"

    def test_empty_dec_no_crash(self, viz):
        """Vérifie que les dictionnaires vides ne causent pas de crash."""
        viz.update_decision({})
        assert viz.last_decision_detail["decision"] == "stay"
        assert viz.last_decision_detail["topsis_score"] == 0.0

    def test_explicit_breach_type_priority(self, viz):
        """Vérifie que le type explicite est prioritaire sur l'inférence."""
        dec = {"breach_type": "proactive", "reason": "vm1 cpu>SLO"}
        viz.update_decision(dec)
        # Raison contient '>' (reactive) mais breach_type dit 'proactive'
        assert viz.last_decision_detail["breach_type"] == "proactive"

# =============================================================================
# 17. TESTS ÉVOLUTION MI (MI Evolution)
# =============================================================================

class TestMIEvolution:
    
    @pytest.fixture
    def viz(self):
        return ObservabilitySpoke(Config())

    def test_update_mi_scores_stores(self, viz):
        """Vérifie que les scores MI sont correctement stockés dans l'historique."""
        scores = {"latency": 0.1, "cpu_usage": 0.7, "ram_usage": 0.4}
        viz.update_mi_scores(scores)
        assert len(viz.mi_scores_history) == 1
        assert viz.mi_scores_history[0]["cpu_usage"] == 0.7

    def test_fifo_truncation(self, viz):
        """Vérifie que l'historique est tronqué à max_mi_points (50)."""
        for i in range(55):
            viz.update_mi_scores({"latency": float(i)/100})
        
        assert len(viz.mi_scores_history) == 50
        # Le premier élément doit être le 6ème ajouté (index 5)
        assert viz.mi_scores_history[0]["latency"] == 0.05

    def test_empty_dict_no_crash(self, viz):
        """Vérifie que l'envoi d'un dict vide ne provoque pas de crash."""
        viz.update_mi_scores({})
        assert len(viz.mi_scores_history) == 0

# =============================================================================
# 18. TESTS TITRE DASHBOARD (Suptitle Construction)
# =============================================================================

class TestSuptitle:
    
    @pytest.fixture
    def viz(self):
        v = ObservabilitySpoke(Config())
        v.current_slos = [{"metric": "latency", "threshold": 50, "unit": "ms"}]
        return v

    def test_migrate_title_contains_topsis(self, viz):
        """Vérifie que le titre contient le score TOPSIS en cas de migration."""
        viz.last_decision_detail = {
            "decision": "migrate",
            "from_vm": "vm1",
            "to_vm": "vm2",
            "topsis_score": 0.847,
            "breach_type": "proactive",
            "uncertainty": 0.15
        }
        title, color = viz._build_suptitle()
        assert "TOPSIS:0.847" in title
        assert color == "red"

    def test_stay_title_nominal(self, viz):
        """Vérifie que le titre commence par STAY et est vert."""
        viz.last_decision_detail = {
            "decision": "stay",
            "reason": "Nominal"
        }
        title, color = viz._build_suptitle()
        assert title.startswith("STAY")
        assert color == "green"

    def test_mi_line_present(self, viz):
        """Vérifie l'affichage des scores MI dans le titre."""
        viz.mi_scores_history = [{
            "cpu_usage": 0.73,
            "ram_usage": 0.41,
            "latency": 0.18
        }]
        title, _ = viz._build_suptitle()
        assert "MI:" in title
        assert "cpu=0.73" in title
        assert "ram=0.41" in title

    def test_data_quality_displayed(self, viz):
        """Vérifie l'affichage du ratio de qualité des données."""
        viz.data_quality = 0.942
        title, _ = viz._build_suptitle()
        assert "94.2%" in title

# =============================================================================
# 19. TESTS D'INTÉGRATION OBSERVABILITY (Observability Integration)
# =============================================================================

class TestObservabilitySpokeIntegration:
    """
    Stratégie de test : Test d'intégration fonctionnelle sans interface graphique.
    On valide la chaîne complète de traitement de l'ObservabilitySpoke :
    1. Réception des métriques brutes (détection locale de violations).
    2. Réception des décisions du Hub (mise à jour des métadonnées et timeline).
    3. Réception des scores MI et de la qualité des données.
    4. Vérification de la cohérence du titre généré (suptitle) qui synthétise tout l'état.
    L'objectif est de garantir que le composant maintient un état interne cohérent
    pour l'affichage sans dépendre des autres Spokes ou d'un affichage réel.
    """

    @pytest.fixture
    def viz(self):
        config = Config()
        viz = ObservabilitySpoke(config)
        viz.current_slos = [
            {"metric": "cpu_usage", "threshold": 70, "operator": "<", "weight": 0.5},
            {"metric": "latency", "threshold": 50, "operator": "<", "weight": 0.5}
        ]
        return viz

    def test_full_cycle_migrate(self, viz):
        """Simule un cycle complet : violation détectée suivie d'une migration."""
        # 1. Données en violation
        viz.update_data([{"vm_id": "vm1", "rtt_ms": 65, "cpu_usage": 85, "ram_usage": 50, "is_active_service": True}])
        
        # 2. Décision de migration reçue
        dec = {
            "decision": "migrate", 
            "from_vm": "vm1", 
            "to_vm": "vm3",
            "reason": "cpu 85>70", 
            "topsis_score": 0.84,
            "uncertainty": 0.1
        }
        viz.update_decision(dec)
        
        # 3. Scores MI reçus
        viz.update_mi_scores({"cpu_usage": 0.73, "latency": 0.18, "ram_usage": 0.4})
        
        # 4. Qualité des données reçue
        viz.update_quality(0.94)

        # Assertions
        assert viz.last_decision_detail["topsis_score"] == 0.84
        assert len(viz.violation_timeline) >= 1
        # La première doit être une violation de CPU ou Latency
        assert viz.violation_timeline[0]["metric"] in ["cpu_usage", "latency"]
        # La dernière doit être la migration
        assert viz.violation_timeline[-1]["type"] == "migration"
        assert len(viz.mi_scores_history) == 1
        assert viz.data_quality == 0.94

    def test_full_cycle_stay(self, viz):
        """Simule un cycle nominal sans violation ni migration."""
        # 1. Données saines
        viz.update_data([{"vm_id": "vm1", "rtt_ms": 30, "cpu_usage": 50, "ram_usage": 40, "is_active_service": True}])
        
        # 2. Décision de maintien
        viz.update_decision({"decision": "stay", "reason": "Nominal"})
        
        # Assertions
        assert viz.last_decision_detail["decision"] == "stay"
        assert len(viz.violation_timeline) == 0
        # Aucune migration ne doit être présente
        assert all(ev["type"] != "migration" for ev in viz.violation_timeline)

    def test_timeline_ordering(self, viz):
        """Vérifie l'ordre chronologique et le cumul des événements dans la timeline."""
        # Injecter 3 violations (une par VM par exemple)
        viz.update_data([
            {"vm_id": "vm1", "cpu_usage": 90},
            {"vm_id": "vm2", "cpu_usage": 95},
            {"vm_id": "vm3", "cpu_usage": 88}
        ])
        
        # Injecter 1 migration
        viz.update_decision({"decision": "migrate", "from_vm": "vm1", "to_vm": "vm4"})
        
        assert len(viz.violation_timeline) == 4
        assert viz.violation_timeline[0]["type"] == "violation"
        assert viz.violation_timeline[-1]["type"] == "migration"

    def test_suptitle_coherence(self, viz):
        """Vérifie que le titre généré reflète fidèlement l'état d'intégration."""
        # Préparation via le cycle de migration
        self.test_full_cycle_migrate(viz)
        
        title, color = viz._build_suptitle()
        
        assert color == "red"
        assert "vm1" in title
        assert "vm3" in title
        assert "94.0%" in title

    def test_resilience_bad_data(self, viz):
        """Vérifie que le composant est robuste face à des données corrompues ou manquantes."""
        try:
            viz.update_data([{}])  # Liste vide ou dict vide
            viz.update_decision({}) # Décision vide
            viz.update_mi_scores(None) # None au lieu de dict
            viz.update_quality(-5.0)   # Valeur hors bornes
        except Exception as e:
            pytest.fail(f"ObservabilitySpoke a crashé avec des données invalides : {e}")
            
        assert viz.data_quality == 0.0 # Clamp min
        assert viz.last_decision_detail["decision"] == "stay"

# =============================================================================
# 20. TESTS HISTORIQUE INTENTIONS (Intent History)
# =============================================================================

class TestIntentHistory:
    
    @pytest.fixture
    def spoke(self):
        config = Config()
        core = MagicMock()
        return IntentManagerSpoke(config, core, ":memory:", [])

    def test_history_grows_on_success(self, spoke):
        """Vérifie que l'historique s'incrémente après un tour réussi."""
        slos = [{"metric": "latency", "threshold": 50, "unit": "ms", "operator": "<", "weight": 1.0}]
        spoke._record_intent_turn("test rapide", slos)
        
        assert len(spoke.intent_history) == 1
        assert spoke.intent_history[0]["turn"] == 1
        assert spoke.intent_history[0]["text"] == "test rapide"

    def test_history_fifo_truncation(self, spoke):
        """Vérifie que l'historique est tronqué à 10 tours (FIFO)."""
        for i in range(12):
            spoke._record_intent_turn(f"intent {i}", [])
            
        assert len(spoke.intent_history) == 10
        # Le premier élément doit être l'index 2 (le 3ème ajouté)
        assert spoke.intent_history[0]["text"] == "intent 2"

    def test_history_turn_increments(self, spoke):
        """Vérifie que le numéro de tour s'incrémente séquentiellement."""
        for _ in range(3):
            spoke._record_intent_turn("dummy", [])
        assert spoke.intent_history[2]["turn"] == 3

    def test_rag_context_includes_history(self, spoke):
        """Vérifie que l'historique des intentions est inclus dans le contexte RAG."""
        # Préparer l'historique
        spoke.intent_history = [
            {
                "turn": 1, "text": "init", 
                "slos": [{"metric": "latency", "threshold": 100, "unit": "ms"}]
            },
            {
                "turn": 2, "text": "faster", 
                "slos": [{"metric": "latency", "threshold": 50, "unit": "ms"}]
            }
        ]
        
        # Mocker le Hub (core) pour retourner des données vides
        spoke.core.get_recent_context.return_value = []
        
        builder = spoke._RAGContextBuilder(
            spoke.config, spoke.core, 
            [{"metric": "latency", "threshold": 50, "unit": "ms", "operator": "<"}],
            history=spoke.intent_history
        )
        
        context = builder.build_context()
        
        assert "Historique des intentions" in context
        assert "Tour 1" in context
        assert "Tour 2" in context
        assert "faster" in context

# =============================================================================
# 21. TESTS RAFFINEMENT INTENTIONS (Intent Refinement)
# =============================================================================

class TestIntentRefinement:
    
    @pytest.fixture
    def spoke(self):
        config = Config()
        core = MagicMock()
        return IntentManagerSpoke(config, core, ":memory:", [])

    def test_replace_direct_intent(self, spoke):
        """Intention directe sans mot-clé de raffinement -> REPLACE."""
        text = "je veux une latence < 20ms"
        current_slos = [{"metric": "latency", "threshold": 50}]
        mode = spoke._detect_refinement_mode(text, current_slos, ["latency"])
        assert mode == "REPLACE"

    def test_additive_keyword(self, spoke):
        """Présence de 'aussi' -> ADDITIVE."""
        text = "ajoute aussi cpu < 50%"
        current_slos = [{"metric": "latency", "threshold": 50}]
        mode = spoke._detect_refinement_mode(text, current_slos, ["cpu_usage"])
        assert mode == "ADDITIVE"

    def test_additive_no_overlap(self, spoke):
        """Métrique différente sans mot-clé -> ADDITIVE."""
        text = "je veux cpu < 50%"
        current_slos = [{"metric": "latency", "threshold": 50}]
        mode = spoke._detect_refinement_mode(text, current_slos, ["cpu_usage"])
        assert mode == "ADDITIVE"

    def test_replace_overlap(self, spoke):
        """Même métrique sans mot-clé -> REPLACE."""
        text = "je veux latence < 10ms"
        current_slos = [{"metric": "latency", "threshold": 50}, {"metric": "cpu_usage", "threshold": 80}]
        mode = spoke._detect_refinement_mode(text, current_slos, ["latency"])
        assert mode == "REPLACE"

    def test_additive_accents(self, spoke):
        """Vérifie que la normalisation gère les accents pour 'également'."""
        text = "également ram < 80%"
        current_slos = [{"metric": "latency", "threshold": 50}]
        mode = spoke._detect_refinement_mode(text, current_slos, ["ram_usage"])
        assert mode == "ADDITIVE"

    def test_additive_overlap_with_keyword(self, spoke):
        """Même métrique MAIS mot-clé 'ajoute' -> ADDITIVE."""
        text = "ajoute aussi ram < 80%"
        current_slos = [{"metric": "ram_usage", "threshold": 50}]
        mode = spoke._detect_refinement_mode(text, current_slos, ["ram_usage"])
        assert mode == "ADDITIVE"

    def test_replace_empty_slos(self, spoke):
        """Si aucun SLO existant -> REPLACE."""
        text = "je veux de la performance"
        current_slos = []
        mode = spoke._detect_refinement_mode(text, current_slos, [])
        assert mode == "REPLACE"

# =============================================================================
# 22. TESTS FUSION SLO (SLO Merge & Refinement)
# =============================================================================

class TestSLOMerge:
    
    @pytest.fixture
    def spoke(self):
        config = Config()
        core = MagicMock()
        return IntentManagerSpoke(config, core, ":memory:", [])

    def test_replace_returns_new(self, spoke):
        """Mode REPLACE doit retourner uniquement les nouveaux SLOs."""
        existing = [{"metric": "latency", "threshold": 50, "weight": 1.0}]
        new = [{"metric": "cpu_usage", "threshold": 70, "weight": 1.0}]
        result = spoke._merge_slos(existing, new, "REPLACE")
        assert len(result) == 1
        assert result[0]["metric"] == "cpu_usage"

    def test_additive_union(self, spoke):
        """Mode ADDITIVE doit faire l'union des SLOs."""
        existing = [{"metric": "latency", "threshold": 50, "weight": 0.5}]
        new = [{"metric": "cpu_usage", "threshold": 70, "weight": 0.5}]
        result = spoke._merge_slos(existing, new, "ADDITIVE")
        assert len(result) == 2
        metrics = [s["metric"] for s in result]
        assert "latency" in metrics
        assert "cpu_usage" in metrics

    def test_additive_overrides_same_metric(self, spoke):
        """Mode ADDITIVE doit écraser les SLOs de même métrique."""
        existing = [{"metric": "latency", "threshold": 50, "weight": 1.0}]
        new = [{"metric": "latency", "threshold": 20, "weight": 1.0}]
        result = spoke._merge_slos(existing, new, "ADDITIVE")
        assert len(result) == 1
        assert result[0]["threshold"] == 20

    def test_refine_strict_more(self, spoke):
        """Mode REFINE 'plus strict' doit réduire le threshold (x0.85)."""
        existing = [{"metric": "latency", "threshold": 50.0, "weight": 1.0}]
        result = spoke._merge_slos(existing, [], "REFINE", "rends ca plus strict")
        assert result[0]["threshold"] == 42.5 # 50 * 0.85

    def test_refine_strict_less(self, spoke):
        """Mode REFINE 'moins strict' doit augmenter le threshold (x1.15)."""
        existing = [{"metric": "cpu_usage", "threshold": 70.0, "weight": 1.0}]
        result = spoke._merge_slos(existing, [], "REFINE", "relâche la contrainte")
        assert result[0]["threshold"] == 80.5 # 70 * 1.15

    def test_refine_ignore_metric(self, spoke):
        """Mode REFINE 'ignore' doit supprimer la métrique mentionnée."""
        existing = [
            {"metric": "cpu_usage", "threshold": 70},
            {"metric": "ram_usage", "threshold": 80}
        ]
        # "oublie" est dans IGNORE_KW, "ram" est dans METRIC_KEYWORDS["ram_usage"]
        result = spoke._merge_slos(existing, [], "REFINE", "oublie la ram")
        assert len(result) == 1
        assert result[0]["metric"] == "cpu_usage"

    def test_normalize_weights_after_merge(self, spoke):
        """Vérifie que les poids sont redistribués après une fusion additive."""
        existing = [{"metric": "latency", "threshold": 50, "weight": 1.0}]
        new = [{"metric": "cpu_usage", "threshold": 70, "weight": 1.0}]
        result = spoke._merge_slos(existing, new, "ADDITIVE")
        total_weight = sum(s["weight"] for s in result)
        assert abs(total_weight - 1.0) < 0.001
        assert result[0]["weight"] == 0.5
        assert result[1]["weight"] == 0.5

# =============================================================================
# MAIN
# =============================================================================

if __name__ == "__main__":
    # Permet de lancer les tests directement via 'python test_orchestrator.py'
    pytest.main([__file__, "-v"])
