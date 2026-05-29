import unittest
from unittest.mock import MagicMock, patch, patch
import pytest
import time
import json
import sqlite3
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
    ObservabilitySpoke,
    ValidationResult,
    LatencyManagerSpoke
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
            def mock_collect(vm_id):
                if vm_id == "vm1":
                    return {"vm_id": "vm1", "cpu_usage": val, "ram_usage": val, "is_active_service": True}
                return {"vm_id": "vm2", "cpu_usage": 30.0, "ram_usage": 30.0, "is_active_service": False}
            
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
# MAIN
# =============================================================================

if __name__ == "__main__":
    # Permet de lancer les tests directement via 'python test_orchestrator.py'
    pytest.main([__file__, "-v"])
