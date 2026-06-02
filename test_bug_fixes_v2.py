import unittest
from unittest.mock import MagicMock, patch
from orchestrator import Config, DecisionIntelligenceSpoke, OrchestratorCore, PredictionResult

class TestBugFixesV2(unittest.TestCase):
    def setUp(self):
        self.config = Config()
        self.decision_spoke = DecisionIntelligenceSpoke(self.config)
        # Pour OrchestratorCore, on mock les dépendances lourdes
        with patch('orchestrator.DatabaseSpoke'), \
             patch('orchestrator.LatencyManagerSpoke'), \
             patch('orchestrator.IntentManagerSpoke'), \
             patch('orchestrator.ObservabilitySpoke'):
            self.core = OrchestratorCore(self.config)

    # --- TESTS BUG 1 : DecisionIntelligenceSpoke.evaluate_enhanced_decision (KeyError fix) ---

    def test_bug1_no_targets_available(self):
        """Test 1 : Une seule VM (source) -> final_pool vide -> doit retourner 'stay'"""
        current_data = [
            {"vm_id": "vm1", "rtt_ms": 150.0, "is_active_service": True}
        ]
        # On simule un SLO violé pour déclencher la recherche de migration
        slos = [{"metric": "latency", "threshold": 100.0, "weight": 1.0}]
        # Map de prédiction vide
        preds_map = {"vm1": {"latency": [150.0, 160.0]}}
        
        # On mock _topsis_select pour qu'il reçoive une liste vide et renvoie {}
        with patch.object(self.decision_spoke, '_topsis_select', return_value={}):
            result = self.decision_spoke.evaluate_enhanced_decision(
                current_data, preds_map, slos, service_vm="vm1"
            )
            
        self.assertEqual(result["decision"], "stay")
        self.assertEqual(result["reason"], "Aucune cible disponible pour migration")

    def test_bug1_all_targets_violate_slos(self):
        """Test 2 : Toutes les cibles violent les SLOs -> fallback targets -> migration OK"""
        current_data = [
            {"vm_id": "vm1", "rtt_ms": 150.0, "is_active_service": True},
            {"vm_id": "vm2", "rtt_ms": 120.0, "is_active_service": False} # viole aussi le SLO (120 > 100)
        ]
        slos = [{"metric": "latency", "threshold": 100.0, "weight": 1.0}]
        preds_map = {
            "vm1": {"latency": [150.0]},
            "vm2": {"latency": [120.0]}
        }
        
        # On s'attend à ce que _topsis_select soit appelé avec [vm2] malgré la violation (fallback)
        with patch.object(self.decision_spoke, '_topsis_select', return_value={"vm_id": "vm2", "topsis_score": 0.5}) as mock_topsis:
            result = self.decision_spoke.evaluate_enhanced_decision(
                current_data, preds_map, slos, service_vm="vm1"
            )
            
            # Vérifie que TOPSIS a bien été appelé avec vm2
            candidates = mock_topsis.call_args[0][0]
            self.assertEqual(len(candidates), 1)
            self.assertEqual(candidates[0]["vm_id"], "vm2")
            
        self.assertEqual(result["decision"], "migrate")
        self.assertEqual(result["to_vm"], "vm2")

    def test_bug1_nominal_migration(self):
        """Test 3 : Migration nominale vers une cible saine"""
        current_data = [
            {"vm_id": "vm1", "rtt_ms": 150.0, "is_active_service": True},
            {"vm_id": "vm2", "rtt_ms": 40.0, "is_active_service": False}
        ]
        slos = [{"metric": "latency", "threshold": 100.0, "weight": 1.0}]
        preds_map = {"vm1": {"latency": [150.0]}, "vm2": {"latency": [40.0]}}
        
        with patch.object(self.decision_spoke, '_topsis_select', return_value={"vm_id": "vm2", "topsis_score": 0.9}):
            result = self.decision_spoke.evaluate_enhanced_decision(
                current_data, preds_map, slos, service_vm="vm1"
            )
            
        self.assertEqual(result["decision"], "migrate")
        self.assertEqual(result["to_vm"], "vm2")
        self.assertIn("topsis_score", result)

    # --- TESTS BUG 2 : OrchestratorCore._filter_active_metrics (Metadata propagation) ---

    def test_bug2_propagation_success(self):
        """Test 4 : Propagation réussie de data_source et reliability"""
        collected = {
            "vm_id": "vm1",
            "cpu_usage": 45.0,
            "ram_usage": 60.0,
            "data_source": "real",
            "reliability": 0.85,
            "is_active_service": True
        }
        active_metrics = ["cpu_usage"]
        
        res = self.core._filter_active_metrics(collected, active_metrics, "vm1")
        
        self.assertEqual(res["data_source"], "real")
        self.assertEqual(res["reliability"], 0.85)
        self.assertEqual(res["cpu_usage"], 45.0)
        self.assertNotIn("ram_usage", res) # ram_usage non demandé

    def test_bug2_fail_fast_on_invalid_input(self):
        """Test 5 : collected invalide -> fallback vm_id sans metadata"""
        # Cas 1 : vide
        res = self.core._filter_active_metrics({}, [], "fallback_vm")
        self.assertEqual(res, {"vm_id": "fallback_vm"})
        
        # Cas 2 : sans vm_id
        res = self.core._filter_active_metrics({"cpu": 10}, [], "fallback_vm")
        self.assertEqual(res, {"vm_id": "fallback_vm"})

    def test_bug2_default_fallbacks(self):
        """Test 6 : collected valide mais sans metadata -> application des fallbacks par défaut"""
        collected = {
            "vm_id": "vm1",
            "cpu_usage": 10.0
        }
        res = self.core._filter_active_metrics(collected, ["cpu_usage"], "vm1")
        
        self.assertEqual(res["vm_id"], "vm1")
        self.assertEqual(res["data_source"], "unknown")
        self.assertEqual(res["reliability"], 1.0)

if __name__ == '__main__':
    unittest.main()
