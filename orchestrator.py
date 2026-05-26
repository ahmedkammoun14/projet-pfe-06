from __future__ import annotations

import functools
import json
import logging
import random
import re
import sqlite3
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import colorama
import matplotlib.pyplot as plt
import requests
from colorama import Fore, Back, Style
from flask import Flask, jsonify, request

# --- INITIALIZATION ---
colorama.init(autoreset=True)

# Silence Werkzeug logs for cleaner terminal output
logging.getLogger('werkzeug').setLevel(logging.ERROR)

# --- UTILITIES ---

def calculate_weighted_mean(values: List[float]) -> float:
    """Calculates a decreasing weighted mean (n, n-1, ..., 1).
    
    Args:
        values: List of numeric values (usually predictions).
        
    Returns:
        The weighted mean or 0.0 if the list is empty.
    """
    if not values:
        return 0.0
    n = len(values)
    weights = list(range(n, 0, -1))
    total_weight = sum(weights)
    weighted_sum = sum(v * w for v, w in zip(values, weights))
    return weighted_sum / total_weight if total_weight > 0 else 0.0

class ColoredFormatter(logging.Formatter):
    """Custom formatter for terminal logging with colors."""
    def format(self, record: logging.LogRecord) -> str:
        level_colors = {
            logging.INFO: Fore.CYAN,
            logging.WARNING: Fore.YELLOW,
            logging.ERROR: Fore.RED,
            logging.CRITICAL: Back.RED + Fore.WHITE
        }
        color = level_colors.get(record.levelno, Fore.WHITE)
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return (f"{Fore.BLACK + Style.BRIGHT}{timestamp}{Style.RESET_ALL} | "
                f"{color}{record.levelname:8}{Style.RESET_ALL} | "
                f"{Style.BRIGHT}{record.getMessage()}{Style.RESET_ALL}")

# Global Logging Setup
handler = logging.StreamHandler()
handler.setFormatter(ColoredFormatter())
logging.root.handlers = [handler]
logging.root.setLevel(logging.INFO)
logger = logging.getLogger("Orchestrator")

def log_step(step_name: str, direction: str, data: Any) -> None:
    """Visual logging of a workflow step in the terminal.
    
    Args:
        step_name: Name of the current step.
        direction: Origin and destination of data flow.
        data: Payload to display.
    """
    try:
        if isinstance(data, (dict, list)):
            data_lines = json.dumps(data, indent=2, ensure_ascii=False).split('\n')
        else:
            data_lines = [str(data)]
    except (TypeError, ValueError):
        data_lines = [str(data)]
        
    width = 60
    print(f"\n{Fore.BLUE}┌{'─'*width}┐")
    print(f"{Fore.BLUE}│  {Fore.YELLOW}➤ ÉTAPE : {Style.BRIGHT}{step_name}")
    print(f"{Fore.BLUE}│  {Fore.CYAN}📤 {direction}")
    print(f"{Fore.BLUE}│  {Fore.WHITE}📦 PAYLOAD :")
    for line in data_lines:
        print(f"{Fore.BLUE}│    {Fore.MAGENTA}{line}")
    print(f"{Fore.BLUE}└{'─'*width}┘")

def safe_call(default_val: Any, context_name: str):
    """Decorator to catch exceptions and prevent system crash.
    
    Args:
        default_val: Value to return if an exception occurs.
        context_name: Identifier for logging purposes.
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"[SafeCall] Error in {context_name}: {type(e).__name__}: {e}")
                return default_val
        return wrapper
    return decorator

# --- CONFIGURATION ---

@dataclass
class Config:
    """Global configuration for ports, thresholds, and simulation parameters."""
    # Ports
    CORE_PORT: int = 8000
    LATENCY_PORT: int = 8010
    ML_PREDICTOR_PORT: int = 8011
    COLLECTOR_PORT: int = 8012
    DECISION_PORT: int = 8013
    INTENT_PORT: int = 8014
    CONFIG_PORT: int = 8015
    OBSERVABILITY_PORT: int = 8016
    DATABASE_PORT: int = 8020
    HISTORY_LOADER_PORT: int = 8021
    METRICS_MANAGER_PORT: int = 8022
    
    # ML & Intent URLs
    INTENT_ENGINE_URL: str = "http://localhost:11434/api/chat"
    ML_RTT_URL: str = "http://localhost:5001/predict"
    ML_CPU_URL: str = "http://localhost:5002/predict"
    ML_RAM_URL: str = "http://localhost:5003/predict"
    
    # VM Settings
    VM_LIST: List[str] = field(default_factory=lambda: ["vm1", "vm2", "vm3", "vm4"])
    VM_PORTS: Dict[str, int] = field(default_factory=lambda: {
        "vm1": 8101, "vm2": 8102, "vm3": 8103, "vm4": 8104
    })
    
    # Default Thresholds
    DEFAULT_LATENCY_THRESHOLD: float = 50.0
    DEFAULT_CPU_THRESHOLD: float = 75.0
    DEFAULT_RAM_THRESHOLD: float = 80.0
    
    # Simulation & Database
    COLLECTION_INTERVAL: int = 5
    HISTORY_WINDOW: int = 10
    DB_NAME: str = "orchestrator.db"

    # Master Cloud Settings
    MASTER_URL: str = "https://master-cloud/api/v1/migrate"
    MASTER_TOKEN: str = "changeme"
    MASTER_TIMEOUT: int = 10
    COOLDOWN_SECONDS: int = 60

# --- SPOKES (SERVICES) ---

class LatencyManagerSpoke:
    """Handles real-time RTT data reception from external sensors."""
    def __init__(self, config: Config, core: 'OrchestratorCore'):
        self.config = config
        self.core = core
        self.app = Flask(f"{__name__}_latency")
        self._setup_routes()

    def _setup_routes(self):
        @self.app.route('/rtt', methods=['POST'])
        def receive_rtt():
            data = request.json
            if not data or "measurements" not in data:
                return jsonify({"error": "Invalid payload"}), 400
            
            self.core.last_real_data_ts = time.time()
            measurements = data["measurements"]
            
            # Route to correct flow based on mode
            if self.core.mode == "enhanced":
                decision = self.core.run_enhanced_flow(measurements)
            else:
                decision = self.core.run_classic_flow(measurements)
                
            return jsonify({"status": "received", "decision": decision}), 200

    def start_api(self):
        threading.Thread(
            target=self.app.run, 
            kwargs={'host': '0.0.0.0', 'port': self.config.LATENCY_PORT, 'threaded': True, 'use_reloader': False}, 
            daemon=True
        ).start()
        logger.info(f"[LatencyManager] API started on port {self.config.LATENCY_PORT}")

class IntentManagerSpoke:
    """Interfaces with LLM to extract SLOs from natural language."""
    def __init__(self, config: Config, core: 'OrchestratorCore'):
        self.config = config
        self.core = core
        self.app = Flask(f"{__name__}_intent")
        self._setup_routes()

    def _setup_routes(self):
        @self.app.route('/intent', methods=['POST'])
        def receive_intent():
            data = request.json
            if not data or "intention" not in data:
                return jsonify({"error": "Empty intention"}), 400
            
            self.core.set_user_intent(data["intention"])
            return jsonify({
                "status": "received", 
                "intent_id": data.get("intent_id", "unknown"), 
                "slos": self.core.current_slos
            }), 200

    def start_api(self):
        threading.Thread(
            target=self.app.run, 
            kwargs={'host': '0.0.0.0', 'port': self.config.INTENT_PORT, 'threaded': True, 'use_reloader': False}, 
            daemon=True
        ).start()
        logger.info(f"[IntentManager] API started on port {self.config.INTENT_PORT}")

    def query_intent_engine(self, text: str) -> List[Dict]:
        """Queries Ollama LLM to parse SLOs from user text."""
        try:
            payload = {
                "model": "qwen2.5",
                "messages": [
                    {
                        "role": "system",
                        "content": (
                            "You are an assistant that MUST respond with ONLY a JSON array and nothing else. "
                            "Extract numeric threshold values directly from the user's intention text. "
                            "Only include SLO objects for metrics explicitly mentioned. "
                            "Keys: metric, operator, threshold, unit, weight. "
                            "Weight is float (0.0-1.0), sum MUST be 1.0. "
                            "Metrics: \"latency\", \"cpu_usage\", \"ram_usage\". "
                            "Example: [{\"metric\":\"latency\",\"operator\":\"<\",\"threshold\":20,\"unit\":\"ms\",\"weight\":1.0}]"
                        )
                    },
                    {"role": "user", "content": text}
                ],
                "stream": False
            }
            resp = requests.post(self.config.INTENT_ENGINE_URL, json=payload, timeout=90)
            llm_text = resp.json().get("message", {}).get("content", "").lower()
            return self._parse_slos_from_text(llm_text)
        except Exception as e:
            logger.warning(f"[Intent] LLM Error: {e}. Falling back to regex.")
            return self._parse_slos_from_text(text)

    def _parse_slos_from_text(self, text: str) -> List[Dict]:
        """Parses JSON or uses regex fallback to extract SLO objects."""
        # JSON parsing
        try:
            start, end = text.find('['), text.rfind(']')
            if start != -1 and end != -1:
                slos = json.loads(text[start:end+1])
                if isinstance(slos, list) and slos:
                    return slos
        except (ValueError, json.JSONDecodeError):
            pass

        # Regex Fallback
        slos = []
        patterns = {"latency": r"latency\s*<\s*(\d+)", "cpu_usage": r"cpu\s*<\s*(\d+)", "ram_usage": r"ram\s*<\s*(\d+)"}
        for metric, regex in patterns.items():
            match = re.search(regex, text)
            if match:
                slos.append({
                    "metric": metric, "operator": "<", 
                    "threshold": float(match.group(1)), 
                    "unit": "ms" if metric == "latency" else "%"
                })
        
        # Distribute weights for fallback
        if slos:
            w = round(1.0 / len(slos), 2)
            for s in slos: s["weight"] = w
            slos[-1]["weight"] = round(1.0 - sum(s["weight"] for s in slos[:-1]), 2)
            return slos

        # Absolute Fallback
        return [
            {"metric": "latency", "operator": "<", "threshold": 50, "unit": "ms", "weight": 0.34},
            {"metric": "cpu_usage", "operator": "<", "threshold": 75, "unit": "%", "weight": 0.33},
            {"metric": "ram_usage", "operator": "<", "threshold": 80, "unit": "%", "weight": 0.33}
        ]

class MetricsManagerSpoke:
    """Determines which physical metrics are needed based on active SLOs."""
    def analyze_needed_metrics(self, slos: List[Dict]) -> List[str]:
        return list(set(slo["metric"] for slo in slos if slo["metric"] in ["cpu_usage", "ram_usage"]))

class CollectorSpoke:
    """Fetches real-time physical metrics (CPU/RAM) from VM simulators."""
    def __init__(self, config: Config):
        self.config = config

    @safe_call({}, "CollectorSpoke.collect_vm_metrics")
    def collect_vm_metrics(self, vm_id: str) -> Dict:
        try:
            port = self.config.VM_PORTS.get(vm_id)
            resp = requests.get(f"http://localhost:{port}/metrics", timeout=1.5)
            data = resp.json()
            return {"vm_id": vm_id, "cpu_usage": data["cpu_usage"], "ram_usage": data["ram_usage"]}
        except Exception:
            # Fallback to simulation if VM is unreachable
            return {
                "vm_id": vm_id, 
                "cpu_usage": random.uniform(20, 95), 
                "ram_usage": random.uniform(30, 90)
            }

class MLPredictorSpoke:
    """Handles communication with specialized Machine Learning prediction APIs."""
    def __init__(self, config: Config): 
        self.config = config

    def _parse_numpy_string(self, raw_str: str) -> List[float]:
        """Parses a string representation of a numpy array into a list of floats."""
        return [float(x) for x in re.findall(r"[\d.]+", raw_str)]

    def _get_api_prediction(self, url: str, current_val: float) -> Optional[List[float]]:
        """Makes a GET request to a specific ML API."""
        try:
            resp = requests.get(url, params={"input_data": current_val / 100.0}, timeout=10)
            raw = resp.json().get("prediction", "[]")
            return [v * 100 for v in self._parse_numpy_string(raw)]
        except Exception:
            return None

    def get_prediction(self, current_val: float) -> List[float]:
        """Classic mode prediction (Latency only)."""
        pred = self._get_api_prediction(self.config.ML_RTT_URL, current_val)
        if pred: return pred
        logger.warning("[ML] Latency API unavailable -> Local simulation fallback")
        return [current_val * (1.05 ** i) for i in range(1, 6)]

    def get_enhanced_prediction(self, metric: str, current_val: float) -> List[float]:
        """Enhanced mode prediction for multi-metrics."""
        urls = {"latency": self.config.ML_RTT_URL, "cpu_usage": self.config.ML_CPU_URL, "ram_usage": self.config.ML_RAM_URL}
        pred = self._get_api_prediction(urls.get(metric, ""), current_val)
        if pred: return pred
        
        logger.warning(f"[ML] {metric} API unavailable -> Local simulation fallback")
        factors = {"latency": 1.05, "cpu_usage": 1.03, "ram_usage": 1.02}
        f = factors.get(metric, 1.01)
        return [current_val * (f ** i) for i in range(1, 6)]

class DecisionIntelligenceSpoke:
    """Core decision engine implementing threshold and predictive logic."""
    def __init__(self, config: Config):
        self.config = config

    def evaluate_classic_decision(self, current_data: List[Dict], predictions_map: Dict[str, List[float]]) -> Dict:
        """Threshold-based decision for RTT only."""
        threshold = self.config.DEFAULT_LATENCY_THRESHOLD
        breached_vm = None
        trigger_val = 0.0
        
        for entry in current_data:
            vm_id = entry["vm_id"]
            preds = predictions_map.get(vm_id, [])
            if entry["rtt_ms"] > threshold or calculate_weighted_mean(preds) > threshold:
                breached_vm, trigger_val = vm_id, entry["rtt_ms"]
                break
                
        if breached_vm:
            targets = [e for e in current_data if e["vm_id"] != breached_vm]
            best = min(targets, key=lambda x: x["rtt_ms"])
            reason = f"{breached_vm} RTT {trigger_val:.1f}ms > {threshold}ms"
            return {"decision": "migrate", "from_vm": breached_vm, "to_vm": best["vm_id"], "reason": reason}
        return {"decision": "stay", "reason": "Nominal"}

    def evaluate_enhanced_decision(self, current_data: List[Dict], predictions_map: Dict[str, Dict[str, List[float]]], slos: List[Dict]) -> Dict:
        """Intention-based decision using multi-criteria weighted scoring."""
        weights = {m: 0.0 for m in ["latency", "cpu_usage", "ram_usage"]}
        for s in slos: weights[s["metric"]] = s.get("weight", 0.0)

        for entry in current_data:
            vm_id = entry["vm_id"]
            for slo in slos:
                m_name, threshold = slo["metric"], slo["threshold"]
                val = entry.get("rtt_ms" if m_name == "latency" else m_name)
                
                if val is None: continue
                
                preds = predictions_map.get(vm_id, {}).get(m_name, [])
                if val > threshold or calculate_weighted_mean(preds) > threshold:
                    # Filter valid targets
                    targets = [e for e in current_data if e["vm_id"] != vm_id]
                    valid = [t for t in targets if all(
                        t.get("rtt_ms" if s["metric"] == "latency" else s["metric"], 999) <= s["threshold"] 
                        for s in slos
                    )]
                    
                    final_pool = valid if valid else targets
                    best = min(final_pool, key=lambda t: (
                        weights["latency"] * calculate_weighted_mean(predictions_map.get(t["vm_id"], {}).get("latency", [])) +
                        weights["cpu_usage"] * calculate_weighted_mean(predictions_map.get(t["vm_id"], {}).get("cpu_usage", [])) +
                        weights["ram_usage"] * calculate_weighted_mean(predictions_map.get(t["vm_id"], {}).get("ram_usage", []))
                    ))
                    
                    reason = f"{vm_id} {m_name} {val:.1f} > SLO {threshold}"
                    return {"decision": "migrate", "from_vm": vm_id, "to_vm": best["vm_id"], "reason": reason}
        return {"decision": "stay", "reason": "All SLOs satisfied"}

class DatabaseSpoke:
    """Persistent thread-safe storage for metrics and decisions."""
    def __init__(self, config: Config):
        self.config = config

    def init_db(self):
        with sqlite3.connect(self.config.DB_NAME) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS metrics (id INTEGER PRIMARY KEY, vm_id TEXT, rtt_ms REAL, cpu_usage REAL, ram_usage REAL, mode TEXT, timestamp TEXT)")
            conn.execute("CREATE TABLE IF NOT EXISTS decisions (id INTEGER PRIMARY KEY, decision TEXT, from_vm TEXT, to_vm TEXT, reason TEXT, mode TEXT, master_ack INTEGER, timestamp TEXT)")

    @safe_call(None, "DatabaseSpoke.save_metrics")
    def save_metrics(self, measurements: List[Dict], mode: str):
        ts = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.config.DB_NAME) as conn:
            for m in measurements:
                conn.execute("INSERT INTO metrics (vm_id, rtt_ms, cpu_usage, ram_usage, mode, timestamp) VALUES (?, ?, ?, ?, ?, ?)", 
                             (m["vm_id"], m.get("rtt_ms", 0), m.get("cpu_usage", 0), m.get("ram_usage", 0), mode, ts))

    @safe_call(False, "DatabaseSpoke.save_decision")
    def save_decision(self, dec: Dict, mode: str, ack: bool):
        ts = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.config.DB_NAME) as conn:
            conn.execute("INSERT INTO decisions (decision, from_vm, to_vm, reason, mode, master_ack, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                         (dec["decision"], dec.get("from_vm"), dec.get("to_vm"), dec["reason"], mode, 1 if ack else 0, ts))
        return True

class HistoryLoaderSpoke:
    """Loads historical data windows for ML prediction input."""
    def __init__(self, config: Config): self.config = config
    def load_window(self, vm_id: str, metric: str, size: int) -> List[float]:
        col = "rtt_ms" if metric == "latency" else metric
        with sqlite3.connect(self.config.DB_NAME) as conn:
            cur = conn.execute(f"SELECT {col} FROM metrics WHERE vm_id = ? ORDER BY id DESC LIMIT ?", (vm_id, size))
            return [row[0] for row in cur.fetchall()]

class ObservabilitySpoke:
    """Real-time Matplotlib dashboard for visualizing RTT, CPU, RAM and SLOs."""
    def __init__(self, config: Config):
        self.config = config
        self.history = {m: {vm: [] for vm in config.VM_LIST} for m in ["rtt", "cpu", "ram"]}
        self.predictions_history = {m: {vm: [] for vm in config.VM_LIST} for m in ["rtt", "cpu", "ram"]}
        self.current_slos = []
        self.max_points = 50

    def update_data(self, current_data: List[Dict]):
        for entry in current_data:
            vm = entry["vm_id"]
            for key, m in [("rtt", "rtt_ms"), ("cpu", "cpu_usage"), ("ram", "ram_usage")]:
                if m in entry:
                    self.history[key][vm].append(entry[m])
                    if len(self.history[key][vm]) > self.max_points: self.history[key][vm].pop(0)

    def update_predictions(self, preds_map: Dict, mode: str = "classic"):
        if mode == "classic":
            for vm, preds in preds_map.items():
                self.predictions_history["rtt"][vm].append(calculate_weighted_mean(preds))
        else:
            for vm, metrics in preds_map.items():
                for key, m in [("rtt", "latency"), ("cpu", "cpu_usage"), ("ram", "ram_usage")]:
                    if m in metrics:
                        self.predictions_history[key][vm].append(calculate_weighted_mean(metrics[m]))
        
        # Cleanup
        for m_key in ["rtt", "cpu", "ram"]:
            for vm in self.config.VM_LIST:
                if len(self.predictions_history[m_key][vm]) > self.max_points:
                    self.predictions_history[m_key][vm].pop(0)

    def start_gui(self):
        plt.ion(); fig = plt.figure(figsize=(20, 15))
        axs = fig.subplots(3, 4)
        while True:
            for r_idx, (m_key, m_name) in enumerate([("rtt", "latency"), ("cpu", "cpu_usage"), ("ram", "ram_usage")]):
                for c_idx, vm in enumerate(self.config.VM_LIST):
                    ax = axs[r_idx, c_idx]
                    ax.clear()
                    ax.plot(self.history[m_key][vm], label="Réel", color="blue")
                    if self.predictions_history[m_key][vm]:
                        ax.plot(self.predictions_history[m_key][vm], label="Prédit", color="red", linestyle="--")
                    for slo in self.current_slos:
                        if slo["metric"] == m_name:
                            ax.axhline(y=slo["threshold"], color='orange', linestyle='--', label=f'SLO {slo["threshold"]}')
                    ax.set_title(f"{m_key.upper()} {vm}"); ax.set_ylim(0, 100); ax.legend(loc="upper right")
            plt.tight_layout(); plt.pause(1)

# --- HUB (CORE) ---

class OrchestratorCore:
    """The central hub coordinating all peripheral spokes in the star architecture."""
    def __init__(self, config: Config):
        self.config = config
        self.db = DatabaseSpoke(config)
        self.history = HistoryLoaderSpoke(config)
        self.ml = MLPredictorSpoke(config)
        self.decision_engine = DecisionIntelligenceSpoke(config)
        self.viz = ObservabilitySpoke(config)
        self.intent_mgr = IntentManagerSpoke(config, self)
        self.metrics_mgr = MetricsManagerSpoke()
        self.collector = CollectorSpoke(config)
        self.latency_mgr = LatencyManagerSpoke(config, self)
        
        self.mode = "classic"
        self.current_slos = []
        self.last_real_data_ts = None
        self.last_migration_ts = None
        self.start_ts = time.time()
        self.app = Flask(f"{__name__}_core")
        self._setup_routes()

    def _setup_routes(self):
        @self.app.route('/status', methods=['GET'])
        def get_status():
            return jsonify({
                "mode": self.mode, "uptime": int(time.time() - self.start_ts), 
                "slos": self.current_slos
            }), 200

    def set_user_intent(self, text: str):
        slos = self.intent_mgr.query_intent_engine(text)
        is_fail = any(s["metric"] == "latency" and s["threshold"] == 50 for s in slos) and \
                  any(s["metric"] == "cpu_usage" and s["threshold"] == 75 for s in slos)
        
        if is_fail:
            logger.critical(f"{Fore.RED}LLM timeout — SLOs non extraits. Renvoie ton intention.")
            return

        self.current_slos = slos
        self.viz.current_slos = slos
        self.mode = "enhanced"
        logger.info(f"[Core] Enhanced Mode Enabled. SLOs: {slos}")

    def _check_cooldown(self) -> Optional[Dict]:
        if self.last_migration_ts and (time.time() - self.last_migration_ts < self.config.COOLDOWN_SECONDS):
            rem = int(self.config.COOLDOWN_SECONDS - (time.time() - self.last_migration_ts))
            logger.info(f"{Fore.YELLOW}⏳ COOLDOWN ACTIF — Prochaine évaluation dans {rem}s")
            return {"decision": "stay", "reason": "Cooldown actif"}
        return None

    def _filter_active_metrics(self, collected: Dict, active_metrics: List[str]) -> Dict:
        res = {"vm_id": collected["vm_id"]}
        for m in ["cpu_usage", "ram_usage"]:
            if m in active_metrics and m in collected:
                res[m] = collected[m]
        return res

    def run_classic_flow(self, measurements: List[Dict]) -> Dict:
        """Executes the standard 7-step network-centric migration flow."""
        self._print_cycle_header()
        
        # Steps 1-4: Observation & Prediction
        log_step("1. STORE METRICS", "Core -> Database", measurements)
        self.db.save_metrics(measurements, "classic")
        
        log_step("2. LOAD HISTORY", "Core -> HistoryLoader", {"vms": self.config.VM_LIST})
        
        log_step("3. ML PREDICTION", "Core -> MLPredictor", {"mode": "classic"})
        preds = {m["vm_id"]: self.ml.get_prediction(m["rtt_ms"]) for m in measurements}
        self.viz.update_predictions(preds)
        
        log_step("4. RETOUR PREDICTIONS", "MLPredictor -> Core", preds)
        log_step("5. DECISION REQUEST", "Core -> DecisionIntelligence", preds)
        
        # Decision step with Cooldown
        cooldown = self._check_cooldown()
        dec = cooldown if cooldown else self.decision_engine.evaluate_classic_decision(measurements, preds)
        
        log_step("6. RETOUR DÉCISION", "DecisionIntelligence -> Core", dec)
        self.viz.update_data(measurements)
        
        # Execution
        self._execute_command(dec, "classic")
        return dec

    def run_enhanced_flow(self, rtt_measurements: List[Dict]) -> Dict:
        """Executes the advanced 9-step intention-centric migration flow."""
        self._print_cycle_header()
        
        # 1. Analyze
        log_step("1. ANALYZE SLOs", "Core -> MetricsManager", self.current_slos)
        needed = self.metrics_mgr.analyze_needed_metrics(self.current_slos)
        active = list(set(["latency"] + [s["metric"] for s in self.current_slos]))
        
        # 2-3. Collect & Store
        log_step("2. COLLECTION REQUEST", "MetricsManager -> Collector", needed)
        enriched = [{**rm, **self._filter_active_metrics(self.collector.collect_vm_metrics(rm["vm_id"]), active)} for rm in rtt_measurements]
        
        log_step("3. STORE METRICS", "Collector -> Database", enriched)
        self.db.save_metrics(enriched, "enhanced")
        
        # 4-6. ML Workflow
        log_step("4. LOAD HISTORY", "Core -> HistoryLoader", active)
        log_step("5. ML PREDICTION REQUEST", "Core -> MLPredictor", active)
        preds_map = {e["vm_id"]: {m: self.ml.get_enhanced_prediction(m, e.get("rtt_ms" if m == "latency" else m, 0)) for m in active} for e in enriched}
        self.viz.update_predictions(preds_map, mode="enhanced")
        
        log_step("6. RETOUR PREDICTIONS", "MLPredictor -> Core", preds_map)
        log_step("7. DECISION REQUEST", "Core -> DecisionIntelligence", self.current_slos)
        
        # Decision step with Cooldown
        cooldown = self._check_cooldown()
        dec = cooldown if cooldown else self.decision_engine.evaluate_enhanced_decision(enriched, preds_map, self.current_slos)
        
        log_step("8. RETOUR DÉCISION", "DecisionIntelligence -> Core", dec)
        self.viz.update_data(enriched)
        
        # Execution
        self._execute_command(dec, "enhanced")
        return dec

    def _execute_command(self, dec: Dict, mode: str):
        log_step(f"9. COMMAND" if mode == "enhanced" else "7. COMMAND", "Core -> Master", dec)
        ack = self._send_to_master(dec, mode)
        self.db.save_decision(dec, mode, ack)
        self._log_final(dec)

    def _send_to_master(self, dec: Dict, mode: str) -> bool:
        if dec["decision"] == "stay": return True
        self.last_migration_ts = time.time()
        try:
            payload = {**dec, "service": "my_service", "mode": mode, "timestamp": datetime.now(timezone.utc).isoformat()}
            resp = requests.post(self.config.MASTER_URL, json=payload, timeout=self.config.MASTER_TIMEOUT)
            return resp.status_code == 200
        except Exception:
            return False

    def _print_cycle_header(self):
        print(f"\n{Fore.WHITE}{'═'*60}")
        print(f"{Fore.WHITE}  🔄 NOUVEAU CYCLE — {datetime.now().strftime('%H:%M:%S')} | Mode: {self.mode.upper()}")
        print(f"{Fore.WHITE}{'═'*60}")

    def _log_final(self, dec: Dict):
        color = Fore.RED if dec["decision"] == "migrate" else Fore.GREEN
        logger.info(f"{color}{Style.BRIGHT}══ DÉCISION : {dec['decision'].upper()} | {dec['reason']} ══")
        print(f"{Fore.WHITE}{'═'*60}")

    def start(self):
        """Initializes all services and starts the background simulation loops."""
        self.db.init_db()
        self.latency_mgr.start_api()
        self.intent_mgr.start_api()
        
        threading.Thread(target=self.app.run, kwargs={'host': '0.0.0.0', 'port': self.config.CORE_PORT, 'threaded': True}, daemon=True).start()
        threading.Thread(target=self.viz.start_gui, daemon=True).start()
        
        logger.info("Orchestrator Hub-and-Spoke started.")
        while True:
            # Simulated data every interval if no real data is incoming
            if self.last_real_data_ts is None or (time.time() - self.last_real_data_ts > 30):
                sim = [{"vm_id": vm, "rtt_ms": random.uniform(5, 100)} for vm in self.config.VM_LIST]
                self.run_enhanced_flow(sim) if self.mode == "enhanced" else self.run_classic_flow(sim)
            time.sleep(self.config.COLLECTION_INTERVAL)

if __name__ == "__main__":
    try:
        OrchestratorCore(Config()).start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
