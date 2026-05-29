from __future__ import annotations

import functools
import json
import logging
import math
import random
import re
import socket
import sqlite3
import sys
import threading
import time
import unicodedata
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Protocol, Union

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

class LatencyHandler(Protocol):
    """Interface exposée au LatencyManagerSpoke."""
    @property
    def mode(self) -> str: ...
    def run_classic_flow(self, measurements: List[Dict]) -> Dict: ...
    def run_autonomous_flow(self, measurements: List[Dict]) -> Dict: ...
    def run_enhanced_flow(self, measurements: List[Dict]) -> Dict: ...
    def set_last_real_data_ts(self, ts: float) -> None: ...

class IntentHandler(Protocol):
    """Interface exposée au IntentManagerSpoke."""
    def set_user_intent(self, text: str) -> None: ...
    @property
    def current_slos(self) -> List[Dict]: ...
    def get_recent_context(self, window_seconds: int) -> List[Dict]: ...
    def get_metric_percentile(self, metric: str, percentile: float, window_seconds: int) -> Optional[float]: ...
    def get_metrics_for_mi(self, window_seconds: int) -> List[Dict]: ...

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
    def __init__(self, config: Config, core: LatencyHandler):
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
            
            self.core.set_last_real_data_ts(time.time())
            measurements = data["measurements"]
            
            # Route to correct flow based on mode
            if self.core.mode == "enhanced":
                decision = self.core.run_enhanced_flow(measurements)
            else:
                decision = self.core.run_autonomous_flow(measurements)
                
            return jsonify({"status": "received", "decision": decision}), 200

    def start_api(self):
        threading.Thread(
            target=self.app.run, 
            kwargs={'host': '0.0.0.0', 'port': self.config.LATENCY_PORT, 'threaded': True, 'use_reloader': False}, 
            daemon=True
        ).start()
        logger.info(f"[LatencyManager] API started on port {self.config.LATENCY_PORT}")

@dataclass
class ValidationResult:
    """Structure for SLO validation feedback and correction."""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    corrected_slos: List[Dict]

class IntentManagerSpoke:
    """Interfaces with LLM to extract SLOs from natural language with RAG context and validation."""
    
    class _SLOCoherenceValidator:
        """Internal helper to validate physical realism and weight balance of SLOs."""
        PHYSICAL_BOUNDS = {
            "latency":   {"min": 5.0,  "max": 2000.0, "unit": "ms"},
            "cpu_usage": {"min": 1.0,  "max": 99.0,   "unit": "%"},
            "ram_usage": {"min": 1.0,  "max": 99.0,   "unit": "%"}
        }

        def validate(self, slos: List[Dict]) -> ValidationResult:
            """Executes all validation checks and returns a structured result.
            
            Args:
                slos: List of enriched SLO dictionaries.
                
            Returns:
                ValidationResult with errors, warnings, and corrected list.
            """
            errors = []
            warnings = []
            
            # 1. Physical Bounds Check
            bound_errors = self._check_physical_bounds(slos)
            if bound_errors:
                errors.extend(bound_errors)
            
            # Filter out invalid SLOs (those that caused bound errors)
            valid_slos = []
            invalid_metrics = [e.split()[0] for e in bound_errors] # Crude way to identify metric
            for s in slos:
                if s["metric"] not in invalid_metrics:
                    valid_slos.append(s)
                else:
                    warnings.append(f"REJETÉ : SLO {s['metric']} hors bornes physiques.")

            if not valid_slos:
                return ValidationResult(is_valid=False, errors=errors, warnings=warnings, corrected_slos=[])

            # 2. Weight Sum Check
            weight_errors = self._check_weight_sum(valid_slos)
            if weight_errors:
                errors.extend(weight_errors)
                # Auto-correction of weights
                w = round(1.0 / len(valid_slos), 3)
                for s in valid_slos:
                    s["weight"] = w
                valid_slos[-1]["weight"] = round(1.0 - sum(s["weight"] for s in valid_slos[:-1]), 3)
                warnings.append(f"POIDS RÉÉQUILIBRÉS : Somme était invalide, redistribuée à {1.0/len(valid_slos):.3f} par SLO.")

            return ValidationResult(is_valid=True, errors=errors, warnings=warnings, corrected_slos=valid_slos)

        def _check_physical_bounds(self, slos: List[Dict]) -> List[str]:
            """Verifies that thresholds are within realistic operating ranges."""
            errors = []
            for s in slos:
                metric = s["metric"]
                val = s["threshold"]
                bounds = self.PHYSICAL_BOUNDS.get(metric)
                if not bounds:
                    continue
                
                if val < bounds["min"] or val > bounds["max"]:
                    errors.append(f"{metric} threshold {val}{bounds['unit']} est hors plage [{bounds['min']}, {bounds['max']}]{bounds['unit']}")
            return errors

        def _check_weight_sum(self, slos: List[Dict]) -> List[str]:
            """Ensures the sum of weights is approximately 1.0."""
            total = sum(s.get("weight", 0.0) for s in slos)
            if abs(total - 1.0) > 0.01:
                return [f"Somme des poids = {total:.2f}, attendu 1.0"]
            return []

    class _RAGContextBuilder:
        """Internal helper to build context for LLM by querying recent performance and SLOs through the Hub."""
        def __init__(self, config: Config, hub: IntentHandler, current_slos: List[Dict]):
            self.config = config
            self.hub = hub
            self.current_slos = current_slos

        def build_context(self) -> str:
            """Aggregates historical and current data into a textual context block."""
            if not self.current_slos:
                return ""

            try:
                # 1. Previous SLOs
                slo_lines = [
                    f"- {s['metric']} {s['operator']} {s['threshold']}{s['unit']} (weight: {s.get('weight', 0)})"
                    for s in self.current_slos
                ]
                slo_ctx = "Derniers SLOs actifs :\n" + "\n".join(slo_lines)

                # Fetch data from Hub instead of DatabaseSpoke directly
                recent_data = self.hub.get_recent_context(300)  # 5 minutes
                if not recent_data:
                    return f"{slo_ctx}\n\nAucune donnée de performance récente disponible dans SQLite."

                # 2. Performance actuelle (latest point per VM)
                latest_points: Dict[str, Dict] = {}
                for m in recent_data:
                    if m['vm_id'] not in latest_points:
                        latest_points[m['vm_id']] = m
                
                perf_lines = [
                    f"{vm} -> RTT: {data.get('rtt_ms', 0):.1f}ms | CPU: {data.get('cpu_usage', 0):.1f}% | RAM: {data.get('ram_usage', 0):.1f}%"
                    for vm, data in sorted(latest_points.items())
                ]
                perf_ctx = "Performance actuelle des VMs :\n" + "\n".join(perf_lines)

                # 3. Violations récentes (last 5 min)
                violations = {vm: {"latency": 0, "cpu_usage": 0, "ram_usage": 0} for vm in self.config.VM_LIST}
                for m in recent_data:
                    for slo in self.current_slos:
                        metric_name = slo['metric']
                        col = "rtt_ms" if metric_name == "latency" else metric_name
                        val = m.get(col)
                        if val is not None and val > slo['threshold']:
                            violations[m['vm_id']][metric_name] += 1
                
                viol_lines = []
                for vm, counts in sorted(violations.items()):
                    if sum(counts.values()) > 0:
                        v_detail = ", ".join([f"{c} violation(s) {m}" for m, c in counts.items() if c > 0])
                        viol_lines.append(f"{vm} : {v_detail}")
                
                viol_ctx = "Violations récentes (5 dernières min) :\n" + ("\n".join(viol_lines) if viol_lines else "Aucune violation détectée.")

                # 4. Seuils historiquement bons (sur 1h)
                percentile_lines = []
                any_valid = False
                metrics_to_check = sorted(list(set(s['metric'] for s in self.current_slos)))
                
                for metric in metrics_to_check:
                    p10 = self.hub.get_metric_percentile(metric, 10.0, 3600)
                    p25 = self.hub.get_metric_percentile(metric, 25.0, 3600)
                    p30 = self.hub.get_metric_percentile(metric, 30.0, 3600)
                    
                    unit = "ms" if metric == "latency" else "%"
                    if p10 is not None and p25 is not None and p30 is not None:
                        percentile_lines.append(f"- {metric:10}: P10={p10:.1f}{unit} | P25={p25:.1f}{unit} | P30={p30:.1f}{unit}")
                        any_valid = True
                    else:
                        percentile_lines.append(f"- {metric:10}: N/A (données insuffisantes)")

                if any_valid:
                    hist_ctx = "Seuils historiquement bons (sur 1h) :\n" + "\n".join(percentile_lines)
                    return f"{slo_ctx}\n\n{perf_ctx}\n\n{viol_ctx}\n\n{hist_ctx}"

                return f"{slo_ctx}\n\n{perf_ctx}\n\n{viol_ctx}"
            except Exception as e:
                logger.error(f"[RAG] Error building context: {e}")
                return ""

    def __init__(self, config: Config, core: IntentHandler, db_path: str, current_slos: List[Dict]):
        self.config = config
        self.core = core
        self.db_path = db_path
        self.slos_ref = current_slos
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

    class _KeywordsMatcher:
        """Fallback matcher that detects semantic profiles from keywords."""
        VOCABULARY = {
            "ux_sensitive": {
                "keywords": [
                    "rapide", "réactif", "délai", "lent", "lente",
                    "streaming", "interactif", "fluide", "réactivité",
                    "latence", "ping", "temps de réponse"
                ],
                "metrics": ["latency", "cpu_usage"],
                "weights": {"latency": 0.7, "cpu_usage": 0.3},
                "percentile": 25
            },
            "resource_heavy": {
                "keywords": [
                    "surcharge", "saturation", "charge", "nœuds",
                    "processeur", "cpu", "consommation", "ressources"
                ],
                "metrics": ["cpu_usage", "ram_usage"],
                "weights": {"cpu_usage": 0.5, "ram_usage": 0.5},
                "percentile": 25
            },
            "edge_critical": {
                "keywords": [
                    "edge", "proximité", "critiques", "utilisateurs",
                    "sensibles", "temps réel", "iot", "capteurs"
                ],
                "metrics": ["latency"],
                "weights": {"latency": 1.0},
                "percentile": 10
            },
            "stability": {
                "keywords": [
                    "stabilité", "stable", "continuité", "qualité",
                    "constante", "fiable", "fiabilité", "robuste"
                ],
                "metrics": ["latency", "cpu_usage", "ram_usage"],
                "weights": {
                    "latency": 0.34,
                    "cpu_usage": 0.33,
                    "ram_usage": 0.33
                },
                "percentile": 30
            }
        }

        def __init__(self, config: Config):
            self.config = config

        def detect_profile(self, text: str) -> str:
            """Analyzes the text to identify the most relevant semantic profile.
            
            Args:
                text: User intention text.
                
            Returns:
                The name of the detected profile or 'default'.
            """
            normalized = self._normalize_text(text)
            counts = {profile: 0 for profile in self.VOCABULARY}
            
            for profile, data in self.VOCABULARY.items():
                for kw in data["keywords"]:
                    if kw in normalized:
                        counts[profile] += 1
            
            best_profile = max(counts, key=counts.get)
            return best_profile if counts[best_profile] > 0 else "default"

        def build_slos(self, profile: str, hub: IntentHandler) -> List[Dict]:
            """Constructs SLOs based on the detected profile and historical data.
            
            Args:
                profile: The detected semantic profile.
                hub: Hub interface to fetch percentiles.
                
            Returns:
                List of enriched SLO dictionaries.
            """
            slos = []
            if profile == "default":
                # Fallback to Config defaults for all 3 metrics
                raw_slos = [
                    {"metric": "latency", "operator": "<", "threshold": self.config.DEFAULT_LATENCY_THRESHOLD, "unit": "ms", "weight": 0.34},
                    {"metric": "cpu_usage", "operator": "<", "threshold": self.config.DEFAULT_CPU_THRESHOLD, "unit": "%", "weight": 0.33},
                    {"metric": "ram_usage", "operator": "<", "threshold": self.config.DEFAULT_RAM_THRESHOLD, "unit": "%", "weight": 0.33}
                ]
            else:
                data = self.VOCABULARY[profile]
                for metric in data["metrics"]:
                    p_val = hub.get_metric_percentile(metric, data["percentile"], 3600)
                    
                    if p_val is None:
                        # Fallback to Config thresholds
                        if metric == "latency":
                            p_val = self.config.DEFAULT_LATENCY_THRESHOLD
                        elif metric == "cpu_usage":
                            p_val = self.config.DEFAULT_CPU_THRESHOLD
                        else:
                            p_val = self.config.DEFAULT_RAM_THRESHOLD
                            
                    slos.append({
                        "metric": metric,
                        "operator": "<",
                        "threshold": p_val,
                        "unit": "ms" if metric == "latency" else "%",
                        "weight": data["weights"].get(metric, 0.0)
                    })
                raw_slos = slos

            # Use the outer class method via the instance if needed, 
            # but _enrich_slo_schema is available in IntentManagerSpoke.
            # Here we assume this is called within IntentManagerSpoke context.
            return raw_slos

        def _normalize_text(self, text: str) -> str:
            """Lowercases and removes accents from text."""
            text = text.lower()
            return "".join(
                c for c in unicodedata.normalize('NFD', text)
                if unicodedata.category(c) != 'Mn'
            )

    def _build_system_prompt(self, context: str) -> str:
        """Construit le prompt système expert pour le LLM avec règles d'inférence et exemples few-shot.
        
        Args:
            context: Le contexte textuel généré par le RAGContextBuilder.
            
        Returns:
            Le prompt système complet.
        """
        return (
            "Tu es un expert SLO pour systèmes distribués.\n"
            "Tu DOIS répondre UNIQUEMENT avec un JSON array. Aucun texte avant ou après le JSON.\n\n"
            "Chaque objet JSON contient exactement :\n"
            "{\n"
            "  \"metric\": \"latency\"|\"cpu_usage\"|\"ram_usage\",\n"
            "  \"operator\": \"<\"|\"<=\"|\">\"|\">=\",\n"
            "  \"threshold\": <float>,\n"
            "  \"unit\": \"ms\"|\"%\",\n"
            "  \"weight\": <float entre 0.0 et 1.0>\n"
            "}\n"
            "Somme des weights DOIT être 1.0.\n\n"
            "Règle d'inférence :\n"
            "Si l'intention ne contient pas de chiffres :\n"
            "→ Utilise les valeurs P25 du contexte comme seuils\n"
            "→ Si P25 non disponible : utilise les valeurs actuelles des VMs réduites de 20%\n"
            "→ Priorise les métriques selon le vocabulaire :\n"
            "  \"rapide/réactif/délai\" → latency en priorité\n"
            "  \"charge/surcharge/CPU\" → cpu_usage en priorité\n"
            "  \"mémoire/RAM\"          → ram_usage en priorité\n"
            "  intention générale     → toutes métriques\n\n"
            "Exemple 1 — Intention abstraite :\n"
            "Contexte : RTT moyen=38ms, P25 latency=24ms, CPU moyen=58%, P25 cpu=45%\n"
            "Intention : \"Je veux éviter les ralentissements\"\n"
            "Réponse : [{\"metric\":\"latency\",\"operator\":\"<\",\"threshold\":24,\"unit\":\"ms\",\"weight\":0.7},{\"metric\":\"cpu_usage\",\"operator\":\"<\",\"threshold\":45,\"unit\":\"%\",\"weight\":0.3}]\n\n"
            "Exemple 2 — Intention numérique :\n"
            "Contexte : (quelconque)\n"
            "Intention : \"latence < 15ms et CPU < 60%\"\n"
            "Réponse : [{\"metric\":\"latency\",\"operator\":\"<\",\"threshold\":15,\"unit\":\"ms\",\"weight\":0.5},{\"metric\":\"cpu_usage\",\"operator\":\"<\",\"threshold\":60,\"unit\":\"%\",\"weight\":0.5}]\n\n"
            "Exemple 3 — Intention mixte :\n"
            "Contexte : P25 ram=51%\n"
            "Intention : \"CPU < 70% et garde la RAM stable\"\n"
            "Réponse : [{\"metric\":\"cpu_usage\",\"operator\":\"<\",\"threshold\":70,\"unit\":\"%\",\"weight\":0.6},{\"metric\":\"ram_usage\",\"operator\":\"<\",\"threshold\":51,\"unit\":\"%\",\"weight\":0.4}]\n\n"
            f"Contexte système actuel :\n{context}"
        )

    def query_intent_engine(self, text: str) -> tuple[List[Dict], bool]:
        """Queries Ollama LLM to parse SLOs from user text with RAG context and validation."""
        success = False
        raw_slos = []

        try:
            # Build RAG Context through the Hub
            builder = self._RAGContextBuilder(self.config, self.core, self.slos_ref)
            context = builder.build_context()

            payload = {
                "model": "qwen2.5",
                "messages": [
                    {
                        "role": "system",
                        "content": self._build_system_prompt(context)
                    },
                    {"role": "user", "content": text}
                ],
                "stream": False
            }
            resp = requests.post(self.config.INTENT_ENGINE_URL, json=payload, timeout=90)
            llm_text = resp.json().get("message", {}).get("content", "").lower()
            
            raw_slos, success = self._parse_slos_from_text(llm_text)
        except Exception as e:
            logger.warning(f"[Intent] LLM Error: {e}. Falling back to smart matching.")
            success = False
            raw_slos = []

        # Hors du try/except — toujours exécuté si le LLM a échoué ou retourné du JSON invalide
        if not success:
            # Niveau 2 : Regex sur le texte original
            raw_slos, regex_success = self._parse_slos_from_text(text)
            
            if not regex_success:
                # Niveau 3 : Keywords Matcher
                logger.info("[Intent] Keywords Matcher activé")
                matcher = self._KeywordsMatcher(self.config)
                profile = matcher.detect_profile(text)
                logger.info(f"[Intent] Profil détecté : {profile}")
                raw_slos = [
                    self._enrich_slo_schema(s) 
                    for s in matcher.build_slos(profile, self.core)
                ]

        # Validation commune — toujours exécutée
        validator = self._SLOCoherenceValidator()
        result = validator.validate(raw_slos)
        
        for err in result.errors:
            logger.warning(f"[Intent] Erreur de cohérence : {err}")
        for warn in result.warnings:
            logger.warning(f"[Intent] Validation : {warn}")
            
        if not result.is_valid:
            logger.critical("[Intent] SLOs invalides — aucun SLO valide extrait")
            return [], False
            
        return result.corrected_slos, True

    def _enrich_slo_schema(self, slo: Dict) -> Dict:
        """Enriches an SLO object with OpenSLO-inspired fields if missing.
        
        Args:
            slo: Dictionary representing a raw SLO.
            
        Returns:
            The enriched SLO dictionary.
        """
        defaults = {
            "target": 0.99,
            "window": "5m",
            "budget_remaining": 100.0,
            "violations": 0
        }
        for key, val in defaults.items():
            if key not in slo:
                slo[key] = val
        return slo

    def _parse_slos_from_text(self, text: str) -> tuple[List[Dict], bool]:
        """Parses JSON or uses regex fallback to extract SLO objects."""
        # JSON parsing
        try:
            start, end = text.find('['), text.rfind(']')
            if start != -1 and end != -1:
                slos = json.loads(text[start:end+1])
                if isinstance(slos, list) and slos:
                    return [self._enrich_slo_schema(s) for s in slos], True
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
            return [self._enrich_slo_schema(s) for s in slos], True

        # Absolute Fallback
        raw_fallback = [
            {"metric": "latency", "operator": "<", "threshold": 50, "unit": "ms", "weight": 0.34},
            {"metric": "cpu_usage", "operator": "<", "threshold": 75, "unit": "%", "weight": 0.33},
            {"metric": "ram_usage", "operator": "<", "threshold": 80, "unit": "%", "weight": 0.33}
        ]
        return [self._enrich_slo_schema(s) for s in raw_fallback], False

class MetricsManagerSpoke:
    """Intelligently determines which physical metrics are needed using Mutual Information (MI)."""
    
    METRICS = ["latency", "cpu_usage", "ram_usage"]
    MI_THRESHOLD = 0.05   # Minimum score to consider a metric relevant
    MIN_POINTS = 5        # Minimum points needed to activate MI scoring

    def analyze_needed_metrics(self, slos: List[Dict], hub: Optional[IntentHandler] = None, window_seconds: int = 300) -> tuple[List[str], Dict[str, float]]:
        """Identifies metrics to collect by combining SLO requirements and MI-based relevance.
        
        Args:
            slos: Active SLOs.
            hub: Hub interface to fetch data for MI.
            window_seconds: Lookback window for MI calculation.
            
        Returns:
            A tuple (sorted_list_of_metrics, mi_scores_dict).
        """
        # 1. Fallback statique (métriques explicitement dans les SLOs)
        static_metrics = set(slo["metric"] for slo in slos if slo["metric"] in ["cpu_usage", "ram_usage"])
        
        if hub is None:
            return (sorted(list(static_metrics)), {})

        # 2. MI Scoring
        data = hub.get_metrics_for_mi(window_seconds)
        if len(data) < self.MIN_POINTS:
            return (sorted(list(static_metrics)), {})

        scores = self.compute_mi_scores(data)
        logger.info(f"[MetricsManager] MI Scores: { {m: round(s, 3) for m, s in scores.items()} }")
        
        # 3. Sélection intelligente (scores >= seuil)
        intelligent_metrics = set(m for m, s in scores.items() if s >= self.MI_THRESHOLD and m in ["cpu_usage", "ram_usage"])
        
        # 4. Union des deux approches
        final_metrics = static_metrics.union(intelligent_metrics)
        return (sorted(list(final_metrics)), scores)

    def compute_mi_scores(self, data: List[Dict]) -> Dict[str, float]:
        """Calculates normalized Mutual Information for each metric against is_violation.
        
        Args:
            data: List of dictionaries containing metrics and is_violation.
            
        Returns:
            Dictionary mapping metric names to their MI scores [0, 1].
        """
        scores = {}
        y_vals = [int(row["is_violation"]) for row in data if row.get("is_violation") is not None]
        
        if not y_vals or len(set(y_vals)) < 2:
            return {m: 0.0 for m in self.METRICS}

        for metric in self.METRICS:
            col = "rtt_ms" if metric == "latency" else metric
            x_vals = [row.get(col) for row in data if row.get(col) is not None]
            
            if len(x_vals) < self.MIN_POINTS:
                scores[metric] = 0.0
            else:
                # Aligner X et Y sur les indices valides (cas où certains points manqueraient)
                # Dans notre cas, save_metrics écrit tout le bloc, mais soyons prudents.
                valid_pairs = [(row.get(col), int(row["is_violation"])) 
                              for row in data 
                              if row.get(col) is not None and row.get("is_violation") is not None]
                
                if len(valid_pairs) < self.MIN_POINTS:
                    scores[metric] = 0.0
                else:
                    x_clean = [p[0] for p in valid_pairs]
                    y_clean = [p[1] for p in valid_pairs]
                    scores[metric] = self._compute_mi(x_clean, y_clean)
        
        return scores

    def _compute_mi(self, x_vals: List[float], y_vals: List[int]) -> float:
        """Core MI calculation logic with median discretization and normalization."""
        n = len(x_vals)
        if n == 0: return 0.0
        
        # 1. Discrétisation de X par la médiane
        sorted_x = sorted(x_vals)
        mid = n // 2
        median = sorted_x[mid] if n % 2 != 0 else (sorted_x[mid-1] + sorted_x[mid]) / 2.0
        
        x_bins = [1 if x > median else 0 for x in x_vals]
        
        # 2. Table de contingence 2x2
        # p(x,y)
        counts = {(0,0): 0, (0,1): 0, (1,0): 0, (1,1): 0}
        for xb, yb in zip(x_bins, y_vals):
            counts[(xb, yb)] += 1
            
        # 3. Probabilités marginales et conjointes
        px = {0: x_bins.count(0) / n, 1: x_bins.count(1) / n}
        py = {0: y_vals.count(0) / n, 1: y_vals.count(1) / n}
        pxy = {k: v / n for k, v in counts.items()}
        
        # 4. Calcul MI
        mi = 0.0
        for (xb, yb), p_joint in pxy.items():
            if p_joint > 0:
                p_prod = px[xb] * py[yb]
                if p_prod > 0:
                    mi += p_joint * math.log2(p_joint / p_prod)
        
        # 5. Normalisation par entropie maximale
        hx = self._entropy(list(px.values()))
        hy = self._entropy(list(py.values()))
        
        denom = max(hx, hy)
        return mi / denom if denom > 0 else 0.0

    def _entropy(self, probs: List[float]) -> float:
        """Calculates Shannon entropy."""
        return -sum(p * math.log2(p) for p in probs if p > 0)

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
            return {
                "vm_id": vm_id, 
                "cpu_usage": data["cpu_usage"], 
                "ram_usage": data["ram_usage"],
                "is_active_service": data.get("is_active_service", False)
            }
        except Exception:
            # Fallback to simulation if VM is unreachable
            return {
                "vm_id": vm_id,
                "cpu_usage": random.uniform(20, 95),
                "ram_usage": random.uniform(30, 90),
                "is_active_service": False
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

    def evaluate_classic_decision(self, current_data: List[Dict], predictions_map: Dict[str, List[float]], service_vm=None) -> Dict:
        """Threshold-based decision for RTT only."""
        threshold = self.config.DEFAULT_LATENCY_THRESHOLD
        
        if service_vm is None:
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

        entry = next((e for e in current_data if e["vm_id"] == service_vm), None)
        if not entry:
            return {"decision": "stay", "reason": "Service VM non trouvée dans ce cycle"}
        
        preds = predictions_map.get(service_vm, [])
        if entry["rtt_ms"] > threshold or calculate_weighted_mean(preds) > threshold:
            targets = [e for e in current_data if e["vm_id"] != service_vm]
            if not targets:
                return {"decision": "stay", "reason": "Aucune cible disponible"}
            best = min(targets, key=lambda x: x["rtt_ms"])
            reason = f"{service_vm} RTT {entry['rtt_ms']:.1f}ms > {threshold}ms"
            return {"decision": "migrate", "from_vm": service_vm, "to_vm": best["vm_id"], "reason": reason}
        return {"decision": "stay", "reason": "Nominal"}

    def _compute_vm_score(self, target: Dict, predictions_map: Dict, slos: List[Dict], weights: Dict[str, float]) -> float:
        """Calculates a selection score for a target VM considering predictions and budget bonus.
        
        Args:
            target: Dictionary of current VM metrics.
            predictions_map: Map of predictions for all VMs.
            slos: List of active SLO objects with budget information.
            weights: Weight mapping for metrics.
            
        Returns:
            Final score (lower is better).
        """
        vm_id = target["vm_id"]
        vm_preds = predictions_map.get(vm_id, {})
        
        # 1. Base prediction score (weighted mean of predicted future values)
        score_pred = 0.0
        for slo in slos:
            m_name = slo["metric"]
            preds = vm_preds.get(m_name, [])
            score_pred += weights.get(m_name, 0.0) * calculate_weighted_mean(preds)
        
        # 2. Budget bonus (incentive for VMs respecting SLOs with healthy budgets)
        budget_bonus = 0.0
        for slo in slos:
            m_name, threshold = slo["metric"], slo["threshold"]
            val = target.get("rtt_ms" if m_name == "latency" else m_name)
            
            # Bonus if current value respects SLO
            if val is not None and val <= threshold:
                bonus = slo.get("weight", 0.0) * (slo.get("budget_remaining", 100.0) / 100.0)
                budget_bonus += bonus
        
        # 3. Final score: pred - budget_bonus (capped bonus at 1.0)
        return score_pred - min(budget_bonus, 1.0)

    def evaluate_enhanced_decision(self, current_data: List[Dict], predictions_map: Dict[str, Dict[str, List[float]]], slos: List[Dict], service_vm=None) -> Dict:
        """Intention-based decision using multi-criteria weighted scoring with severity prioritization and budget awareness."""
        weights = {m: 0.0 for m in ["latency", "cpu_usage", "ram_usage"]}
        for s in slos: weights[s["metric"]] = s.get("weight", 0.0)

        # Phase 1: Collect all violations with their severity
        violations = []
        
        # Filtre current_data pour n'évaluer que service_vm
        eval_data = current_data
        if service_vm:
            service_entry = next((e for e in current_data if e["vm_id"] == service_vm), None)
            if not service_entry:
                return {"decision": "stay", "reason": "Service VM non trouvée"}
            eval_data = [service_entry]

        for entry in eval_data:
            vm_id = entry["vm_id"]
            for slo in slos:
                m_name, threshold = slo["metric"], slo["threshold"]
                val = entry.get("rtt_ms" if m_name == "latency" else m_name)
                
                if val is None: continue
                
                preds = predictions_map.get(vm_id, {}).get(m_name, [])
                if val > threshold or calculate_weighted_mean(preds) > threshold:
                    severity = (val - threshold) / threshold if threshold > 0 else val
                    violations.append({
                        "vm_id": vm_id,
                        "metric": m_name,
                        "val": val,
                        "threshold": threshold,
                        "severity": severity
                    })

        # Phase 2: Process the most severe violation first
        if violations:
            worst = max(violations, key=lambda x: x["severity"])
            worst_vm = worst["vm_id"]
            
            # Filter valid targets
            targets = [e for e in current_data if e["vm_id"] != worst_vm]
            
            def target_respects_slo(t: Dict, s: Dict) -> bool:
                key = "rtt_ms" if s["metric"] == "latency" else s["metric"]
                val = t.get(key)
                if val is None:
                    return True
                return val <= s["threshold"]

            valid = [t for t in targets if all(
                target_respects_slo(t, s) for s in slos
            )]
            
            final_pool = valid if valid else targets
            # Use budget-aware scoring
            best = min(final_pool, key=lambda t: self._compute_vm_score(t, predictions_map, slos, weights))
            
            reason = f"{worst_vm} {worst['metric']} {worst['val']:.1f} > SLO {worst['threshold']} (Sévérité: {worst['severity']:.2f})"
            return {"decision": "migrate", "from_vm": worst_vm, "to_vm": best["vm_id"], "reason": reason}

        return {"decision": "stay", "reason": "All SLOs satisfied"}

class DatabaseSpoke:
    """Persistent thread-safe storage for metrics and decisions."""
    def __init__(self, config: Config):
        self.config = config

    def init_db(self):
        with sqlite3.connect(self.config.DB_NAME) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS metrics (id INTEGER PRIMARY KEY, vm_id TEXT, rtt_ms REAL, cpu_usage REAL, ram_usage REAL, mode TEXT, timestamp TEXT)")
            conn.execute("CREATE TABLE IF NOT EXISTS decisions (id INTEGER PRIMARY KEY, decision TEXT, from_vm TEXT, to_vm TEXT, reason TEXT, mode TEXT, master_ack INTEGER, timestamp TEXT)")
            # Migration douce pour budget_remaining
            try:
                conn.execute("ALTER TABLE decisions ADD COLUMN budget_remaining REAL")
            except sqlite3.OperationalError:
                pass # Colonne déjà existante
            
            # Migration douce pour is_violation
            try:
                conn.execute("ALTER TABLE metrics ADD COLUMN is_violation INTEGER DEFAULT 0")
            except sqlite3.OperationalError:
                pass # Colonne déjà existante

    @safe_call(None, "DatabaseSpoke.save_metrics")
    def save_metrics(self, measurements: List[Dict], mode: str, slos: List[Dict] = []):
        """Saves a batch of VM metrics to SQLite, identifying violations in real-time.
        
        Args:
            measurements: List of metric dictionaries.
            mode: The orchestrator mode (classic/enhanced).
            slos: Active SLOs to check for violations.
        """
        ts = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.config.DB_NAME) as conn:
            for m in measurements:
                is_violation = 0
                for slo in slos:
                    col = "rtt_ms" if slo["metric"] == "latency" else slo["metric"]
                    val = m.get(col)
                    if val is not None and val > slo["threshold"]:
                        is_violation = 1
                        break
                
                conn.execute(
                    "INSERT INTO metrics (vm_id, rtt_ms, cpu_usage, ram_usage, mode, is_violation, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?)", 
                    (m["vm_id"], m.get("rtt_ms", 0), m.get("cpu_usage", 0), m.get("ram_usage", 0), mode, is_violation, ts)
                )

    @safe_call(False, "DatabaseSpoke.save_decision")
    def save_decision(self, dec: Dict, mode: str, ack: bool):
        ts = datetime.now(timezone.utc).isoformat()
        with sqlite3.connect(self.config.DB_NAME) as conn:
            conn.execute("INSERT INTO decisions (decision, from_vm, to_vm, reason, mode, master_ack, budget_remaining, timestamp) VALUES (?, ?, ?, ?, ?, ?, ?, ?)", 
                         (dec["decision"], dec.get("from_vm"), dec.get("to_vm"), dec["reason"], mode, 1 if ack else 0, dec.get("budget_remaining", 100.0), ts))
        return True

    def get_slo_violations(self, metric: str, threshold: float, operator: str, window_seconds: int) -> int:
        """Counts metrics rows where the value exceeds the SLO threshold within a window.
        
        Args:
            metric: The metric name (latency, cpu_usage, ram_usage).
            threshold: The numeric threshold.
            operator: The comparison operator from the SLO (<, <=, >, >=).
            window_seconds: The lookback window in seconds.
            
        Returns:
            The count of violations.
        """
        try:
            col = "rtt_ms" if metric == "latency" else metric
            cutoff = (datetime.now(timezone.utc) - timedelta(seconds=window_seconds)).isoformat()
            
            # Déterminer la condition de violation (l'inverse de l'opérateur SLO)
            # Si SLO est 'val < threshold', violation est 'val >= threshold'
            op_map = {"<": ">=", "<=": ">", ">": "<=", ">=": "<"}
            sql_op = op_map.get(operator, ">")
            
            with sqlite3.connect(self.config.DB_NAME) as conn:
                cur = conn.execute(
                    f"SELECT COUNT(*) FROM metrics WHERE {col} {sql_op} ? AND timestamp > ?",
                    (threshold, cutoff)
                )
                return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"[Database] Error counting violations for {metric}: {e}")
            return 0

    def get_window_count(self, window_seconds: int) -> int:
        """Returns the total number of metric entries in the given window.
        
        Args:
            window_seconds: The lookback window in seconds.
            
        Returns:
            The count of points.
        """
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(seconds=window_seconds)).isoformat()
            with sqlite3.connect(self.config.DB_NAME) as conn:
                cur = conn.execute("SELECT COUNT(*) FROM metrics WHERE timestamp > ?", (cutoff,))
                return cur.fetchone()[0]
        except Exception as e:
            logger.error(f"[Database] Error counting window points: {e}")
            return 0

    def get_metrics_with_violations(self, window_seconds: int) -> List[Dict]:
        """Retrieves metrics and violation status for Mutual Information calculation.
        
        Args:
            window_seconds: Lookback window in seconds.
            
        Returns:
            List of dictionaries containing metrics and is_violation flag.
        """
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(seconds=window_seconds)).isoformat()
            with sqlite3.connect(self.config.DB_NAME) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT vm_id, rtt_ms, cpu_usage, ram_usage, is_violation, timestamp FROM metrics "
                    "WHERE timestamp > ? ORDER BY timestamp DESC",
                    (cutoff,)
                )
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"[Database] Error fetching metrics for MI: {e}")
            return []

    def get_recent_metrics(self, window_seconds: int) -> List[Dict]:
        """Retrieves metrics from the last window_seconds for RAG context.
        
        Args:
            window_seconds: Time window in seconds to look back.
            
        Returns:
            List of dictionaries containing VM metrics and timestamps.
        """
        try:
            cutoff = (datetime.now(timezone.utc) - timedelta(seconds=window_seconds)).isoformat()
            with sqlite3.connect(self.config.DB_NAME) as conn:
                conn.row_factory = sqlite3.Row
                cur = conn.execute(
                    "SELECT vm_id, rtt_ms, cpu_usage, ram_usage, timestamp FROM metrics "
                    "WHERE timestamp > ? ORDER BY timestamp DESC", 
                    (cutoff,)
                )
                return [dict(row) for row in cur.fetchall()]
        except Exception as e:
            logger.error(f"[Database] Error fetching recent metrics: {e}")
            return []

    def get_historical_percentiles(self, metric: str, percentile: float, window_seconds: int = 3600) -> Optional[float]:
        """Calculates a specific percentile for a metric over a historical window.
        
        Args:
            metric: The metric name (latency, cpu_usage, ram_usage).
            percentile: The percentile to calculate (0-100).
            window_seconds: Lookback window in seconds.
            
        Returns:
            The calculated percentile value or None if insufficient data.
        """
        try:
            col = "rtt_ms" if metric == "latency" else metric
            cutoff = (datetime.now(timezone.utc) - timedelta(seconds=window_seconds)).isoformat()
            
            with sqlite3.connect(self.config.DB_NAME) as conn:
                cur = conn.execute(
                    f"SELECT {col} FROM metrics WHERE timestamp > ? AND {col} IS NOT NULL",
                    (cutoff,)
                )
                values = [row[0] for row in cur.fetchall()]
            
            if len(values) < 10:
                return None
                
            values.sort()
            n = len(values)
            index = (percentile / 100.0) * (n - 1)
            
            if index.is_integer():
                return round(values[int(index)], 2)
            else:
                lower = int(index)
                upper = lower + 1
                weight = index - lower
                return round(values[lower] + weight * (values[upper] - values[lower]), 2)
                
        except Exception as e:
            logger.error(f"[Database] Error calculating percentile for {metric}: {e}")
            return None

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
        self.current_service_vm = None  # VM qui héberge le service
        self.last_decision = {"decision": "stay", "reason": ""}

    def update_decision(self, dec: Dict, current_vm: str = None):
        self.last_decision = dec

    def update_data(self, current_data: List[Dict]):
        # Découverte dynamique de la VM active
        active_vm = next((e["vm_id"] for e in current_data if e.get("is_active_service")), None)
        if active_vm:
            self.current_service_vm = active_vm

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
                    
                    if vm == self.current_service_vm and m_key == "rtt":
                        ax.set_title(f"{m_key.upper()} {vm} 🟢 SERVICE", 
                                     color="green", fontweight="bold")
                    elif vm == self.current_service_vm:
                        ax.set_title(f"{m_key.upper()} {vm} 🟢", 
                                     color="green", fontweight="bold")
                    else:
                        ax.set_title(f"{m_key.upper()} {vm}")
                        
                    ax.set_ylim(0, 100); ax.legend(loc="upper right")
            
            dec = self.last_decision
            if dec["decision"] == "migrate":
                title = (f"🔴 DÉCISION : MIGRATE "
                         f"{dec.get('from_vm', '?')} → {dec.get('to_vm', '?')}"
                         f"  |  {dec.get('reason', '')}")
                color = "red"
            else:
                title = f"🟢 DÉCISION : STAY  |  {dec.get('reason', 'Nominal')}"
                color = "green"

            fig.suptitle(title, fontsize=11, color=color, 
                         fontweight="bold", y=0.02)
            
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
        
        self.mode = "autonomous"
        self.current_slos = [
            {
                "metric": "latency",
                "operator": "<",
                "threshold": config.DEFAULT_LATENCY_THRESHOLD,
                "unit": "ms",
                "weight": 0.34,
                "target": 0.99,
                "window": "5m",
                "budget_remaining": 100.0,
                "violations": 0
            },
            {
                "metric": "cpu_usage", 
                "operator": "<",
                "threshold": config.DEFAULT_CPU_THRESHOLD,
                "unit": "%",
                "weight": 0.33,
                "target": 0.99,
                "window": "5m",
                "budget_remaining": 100.0,
                "violations": 0
            },
            {
                "metric": "ram_usage",
                "operator": "<", 
                "threshold": config.DEFAULT_RAM_THRESHOLD,
                "unit": "%",
                "weight": 0.33,
                "target": 0.99,
                "window": "5m",
                "budget_remaining": 100.0,
                "violations": 0
            }
        ]
        self.last_real_data_ts = None
        self.last_migration_ts = None
        self._last_known_active_vm: Optional[str] = None
        self.last_mi_scores: Dict[str, float] = {}
        
        self.intent_mgr = IntentManagerSpoke(config, self, config.DB_NAME, self.current_slos)
        self.metrics_mgr = MetricsManagerSpoke()
        self.collector = CollectorSpoke(config)
        self.latency_mgr = LatencyManagerSpoke(config, self)
        self._lock = threading.RLock()
        
        self.start_ts = time.time()
        self.app = Flask(f"{__name__}_core")
        self._setup_routes()

    def _setup_routes(self):
        @self.app.route('/status', methods=['GET'])
        def get_status():
            return jsonify({
                "mode": self.mode, 
                "uptime": int(time.time() - self.start_ts), 
                "slos": self.current_slos,
                "service_vm": self._last_known_active_vm,
                "mi_scores": self.last_mi_scores
            }), 200

    def _find_active_vm(self, enriched: List[Dict]) -> Optional[str]:
        """Retourne le vm_id de la VM active dans le payload courant."""
        for e in enriched:
            if e.get("is_active_service"):
                return e["vm_id"]
        return None

    def get_recent_context(self, window_seconds: int) -> List[Dict]:
        """Hub method to provide RAG context without direct spoke-to-spoke access."""
        return self.db.get_recent_metrics(window_seconds)

    def get_metric_percentile(self, metric: str, percentile: float, window_seconds: int = 3600) -> Optional[float]:
        """Hub method to calculate historical percentiles via the database spoke.
        
        Args:
            metric: The metric name (latency, cpu_usage, ram_usage).
            percentile: The percentile to calculate (0-100).
            window_seconds: Lookback window in seconds.
            
        Returns:
            The calculated percentile value or None.
        """
        return self.db.get_historical_percentiles(metric, percentile, window_seconds)

    def get_metrics_for_mi(self, window_seconds: int = 300) -> List[Dict]:
        """Hub method to retrieve metrics and violations for MI scoring.
        
        Args:
            window_seconds: Lookback window in seconds.
            
        Returns:
            List of dictionaries containing metrics and is_violation flag.
        """
        return self.db.get_metrics_with_violations(window_seconds)

    def set_user_intent(self, text: str):
        slos, success = self.intent_mgr.query_intent_engine(text)
        
        if not success or not slos:
            logger.critical(f"{Fore.RED}Intention rejetée — Aucun SLO valide extrait ou échec LLM. État inchangé.")
            return

        total_weight = sum(s.get("weight", 0) for s in slos)
        if total_weight > 0 and abs(total_weight - 1.0) > 0.01:
            for s in slos:
                s["weight"] = round(s.get("weight", 0) / total_weight, 3)
                
        with self._lock:
            self.current_slos = slos
            self.viz.current_slos = slos
            self.mode = "enhanced"
        
        self._refresh_slo_budgets()
        logger.info(f"[Core] Enhanced Mode Enabled. SLOs: {slos}")

    def set_last_real_data_ts(self, ts: float) -> None:
        with self._lock:
            self.last_real_data_ts = ts

    def _is_budget_exhausted(self) -> bool:
        """Checks if any active SLO has a remaining budget of 0%."""
        if not self.current_slos:
            return False
        with self._lock:
            return any(slo.get("budget_remaining") == 0.0 for slo in self.current_slos)

    def _check_cooldown(self) -> Optional[Dict]:
        with self._lock:
            if self.last_migration_ts and (time.time() - self.last_migration_ts < self.config.COOLDOWN_SECONDS):
                # Check for Budget Exhaustion Override (Enhanced Mode only)
                if self.mode == "enhanced" and self._is_budget_exhausted():
                    logger.critical(f"{Fore.RED}{Style.BRIGHT}⚠️ BUDGET ÉPUISÉ — Override cooldown")
                    return None
                    
                rem = int(self.config.COOLDOWN_SECONDS - (time.time() - self.last_migration_ts))
                logger.info(f"{Fore.YELLOW}⏳ COOLDOWN ACTIF — Prochaine évaluation dans {rem}s")
                return {"decision": "stay", "reason": "Cooldown actif"}
        return None

    def _filter_active_metrics(self, collected: Dict, active_metrics: List[str], fallback_vm_id: str) -> Dict:
        if not collected or "vm_id" not in collected:
            return {"vm_id": fallback_vm_id}
        res = {"vm_id": collected["vm_id"]}
        for m in ["cpu_usage", "ram_usage"]:
            if m in active_metrics and m in collected:
                res[m] = collected[m]
        return res

    def _refresh_slo_budgets(self):
        """Updates violations and budget_remaining for all active SLOs using historical data."""
        if not self.current_slos:
            return

        window_seconds = 300 # 5m par défaut
        total_points = self.db.get_window_count(window_seconds)
        
        with self._lock:
            for slo in self.current_slos:
                violations = self.db.get_slo_violations(
                    slo["metric"], slo["threshold"], slo["operator"], window_seconds
                )
                slo["violations"] = violations
                
                if total_points > 0:
                    budget = 100.0 * (1 - (violations / total_points))
                    slo["budget_remaining"] = max(0.0, min(100.0, round(budget, 2)))
                else:
                    slo["budget_remaining"] = 100.0

    def run_classic_flow(self, measurements: List[Dict]) -> Dict:
        """Executes the standard 7-step network-centric migration flow. (DEPRECATED)"""
        logger.warning("[Core] run_classic_flow() est déprécié. Utiliser run_autonomous_flow() à la place.")
        self._print_cycle_header()
        
        # Découverte dynamique du service
        service_vm = self._find_active_vm(measurements)
        if service_vm is None:
            logger.warning("[Core] Aucune VM active détectée dans le payload classic.")

        # Steps 1-4: Observation & Prediction
        log_step("1. STORE METRICS", "Core -> Database", measurements)
        self.db.save_metrics(measurements, "classic", self.current_slos)
        self._refresh_slo_budgets()
        
        log_step("2. LOAD HISTORY", "Core -> HistoryLoader", {"vms": self.config.VM_LIST})
        
        log_step("3. ML PREDICTION", "Core -> MLPredictor", {"mode": "classic"})
        preds = {m["vm_id"]: self.ml.get_prediction(m["rtt_ms"]) for m in measurements}
        self.viz.update_predictions(preds)
        
        log_step("4. RETOUR PREDICTIONS", "MLPredictor -> Core", preds)
        log_step("5. DECISION REQUEST", "Core -> DecisionIntelligence", preds)
        
        # Decision step with Cooldown
        cooldown = self._check_cooldown()
        dec = cooldown if cooldown else self.decision_engine.evaluate_classic_decision(measurements, preds, service_vm)
        
        log_step("6. RETOUR DÉCISION", "DecisionIntelligence -> Core", dec)
        self.viz.update_data(measurements)
        
        # Execution
        self._execute_command(dec, "classic")
        return dec

    def run_autonomous_flow(self, measurements: List[Dict]) -> Dict:
        """Executes the 8-step autonomous migration flow with default SLOs and MI scoring."""
        self._print_cycle_header()
        
        # 1. Analyze Metrics (Intelligent selection)
        log_step("1. ANALYZE METRICS", "Core -> MetricsManager", self.current_slos)
        needed, mi_scores = self.metrics_mgr.analyze_needed_metrics(self.current_slos, hub=self, window_seconds=300)
        active = list(set(["latency"] + needed))

        with self._lock:
            self.last_mi_scores = mi_scores
        
        # 2. Collect Metrics
        log_step("2. COLLECTION REQUEST", "MetricsManager -> Collector", needed)
        enriched = [
            {**rm, **self._filter_active_metrics(self.collector.collect_vm_metrics(rm["vm_id"]), active, rm["vm_id"])} 
            for rm in measurements
        ]
        
        # 3. Découverte dynamique du service
        service_vm = self._find_active_vm(enriched)
        if service_vm is None:
            logger.warning("[Core] Aucune VM active détectée dans le payload autonomous.")

        # 4. Store Metrics & Refresh Budgets
        log_step("3. STORE METRICS", "Collector -> Database", enriched)
        self.db.save_metrics(enriched, "autonomous", self.current_slos)
        self._refresh_slo_budgets()
        
        # 5. ML Prediction
        log_step("4. ML PREDICTION", "Core -> MLPredictor", active)
        preds_map = {
            e["vm_id"]: {
                m: self.ml.get_enhanced_prediction(m, e.get("rtt_ms" if m == "latency" else m, 0)) 
                for m in active
            } for e in enriched
        }
        self.viz.update_predictions(preds_map, mode="enhanced")
        
        log_step("5. RETOUR PREDICTIONS", "MLPredictor -> Core", preds_map)

        # 6. Decision logic
        log_step("6. DECISION REQUEST", "Core -> DecisionIntelligence", self.current_slos)
        cooldown = self._check_cooldown()
        dec = cooldown if cooldown else self.decision_engine.evaluate_enhanced_decision(
            enriched, preds_map, self.current_slos, service_vm
        )
        
        # 7. Update Observability
        log_step("7. RETOUR DÉCISION", "DecisionIntelligence -> Core", dec)
        self.viz.update_data(enriched)
        
        # 8. Execute Command
        self._execute_command(dec, "autonomous")
        return dec

    def run_enhanced_flow(self, rtt_measurements: List[Dict]) -> Dict:
        """Executes the advanced 9-step intention-centric migration flow."""
        self._print_cycle_header()
        
        # 1. Analyze
        log_step("1. ANALYZE SLOs", "Core -> MetricsManager", self.current_slos)
        needed, mi_scores = self.metrics_mgr.analyze_needed_metrics(self.current_slos, hub=self, window_seconds=300)
        active = list(set(["latency"] + [s["metric"] for s in self.current_slos]))

        with self._lock:
            self.last_mi_scores = mi_scores
        
        # 2-3. Collect & Store
        log_step("2. COLLECTION REQUEST", "MetricsManager -> Collector", needed)
        enriched = [{**rm, **self._filter_active_metrics(self.collector.collect_vm_metrics(rm["vm_id"]), active, rm["vm_id"])} for rm in rtt_measurements]
        
        # 3. Découverte dynamique du service
        service_vm = self._find_active_vm(enriched)
        if service_vm is None:
            logger.warning("[Core] Aucune VM active détectée dans le payload enhanced.")

        log_step("3. STORE METRICS", "Collector -> Database", enriched)
        self.db.save_metrics(enriched, "enhanced", self.current_slos)
        self._refresh_slo_budgets()
        
        # 4-6. ML Workflow
        log_step("4. LOAD HISTORY", "Core -> HistoryLoader", active)
        log_step("5. ML PREDICTION REQUEST", "Core -> MLPredictor", active)
        preds_map = {e["vm_id"]: {m: self.ml.get_enhanced_prediction(m, e.get("rtt_ms" if m == "latency" else m, 0)) for m in active} for e in enriched}
        self.viz.update_predictions(preds_map, mode="enhanced")
        
        log_step("6. RETOUR PREDICTIONS", "MLPredictor -> Core", preds_map)
        log_step("7. DECISION REQUEST", "Core -> DecisionIntelligence", self.current_slos)
        
        # Decision step with Cooldown
        cooldown = self._check_cooldown()
        dec = cooldown if cooldown else self.decision_engine.evaluate_enhanced_decision(enriched, preds_map, self.current_slos, service_vm)
        
        log_step("8. RETOUR DÉCISION", "DecisionIntelligence -> Core", dec)
        self.viz.update_data(enriched)
        
        # Execution
        self._execute_command(dec, "enhanced")
        return dec

    def _execute_command(self, dec: Dict, mode: str):
        log_step(f"9. COMMAND" if mode == "enhanced" else "7. COMMAND", "Core -> Master", dec)
        
        # Attacher le budget moyen à la décision pour persistance
        if self.current_slos:
            dec["budget_remaining"] = round(sum(s["budget_remaining"] for s in self.current_slos) / len(self.current_slos), 2)
        else:
            dec["budget_remaining"] = 100.0

        ack = self._send_to_master(dec, mode)
        self.db.save_decision(dec, mode, ack)
        
        if dec["decision"] == "stay":
            self.viz.update_decision(dec)
        
        self._log_final(dec)

    def _send_to_master(self, dec: Dict, mode: str) -> bool:
        if dec["decision"] == "stay": return True
        
        from_vm = dec.get("from_vm")
        to_vm = dec.get("to_vm")

        # 1. Deactivate source VM
        if from_vm:
            try:
                url = f"http://localhost:{self.config.VM_PORTS[from_vm]}/deactivate"
                resp = requests.post(url, timeout=2.0)
                if resp.status_code == 200:
                    logger.info(f"[Core] Source {from_vm} désactivée avec succès.")
                else:
                    logger.warning(f"[Core] Échec désactivation source {from_vm} (status {resp.status_code}).")
            except Exception as e:
                logger.error(f"[Core] Erreur lors de la désactivation de {from_vm}: {e}")

        # 2. Activate target VM
        if to_vm:
            try:
                url = f"http://localhost:{self.config.VM_PORTS[to_vm]}/activate"
                resp = requests.post(url, timeout=2.0)
                if resp.status_code == 200:
                    logger.info(f"[Core] Cible {to_vm} activée avec succès.")
                    with self._lock:
                        self._last_known_active_vm = to_vm
                else:
                    logger.warning(f"[Core] Échec activation cible {to_vm} (status {resp.status_code}).")
            except Exception as e:
                logger.error(f"[Core] Erreur lors de la activation de {to_vm}: {e}")

        # 3. Notify Master Cloud
        with self._lock:
            self.last_migration_ts = time.time()
        
        self.viz.update_decision(dec)

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

    def _health_check(self):
        """Verifies system readiness before startup."""
        print(f"\n{Fore.CYAN}{Style.BRIGHT}🔍 EXÉCUTION DU BILAN DE SANTÉ...{Style.RESET_ALL}")
        results = []
        
        # 1. SQLite Check
        try:
            with sqlite3.connect(self.config.DB_NAME) as conn:
                conn.execute("SELECT 1")
            results.append(f"{Fore.GREEN}✅ SQLite OK")
        except Exception as e:
            logger.critical(f"Impossible d'accéder à SQLite ({self.config.DB_NAME}): {e}")
            sys.exit(1)

        # 2. Ports Check (8000, 8010, 8014)
        ports_to_check = [self.config.CORE_PORT, self.config.LATENCY_PORT, self.config.INTENT_PORT]
        ports_ok = True
        for port in ports_to_check:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                try:
                    s.bind(("0.0.0.0", port))
                except socket.error:
                    logger.critical(f"Port {port} déjà occupé.")
                    ports_ok = False
        if not ports_ok: sys.exit(1)
        results.append(f"{Fore.GREEN}✅ Ports libres")

        # 3. Ollama Check
        try:
            requests.get("http://localhost:11434", timeout=3)
            results.append(f"{Fore.GREEN}✅ Ollama détecté")
        except Exception:
            results.append(f"{Fore.YELLOW}⚠️ Ollama non détecté (mode classic uniquement)")
            logger.warning("Ollama non détecté sur http://localhost:11434")

        # 4. ML APIs Check (5001, 5002, 5003)
        ml_ports = [5001, 5002, 5003]
        for port in ml_ports:
            try:
                requests.get(f"http://localhost:{port}", timeout=2)
                results.append(f"{Fore.GREEN}✅ ML API {port} OK")
            except Exception:
                results.append(f"{Fore.YELLOW}⚠️ ML API {port} non détectée (simulation activée)")
                logger.warning(f"ML API sur le port {port} injoignable.")

        print("\n".join(results))
        print(f"{Fore.CYAN}{'─'*30}\n")

    def start(self):
        """Initializes all services and starts the background simulation loops."""
        self._health_check()
        self.db.init_db()
        self.latency_mgr.start_api()
        self.intent_mgr.start_api()
        
        threading.Thread(target=self.app.run, kwargs={'host': '0.0.0.0', 'port': self.config.CORE_PORT, 'threaded': True}, daemon=True).start()
        threading.Thread(target=self.viz.start_gui, daemon=True).start()
        
        logger.info("Orchestrator Hub-and-Spoke started.")
        while True:
            with self._lock:
                last_ts = self.last_real_data_ts
            if last_ts is None:
                logger.warning("⚠️ En attente des données PiCar/Raspberry...")
            elif time.time() - last_ts > 30:
                elapsed = int(time.time() - last_ts)
                logger.warning(
                    f"⚠️ Aucune donnée reçue depuis {elapsed}s "
                    f"— Vérifier la connexion PiCar/Raspberry"
                )
            time.sleep(self.config.COLLECTION_INTERVAL)

if __name__ == "__main__":
    try:
        OrchestratorCore(Config()).start()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
