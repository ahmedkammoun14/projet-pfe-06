# 🔬 ANALYSE COMPLÈTE DE L'ORCHESTRATEUR

## 1️⃣ TABLEAU DES SPOKES

| Spoke | Port | Rôle | Inputs | Outputs | Dépendances | Fichiers |
|-------|------|------|--------|---------|-------------|----------|
| **LatencyManager** | 8010 | Reçoit RTT du PiCar, route vers flows | POST /rtt (RTT measurements) | decision JSON | OrchestratorCore | orchestrator.py:181-213 |
| **MLPredictor** | 8011 | Prédictions 5 points futurs | Valeur courante métrique | [predictions] floats | APIs externes (5001-5003) | orchestrator.py:834-868 |
| **Collector** | 8012 | Collecte CPU/RAM VMs | vm_id (string) | {vm_id, cpu, ram} | VM simulateurs (8101-8104) | orchestrator.py:814-832 |
| **DecisionIntelligence** | 8013 | Évalue seuils + scoring IA | current_data, predictions, SLOs | {decision, from_vm, to_vm, reason} | Aucune | orchestrator.py:870-1006 |
| **IntentManager** | 8014 | Parse LLM → SLOs | POST /intent (texte) | SLOs normalisés | Ollama (11434) | orchestrator.py:223-694 |
| **Config** | 8015 | Gestion thresholds | None (config static) | thresholds | orchestrator.py Config class | orchestrator.py:135-177 |
| **Observability** | 8016 | Dashboard Matplotlib | measurements, predictions, decisions | GUI visuelle | Matplotlib | orchestrator.py:1207-1288 |
| **Database** | 8020 | SQLite persistance | metrics, decisions | {violations, percentiles} | orchestrator.db | orchestrator.py:1008-1196 |
| **HistoryLoader** | 8021 | Extraction windows historiques | vm_id, metric, size | List[float] values | Database | orchestrator.py:1198-1205 |
| **MetricsManager** | 8022 | MI scoring sélection | SLOs, hub | (metrics, mi_scores) | Database | orchestrator.py:696-812 |

---

## 2️⃣ ANALYSE DÉTAILLÉE DES SPOKES

### **SPOKE 1: LatencyManager (Port 8010)**

#### Fonctionnement Interne
```python
LatencyManagerSpoke:
├─ Flask app avec route POST /rtt
├─ Synchrone (non-blocking accepte connexion)
├─ Parse JSON payload: {"measurements": [...], "timestamp": ..., "source": "picar"}
├─ Valide structure
├─ Route basé sur self.core.mode
└─ Retourne decision + status
```

#### Pipeline Interne
```
1. receive_rtt() → POST /rtt
2. Parse request.json
3. Validate: "measurements" in data
4. Set last_real_data_ts = time.time()
5. Extract measurements = data["measurements"]
6. If mode == "enhanced" → self.core.run_enhanced_flow(measurements)
7. Else → self.core.run_autonomous_flow(measurements)
8. Return {"status": "received", "decision": result}
```

#### Gestion d'État
- Pas d'état persistant local (state-less)
- Utilise state du core via protocol LatencyHandler
- Thread-safe car Flask (threaded=True) + core lock protected

#### Communication Orchestrateur
- Protocol: `LatencyHandler` (lines 115-122)
- Methods exposées du core:
  - `mode` property → read
  - `run_enhanced_flow()` → invoke
  - `run_autonomous_flow()` → invoke
  - `set_last_real_data_ts()` → invoke

#### Gestion Erreurs
```python
receive_rtt():
    if not data or "measurements" not in data:
        return {"error": "Invalid payload"}, 400
    # No try-catch (Flask handles)
```

#### LLM
- Aucun appel LLM direct
- Route vers IntentManager (autre spoke)

---

### **SPOKE 2: MLPredictor (Port 8011)**

#### Fonctionnement Interne
```python
MLPredictorSpoke:
├─ Non-HTTP (internal class)
├─ 2 modes: classic (latency only) + enhanced (multi-metric)
├─ Appelle APIs ML externes
├─ Parse numpy array strings
├─ Fallback simulation local
└─ Timeout: 10s (ML), 90s (Ollama)
```

#### Pipeline Interne
```
get_prediction(current_val):           # CLASSIC MODE
  1. _get_api_prediction(ML_RTT_URL, current_val)
  2. POST URL avec timeout=10s
  3. Parse response JSON
  4. Extract "prediction" field
  5. _parse_numpy_string() → [floats]
  6. Scale by 100 (denormalization)
  7. If fail → local simulation
  8. Return [v1, v2, v3, v4, v5] (5-step predictions)

get_enhanced_prediction(metric, val): # ENHANCED MODE
  1. Select URL basé sur metric
  2. _get_api_prediction() same logic
  3. If fail → metric-specific simulation factor
  4. Return [predictions]
```

#### Gestion d'État
- Pas d'état (stateless)
- Utilise config immutable

#### Communication Orchestrateur
```python
Direct call: self.ml.get_prediction(rtt_ms)
           self.ml.get_enhanced_prediction(metric, value)
```

#### Gestion Erreurs
```python
_get_api_prediction():
    try:
        resp = requests.get(url, params={...}, timeout=10)
        return [v * 100 for v in _parse_numpy_string(raw)]
    except Exception:
        return None

get_prediction() / get_enhanced_prediction():
    pred = _get_api_prediction(...)
    if pred: return pred
    logger.warning(f"[ML] {metric} API unavailable")
    # FALLBACK: return [current_val * (factor ** i) for i in range(1, 6)]
    
Factors:
├─ latency: 1.05
├─ cpu_usage: 1.03
└─ ram_usage: 1.02
```

#### LLM
- Aucun

---

### **SPOKE 3: Collector (Port 8012)**

#### Fonctionnement Interne
```python
CollectorSpoke:
├─ Non-HTTP (internal class)
├─ Collecte CPU/RAM des VMs
├─ Sync requests avec timeout 1.5s
├─ Decorated avec @safe_call
└─ Fallback: random simulated values
```

#### Pipeline Interne
```
collect_vm_metrics(vm_id):
  1. config.VM_PORTS[vm_id] → port
  2. GET http://localhost:{port}/metrics
  3. Timeout: 1.5s
  4. Extract JSON: {cpu_usage, ram_usage}
  5. Return {vm_id, cpu_usage, ram_usage}
  6. On exception:
     └─ Return {vm_id, cpu: random(20,95), ram: random(30,90)}
```

#### Gestion d'État
- Pas d'état

#### Communication Orchestrateur
```python
Direct call: enriched_data = self.collector.collect_vm_metrics(vm_id)
```

#### Gestion Erreurs
```python
@safe_call({}, "CollectorSpoke.collect_vm_metrics")
def collect_vm_metrics(vm_id):
    try:
        port = self.config.VM_PORTS.get(vm_id)
        resp = requests.get(f"http://localhost:{port}/metrics", timeout=1.5)
        data = resp.json()
        return {vm_id, cpu, ram}
    except Exception:
        return {vm_id, cpu: random(20,95), ram: random(30,90)}
```

#### LLM
- Aucun

---

### **SPOKE 4: DecisionIntelligence (Port 8013)**

#### Fonctionnement Interne
```python
DecisionIntelligenceSpoke:
├─ 2 stratégies: classic + enhanced
├─ Classic: threshold-based latency only
├─ Enhanced: weighted multi-criteria + MI + budget
├─ Severity scoring
└─ Budget-aware target selection
```

#### Pipeline Interne - CLASSIC

```
evaluate_classic_decision(current_data, predictions_map, service_vm):
  1. threshold = DEFAULT_LATENCY_THRESHOLD (50ms)
  2. If service_vm is None:
     ├─ Find breached_vm (rtt > threshold OR pred > threshold)
     └─ If breached: select best other vm (min rtt)
  3. Else:
     ├─ Check service_vm RTT vs threshold
     ├─ If breach: migrate to min RTT vm
     └─ Else: stay
  4. Return {decision, from_vm, to_vm, reason}
```

#### Pipeline Interne - ENHANCED

```
evaluate_enhanced_decision(current_data, predictions_map, slos, service_vm):
  1. Build weights dict from SLOs
  2. Phase 1: Collect violations
     ├─ For each vm in eval_data (filtered to service_vm if set)
     │  └─ For each SLO:
     │     ├─ Get current value
     │     ├─ Get predicted weighted mean
     │     ├─ If value > threshold OR pred > threshold:
     │     │  └─ Calculate severity = (val - threshold) / threshold
     │     │  └─ Add violation object
  3. Phase 2: Process worst violation
     ├─ worst = max violations by severity
     ├─ Filter targets (vm ≠ worst_vm)
     ├─ Find valid targets (all SLO respected)
     │  └─ If none: use all targets
     ├─ Score each target via _compute_vm_score()
     └─ Select best (min score)
  4. Return decision
```

#### _compute_vm_score()
```
score = pred_score - budget_bonus

pred_score = Σ(weight[metric] * weighted_mean(predictions[metric]))

budget_bonus = Σ(weight[slo] * (current_val ≤ threshold ? budget_remaining/100 : 0))
             → capped at 1.0
```

#### Gestion d'État
- Pas d'état (stateless)
- Immutable config

#### Communication Orchestrateur
```python
Direct call: dec = self.decision_engine.evaluate_classic_decision(...)
            dec = self.decision_engine.evaluate_enhanced_decision(...)
```

#### Gestion Erreurs
- Pas de try-catch (logique pure)
- Assume valid inputs
- Fallback: stay decision si no targets available

#### LLM
- Aucun

---

### **SPOKE 5: IntentManager (Port 8014)**

#### Fonctionnement Interne
```python
IntentManagerSpoke:
├─ HTTP endpoint POST /intent
├─ Calls LLM (Ollama qwen2.5)
├─ 4-tier fallback cascade
├─ RAG context builder
├─ SLO validation + normalization
└─ Keywords matcher (4 profiles)
```

#### Pipeline Interne

```
receive_intent() @ POST /intent:
  1. Parse request.json
  2. Extract intention text
  3. core.set_user_intent(intention)
  4. Return {status, intent_id, slos}
```

#### query_intent_engine(text) - 4-TIER FALLBACK

```
TIER 1: LLM (Ollama qwen2.5)
  ├─ Build RAG context (via _RAGContextBuilder)
  │  ├─ Fetch recent metrics (5min window)
  │  ├─ Calculate latest VM performance
  │  ├─ Count violations per VM/metric
  │  └─ Get percentiles P10, P25, P30 (1h window)
  ├─ Build system prompt (few-shot examples)
  ├─ POST /api/chat with timeout=90s
  ├─ Parse response JSON array
  └─ If success → enrich + validate + return

TIER 2: Regex Matching
  ├─ Patterns: r"latency\s*<\s*(\d+)", etc.
  ├─ Extract thresholds
  ├─ Distribute weights equally
  ├─ If success → enrich + validate + return

TIER 3: Keywords Matcher
  ├─ 4 profiles:
  │  ├─ ux_sensitive: {latency:0.7, cpu:0.3}
  │  ├─ resource_heavy: {cpu:0.5, ram:0.5}
  │  ├─ edge_critical: {latency:1.0}
  │  └─ stability: {latency:0.34, cpu:0.33, ram:0.33}
  ├─ Detect profile (keyword count)
  ├─ Fetch percentiles via hub
  ├─ Build SLOs from profile

TIER 4: Absolute Fallback
  └─ Return defaults: latency<50, cpu<75, ram<80

VALIDATION (Always):
  ├─ _SLOCoherenceValidator.validate()
  ├─ Check physical bounds
  ├─ Auto-normalize weights → sum = 1.0
  ├─ Return corrected_slos
  └─ Log errors + warnings
```

#### LLM Details

**Model**: Ollama qwen2.5 @ localhost:11434

**System Prompt** (from _build_system_prompt):
```
"Tu es un expert SLO pour systèmes distribués.
Tu DOIS répondre UNIQUEMENT avec un JSON array.
Aucun texte avant ou après le JSON.

Chaque objet contient:
{
  "metric": "latency"|"cpu_usage"|"ram_usage",
  "operator": "<"|"<="|">"|">=",
  "threshold": <float>,
  "unit": "ms"|"%",
  "weight": <float 0-1>
}

Somme des weights DOIT être 1.0.

Règle d'inférence:
Si pas de chiffres:
  → Utilise P25 du contexte comme seuils
  → Si P25 indisponible: utilise valeurs actuelles - 20%
  → Priorise par vocabulaire:
     "rapide/réactif/délai" → latency prioritaire
     "charge/CPU" → cpu_usage prioritaire
     "mémoire/RAM" → ram_usage prioritaire
     général → toutes métriques

[Few-shot examples inclus]"
```

**Context Injection** (RAGContextBuilder):
```
1. Précédents SLOs actifs
2. Performance actuelle des VMs (derniers points)
3. Violations récentes (5 min)
4. Seuils historiquement bons (P10, P25, P30 sur 1h)
```

#### Gestion d'État
- Stores self.slos_ref (reference to core.current_slos)
- Updates via core.current_slos
- Thread-safe via core._lock

#### Communication Orchestrateur
- Protocol: `IntentHandler`
- Methods:
  - `set_user_intent(text)` → invoke query_intent_engine
  - `get_recent_context(window)` → return db data
  - `get_metric_percentile()` → return db data
  - `current_slos` → read property
  - `get_metrics_for_mi()` → return db data

#### Gestion Erreurs
```python
query_intent_engine(text):
    try:
        # Ollama call
    except Exception as e:
        logger.warning(f"[Intent] LLM Error: {e}")
        success = False
        # Fall through to regex
    
    if not success:
        # Tier 2: regex
    if not regex_success:
        # Tier 3: keywords
    
    # Always validate
    validator = _SLOCoherenceValidator()
    result = validator.validate(raw_slos)
    for err in result.errors:
        logger.warning(f"[Intent] Validation error: {err}")
    return result.corrected_slos, result.is_valid
```

#### LLM Prompts
- **System Prompt**: Voir _build_system_prompt() (ligne 531-574)
- **Context**: Voir _RAGContextBuilder.build_context() (ligne 304-376)
- **Few-shot examples**: 3 exemples dans système prompt

---

### **SPOKE 6: Config (Port 8015)**

#### Fonctionnement Interne
```python
@dataclass
Config:
├─ Ports (8000-8022)
├─ URLs (ML APIs, Ollama, Master)
├─ VM_LIST, VM_PORTS
├─ Thresholds (DEFAULT_LATENCY, CPU, RAM)
├─ Timings (intervals, cooldown)
└─ Master settings
```

#### Pipeline
- Static configuration (no processing)
- Used by all spokes
- Immutable dataclass

#### Gestion d'État
- Immutable (dataclass)
- Global Config instance

---

### **SPOKE 7: Observability (Port 8016)**

#### Fonctionnement Interne
```python
ObservabilitySpoke:
├─ Matplotlib dashboard (real-time)
├─ 3x4 grid: 3 metrics (RTT, CPU, RAM) x 4 VMs
├─ Plots: actual + predicted + SLO thresholds
├─ Highlights service_vm in green
├─ Shows last decision (STAY/MIGRATE)
├─ Max 50 points buffer (sliding window)
└─ Updates via plt.pause(1)
```

#### Pipeline Interne
```
start_gui():
  1. plt.ion() → interactive mode
  2. Infinite loop:
     ├─ For each (metric, vm):
     │  ├─ ax.clear()
     │  ├─ Plot history[metric][vm] (blue)
     │  ├─ Plot predictions_history[metric][vm] (red dashed)
     │  ├─ Plot SLO thresholds (orange)
     │  └─ Highlight if service_vm (green)
     ├─ Set title + legend
     ├─ Display last_decision
     └─ plt.pause(1)
```

#### Gestion d'État
```python
self.history           # {metric: {vm: [50 max]}}
self.predictions_history
self.current_slos
self.current_service_vm
self.last_decision
self.max_points = 50   # Sliding window
```

#### Communication Orchestrateur
```python
Direct calls:
├─ viz.update_data(measurements)
├─ viz.update_predictions(preds_map, mode)
├─ viz.update_decision(dec, current_vm)
├─ viz.current_slos = slos (direct assign)
```

#### Gestion Erreurs
- Matplotlib non-blocking
- Errors don't crash loop

---

### **SPOKE 8: Database (Port 8020)**

#### Fonctionnement Interne
```python
DatabaseSpoke:
├─ SQLite3 synchrone
├─ 2 tables: metrics, decisions
├─ Thread-safe via sqlite3 connection pooling
├─ Soft migrations (ALTER TABLE)
└─ Query builders dynamic
```

#### Pipeline Interne
```
init_db():
  1. CREATE TABLE metrics (if not exists)
     ├─ id, vm_id, rtt_ms, cpu_usage, ram_usage
     ├─ mode (classic/autonomous/enhanced)
     ├─ is_violation (INTEGER 0/1)
     └─ timestamp (ISO 8601)
  2. CREATE TABLE decisions (if not exists)
     ├─ id, decision, from_vm, to_vm, reason
     ├─ mode, master_ack, budget_remaining
     └─ timestamp
  3. Soft ALTER (try/except)

save_metrics(measurements, mode, slos):
  1. For each measurement:
     ├─ Check violations vs active SLOs
     ├─ is_violation = 1 if any slo breached
     └─ INSERT INTO metrics
  2. Commit

save_decision(dec, mode, ack):
  1. INSERT INTO decisions
  2. Commit

get_recent_metrics(window_seconds):
  1. Query: WHERE timestamp > (now - window)
  2. ORDER BY timestamp DESC
  3. Return List[Dict]

get_historical_percentiles(metric, percentile, window):
  1. Query: SELECT metric_col WHERE timestamp > cutoff
  2. values.sort()
  3. Linear interpolation for percentile
  4. Return float or None

get_metrics_with_violations(window):
  1. Query: SELECT * WHERE timestamp > cutoff
  2. Return with is_violation flag (for MI)

get_slo_violations(metric, threshold, operator, window):
  1. Query: COUNT WHERE metric op threshold AND timestamp > cutoff
  2. Return count

get_window_count(window):
  1. Query: COUNT(*) WHERE timestamp > cutoff
  2. Return count
```

#### Gestion d'État
- SQLite persistent store
- Auto-created at startup

#### Communication Orchestrateur
```python
Direct calls:
├─ db.save_metrics(measurements, mode, slos)
├─ db.save_decision(dec, mode, ack)
├─ db.get_recent_metrics(window)
├─ db.get_historical_percentiles(metric, pct, window)
├─ db.get_metrics_with_violations(window)
├─ db.get_slo_violations(metric, threshold, op, window)
└─ db.get_window_count(window)
```

#### Gestion Erreurs
```python
@safe_call(None / False / 0, context_name)
def method():
    try:
        # SQL operations
    except sqlite3.OperationalError as e:
        logger.error(f"[Database] Error: {e}")
        return default
```

---

### **SPOKE 9: HistoryLoader (Port 8021)**

#### Fonctionnement Interne
```python
HistoryLoaderSpoke:
├─ Non-HTTP (internal)
├─ Loads metric windows from DB
├─ Returns List[float] for ML
└─ Currently minimal usage
```

#### Pipeline Interne
```
load_window(vm_id, metric, size):
  1. col = "rtt_ms" if metric == "latency" else metric
  2. Query: SELECT col FROM metrics
           WHERE vm_id = ? 
           ORDER BY id DESC 
           LIMIT size
  3. Return [values]
```

#### Gestion d'État
- Stateless

---

### **SPOKE 10: MetricsManager (Port 8022)**

#### Fonctionnement Interne
```python
MetricsManagerSpoke:
├─ Intelligent metric selection via MI
├─ Mutual Information scoring
├─ Static fallback (SLO metrics)
├─ Minimum 5 points required
└─ Threshold: 0.05 MI score
```

#### Pipeline Interne
```
analyze_needed_metrics(slos, hub, window):
  1. Static metrics: extract SLO metrics (cpu, ram exclude latency)
  2. If hub is None:
     └─ Return static_metrics only
  3. Fetch data = hub.get_metrics_for_mi(window)
  4. If len(data) < MIN_POINTS (5):
     └─ Return static_metrics
  5. Compute MI scores via compute_mi_scores()
  6. Select metrics with score >= MI_THRESHOLD (0.05)
  7. Return union(static_metrics, intelligent_metrics)

compute_mi_scores(data) → Dict[metric, float]:
  1. For each metric (latency, cpu, ram):
     ├─ Extract x_vals (metric values)
     ├─ Extract y_vals (is_violation binary)
     ├─ If < MIN_POINTS: score = 0.0
     └─ Else: score = _compute_mi(x, y)

_compute_mi(x_vals, y_vals):
  1. Discretize X by median:
     ├─ median = sorted_x[n//2]
     └─ x_bins = [1 if x > median else 0 for x]
  2. Build 2x2 contingency table:
     ├─ counts[(x_bin, y_bin)] for each pair
  3. Calculate probabilities:
     ├─ px[0], px[1] → marginal P(x)
     ├─ py[0], py[1] → marginal P(y)
     ├─ pxy[(x,y)] → joint P(x,y)
  4. MI = Σ P(x,y) * log2(P(x,y) / (P(x)*P(y)))
  5. Normalize by max entropy:
     ├─ H(X) = Shannon entropy of px
     ├─ H(Y) = Shannon entropy of py
     └─ MI_norm = MI / max(H(X), H(Y))
  6. Return MI_norm ∈ [0, 1]
```

#### Gestion d'État
- Stateless
- Immutable constants (MI_THRESHOLD, MIN_POINTS)

#### Communication Orchestrateur
```python
Direct call: needed_metrics, mi_scores = self.metrics_mgr.analyze_needed_metrics(...)
```

---

## 3️⃣ TABLEAU COMPOSANTS INTERNES PAR SPOKE

| Spoke | Composant Interne | Type | Rôle | Interactions |
|-------|-------------------|------|------|--------------|
| **IntentManager** | _RAGContextBuilder | Class | Fetch context historique + percentiles | → hub.get_recent_context(), hub.get_metric_percentile() |
| **IntentManager** | _SLOCoherenceValidator | Class | Valide bounds physiques + normalise poids | → check_physical_bounds(), check_weight_sum() |
| **IntentManager** | _KeywordsMatcher | Class | Détecte profils sémantiques | → detect_profile(), build_slos() |
| **DecisionIntelligence** | _compute_vm_score() | Method | Score cible migration (pred + budget) | Used in evaluate_enhanced_decision |
| **Collector** | collect_vm_metrics() | Method Decorated | @safe_call wrapper | Timeout + fallback simulated |
| **MLPredictor** | _parse_numpy_string() | Method | Parse numpy array string → floats | Used in _get_api_prediction |
| **MLPredictor** | _get_api_prediction() | Method | Call ML API avec timeout | Used in get_prediction/get_enhanced_prediction |
| **Database** | SQL Query Builders | Dynamic SQL | Dynamic query construction | Used in all get_* methods |
| **MetricsManager** | compute_mi_scores() | Method | Calculate MI per metric | Used in analyze_needed_metrics |
| **MetricsManager** | _compute_mi() | Method | Core MI calculation | Used in compute_mi_scores |
| **MetricsManager** | _entropy() | Method | Shannon entropy calculation | Used in _compute_mi |

---

## 4️⃣ PIPELINE MODE AUTONOMOUS (8 ÉTAPES)

```
┌─────────────────────────────────────────────────────────────┐
│                   INPUT LAYER                               │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  POST /rtt ← PiCar (JSON)                                  │
│  {                                                            │
│    "timestamp": "2024-05-28T10:00:00Z",                    │
│    "source": "picar",                                       │
│    "measurements": [                                         │
│      {"vm_id": "vm1", "rtt_ms": 15.3},                    │
│      {"vm_id": "vm2", "rtt_ms": 55.2},                    │
│      {"vm_id": "vm3", "rtt_ms": 8.9},                     │
│      {"vm_id": "vm4", "rtt_ms": 28.5}                     │
│    ]                                                          │
│  }                                                            │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                VALIDATION LAYER                              │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  LatencyManagerSpoke.receive_rtt()                          │
│  ├─ Validate JSON structure                                │
│  │  └─ If invalid: return 400 {"error": "Invalid payload"}│
│  ├─ Validate "measurements" field exists                   │
│  │  └─ If missing: return 400                              │
│  ├─ Extract measurements = data["measurements"]            │
│  └─ Set core.last_real_data_ts = time.time()             │
│     └─ Timestamp for staleness detection                   │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│        ROUTING & INITIALIZATION LAYER                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Route to run_autonomous_flow(measurements)                │
│  ├─ Thread-safe lock acquisition                           │
│  ├─ If service_vm is None:                                 │
│  │  ├─ Find vm with min RTT                                │
│  │  ├─ Set self.service_vm = vm_id                         │
│  │  └─ Log: "Service initialized on {vm} (best RTT)"      │
│  └─ Proceed to analysis                                     │
│                                                               │
│  Output State:                                               │
│  ├─ self.service_vm ≠ None (or first vm if None)          │
│  └─ self.mode = "autonomous"                               │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│         STEP 1: ANALYZE METRICS LAYER                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  MetricsManager.analyze_needed_metrics()                    │
│  ├─ Input: self.current_slos, hub=self, window=300s       │
│  │                                                            │
│  │  Current SLOs default:                                   │
│  │  [                                                         │
│  │    {metric: "latency", threshold: 50, weight: 0.34},   │
│  │    {metric: "cpu_usage", threshold: 75, weight: 0.33}, │
│  │    {metric: "ram_usage", threshold: 80, weight: 0.33}  │
│  │  ]                                                         │
│  │                                                            │
│  ├─ Step 1a: Extract SLO metrics (cpu, ram)               │
│  │  └─ static_metrics = {"cpu_usage", "ram_usage"}        │
│  │                                                            │
│  ├─ Step 1b: Fetch historical data (5 min window)         │
│  │  └─ data = db.get_metrics_with_violations(300)         │
│  │  └─ Returns List[Dict] with is_violation flag          │
│  │  └─ If < 5 points: return static_metrics only          │
│  │                                                            │
│  ├─ Step 1c: MI Scoring                                    │
│  │  └─ compute_mi_scores(data)                             │
│  │  └─ For each metric (latency, cpu, ram):               │
│  │     ├─ Get x_vals (metric values)                      │
│  │     ├─ Get y_vals (is_violation binary 0/1)            │
│  │     ├─ Discretize x by median → x_bins [0,1]          │
│  │     ├─ Build 2x2 contingency table                     │
│  │     ├─ MI = Σ P(x,y) * log2(P(x,y)/(P(x)*P(y)))       │
│  │     └─ Normalize: MI_norm = MI / max(H(X), H(Y))       │
│  │                                                            │
│  │  Example MI scores:                                      │
│  │  {                                                         │
│  │    "latency": 0.0 (always required),                   │
│  │    "cpu_usage": 0.12 (≥ 0.05 threshold → selected),   │
│  │    "ram_usage": 0.02 (< 0.05 → not selected)          │
│  │  }                                                         │
│  │                                                            │
│  ├─ Step 1d: Select intelligent metrics                   │
│  │  └─ intelligent_metrics = {m | score[m] ≥ 0.05}       │
│  │  └─ Final: union(static, intelligent)                  │
│  │                                                            │
│  └─ Output:                                                  │
│     ├─ needed = ["cpu_usage"] (based on MI)               │
│     ├─ mi_scores = {"latency": 0.0, "cpu": 0.12, ...}   │
│     └─ active = ["latency", "cpu_usage"]                  │
│                                                               │
│  Store in core:                                              │
│  └─ self.last_mi_scores = mi_scores                        │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│     STEP 2-3: COLLECTION & STORAGE LAYER                    │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  For each measurement in measurements:                       │
│  ├─ Step 2: Collector.collect_vm_metrics(vm_id)           │
│  │  ├─ GET http://localhost:{port}/metrics                │
│  │  ├─ Timeout: 1.5s                                       │
│  │  ├─ Parse: {cpu_usage, ram_usage}                      │
│  │  ├─ On success: return {vm_id, cpu, ram}              │
│  │  └─ On fail: return {vm_id, cpu: random(20,95), ...} │
│  │                                                            │
│  │  Collected example:                                       │
│  │  {                                                         │
│  │    "vm_id": "vm1",                                      │
│  │    "cpu_usage": 45.2,                                   │
│  │    "ram_usage": 62.3                                    │
│  │  }                                                         │
│  │                                                            │
│  ├─ Step 3: Enrich measurements                            │
│  │  ├─ enriched = [{...measurement, ...collected}]        │
│  │  └─ Example enriched entry:                             │
│  │     {                                                      │
│  │       "vm_id": "vm1",                                   │
│  │       "rtt_ms": 15.3,                ← from PiCar      │
│  │       "cpu_usage": 45.2,             ← from Collector  │
│  │       "ram_usage": 62.3              ← from Collector  │
│  │     }                                                      │
│  │                                                            │
│  ├─ Step 4: Save to Database                               │
│  │  ├─ db.save_metrics(enriched, "autonomous", SLOs)      │
│  │  ├─ For each entry:                                      │
│  │  │  ├─ Check violations vs SLOs:                        │
│  │  │  │  ├─ latency: 15.3 < 50 → OK                     │
│  │  │  │  ├─ cpu: 45.2 < 75 → OK                          │
│  │  │  │  └─ ram: 62.3 < 80 → OK                          │
│  │  │  ├─ is_violation = 0 (no breach)                     │
│  │  │  └─ INSERT INTO metrics                              │
│  │  │                                                         │
│  │  └─ _refresh_slo_budgets():                             │
│  │     ├─ For each SLO:                                     │
│  │     │  ├─ violations = db.get_slo_violations(metric,    │
│  │     │  │                  threshold, operator, 300s)    │
│  │     │  ├─ total_points = db.get_window_count(300)      │
│  │     │  ├─ budget = 100 * (1 - violations/total)        │
│  │     │  └─ slo["budget_remaining"] = budget             │
│  │     │                                                      │
│  │     │  Example: violations=2, total_points=12          │
│  │     │  budget = 100 * (1 - 2/12) = 83.33%             │
│  │     │                                                      │
│  │     └─ self.viz.current_slos = updated SLOs            │
│  │                                                            │
│  └─ Output State:                                            │
│     ├─ enriched = List[Dict] with RTT + CPU + RAM         │
│     ├─ current_slos[*].budget_remaining updated           │
│     └─ metrics table: new rows inserted                     │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│       STEP 5: ML PREDICTION LAYER                           │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  For each vm in enriched:                                   │
│  ├─ For each metric in active (e.g., ["latency", "cpu"]): │
│  │  └─ MLPredictor.get_enhanced_prediction(metric, value) │
│  │     ├─ Select URL from config:                          │
│  │     │  ├─ latency → http://localhost:5001/predict     │
│  │     │  ├─ cpu → http://localhost:5002/predict         │
│  │     │  └─ ram → http://localhost:5003/predict         │
│  │     │                                                      │
│  │     ├─ _get_api_prediction(url, current_val):           │
│  │     │  ├─ GET {url}?input_data={val/100}              │
│  │     │  ├─ Timeout: 10s                                  │
│  │     │  ├─ On success:                                    │
│  │     │  │  ├─ Parse "prediction" field (numpy string)    │
│  │     │  │  ├─ Extract floats via regex                   │
│  │     │  │  └─ Scale by 100 (denormalization)            │
│  │     │  │  └─ Return [v1*100, v2*100, v3*100, v4*100,  │
│  │     │  │           v5*100]                              │
│  │     │  │                                                   │
│  │     │  │  Example response for latency=15:              │
│  │     │  │  {                                               │
│  │     │  │    "prediction": "[0.017, 0.019, 0.022, 0.025,│
│  │     │  │                   0.028]"                       │
│  │     │  │  }                                               │
│  │     │  │  → Output: [17.0, 19.0, 22.0, 25.0, 28.0]    │
│  │     │  │                                                   │
│  │     │  └─ On fail → local simulation:                    │
│  │     │     ├─ factor = 1.05 (latency)                    │
│  │     │     └─ return [val * (factor ** i)                │
│  │     │             for i in range(1, 6)]                 │
│  │     │     └─ [15*1.05, 15*1.05², ..., 15*1.05⁵]       │
│  │     │     └─ [15.75, 16.54, 17.37, 18.24, 19.14]      │
│  │     │                                                      │
│  │     ├─ Compute weighted mean:                            │
│  │     │  ├─ weights = [5, 4, 3, 2, 1]                    │
│  │     │  ├─ weighted_sum = Σ(val * weight)               │
│  │     │  ├─ total_weight = 15                             │
│  │     │  └─ weighted_mean = weighted_sum / 15             │
│  │     │                                                      │
│  │     │  Example: [17, 19, 22, 25, 28]                   │
│  │     │  weighted = (17*5 + 19*4 + 22*3 + 25*2 + 28*1)/15│
│  │     │          = (85 + 76 + 66 + 50 + 28) / 15         │
│  │     │          = 305 / 15 = 20.33 ms                   │
│  │     │                                                      │
│  │     └─ Return [17, 19, 22, 25, 28]                      │
│  │                                                            │
│  └─ preds_map structure:                                    │
│     {                                                         │
│       "vm1": {                                               │
│         "latency": [17, 19, 22, 25, 28],                  │
│         "cpu_usage": [45.5, 47.1, 48.8, 50.6, 52.5]      │
│       },                                                      │
│       "vm2": {...},                                         │
│       "vm3": {...},                                         │
│       "vm4": {...}                                          │
│     }                                                         │
│                                                               │
│  Update visualization:                                       │
│  └─ viz.update_predictions(preds_map, mode="enhanced")    │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│     STEP 6-7: DECISION LAYER                                │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Step 1: Check Cooldown                                     │
│  ├─ _check_cooldown():                                      │
│  │  ├─ If last_migration_ts exists:                         │
│  │  │  ├─ elapsed = now - last_migration_ts                │
│  │  │  ├─ If elapsed < COOLDOWN_SECONDS (60):              │
│  │  │  │  ├─ If mode="enhanced" AND budget_exhausted:      │
│  │  │  │  │  └─ Return None (override cooldown)            │
│  │  │  │  └─ Else:                                          │
│  │  │  │     └─ Return {"decision": "stay",                │
│  │  │  │               "reason": "Cooldown actif"}         │
│  │  │  └─ Else:                                             │
│  │  │     └─ Return None (cooldown expired)                │
│  │  └─ Return None (no previous migration)                  │
│  │                                                            │
│  │ Cooldown Logic (anti-flapping):                          │
│  │ ├─ Ex: Last migration at 10:00:30                       │
│  │ ├─ Cooldown duration: 60s                               │
│  │ ├─ Current time: 10:00:45                               │
│  │ ├─ Elapsed: 15s < 60s → cooldown active                │
│  │ └─ Decision: stay (bypass evaluate)                     │
│  │                                                            │
│  ├─ If cooldown decision returned: use it                   │
│  └─ Else: proceed to evaluate                               │
│                                                               │
│  Step 2: Evaluate Decision                                  │
│  └─ DecisionIntelligence.evaluate_enhanced_decision()       │
│     ├─ Filter eval_data to service_vm (if set):            │
│     │  └─ eval_data = [enriched entry for service_vm]     │
│     │                                                         │
│     ├─ Phase 1: Collect Violations                          │
│     │  ├─ violations = []                                   │
│     │  ├─ For each entry in eval_data:                     │
│     │  │  ├─ For each SLO:                                  │
│     │  │  │  ├─ metric_name = slo["metric"]                │
│     │  │  │  ├─ threshold = slo["threshold"]               │
│     │  │  │  ├─ current_val = entry[metric_col]            │
│     │  │  │  ├─ preds = preds_map[vm_id][metric]          │
│     │  │  │  ├─ weighted_mean = calculate_weighted_mean()  │
│     │  │  │  │                                               │
│     │  │  │  ├─ If current_val > threshold OR              │
│     │  │  │  │    weighted_mean > threshold:               │
│     │  │  │  │                                               │
│     │  │  │  │  severity = (val - threshold) / threshold    │
│     │  │  │  │  violations.append({                         │
│     │  │  │  │    "vm_id": vm_id,                          │
│     │  │  │  │    "metric": metric,                        │
│     │  │  │  │    "val": current_val,                      │
│     │  │  │  │    "threshold": threshold,                  │
│     │  │  │  │    "severity": severity                     │
│     │  │  │  │  })                                           │
│     │  │  │                                                   │
│     │  │  Example violation:                                │
│     │  │  ├─ service_vm latency: 55ms > threshold 50ms    │
│     │  │  ├─ severity = (55-50)/50 = 0.1                  │
│     │  │  └─ violations = [{vm: "vm1", metric: "latency",  │
│     │  │                   val: 55, threshold: 50,         │
│     │  │                   severity: 0.1}]                 │
│     │  │                                                      │
│     │  └─ If violations.empty: return {"decision": "stay"} │
│     │                                                         │
│     ├─ Phase 2: Process Worst Violation                     │
│     │  ├─ worst = max(violations, key=λ: severity)        │
│     │  ├─ worst_vm = worst["vm_id"]                        │
│     │  │                                                      │
│     │  │ Example:                                            │
│     │  │ ├─ worst = {vm: "vm2", metric: "cpu",             │
│     │  │ │            val: 82, threshold: 75, severity: 0.09}
│     │  │ └─ worst_vm = "vm2"                               │
│     │  │                                                      │
│     │  ├─ Find targets = [e for e if e.vm ≠ worst_vm]     │
│     │  │  └─ targets = ["vm1", "vm3", "vm4"]              │
│     │  │                                                      │
│     │  ├─ Find valid targets (all SLO respected):          │
│     │  │  ├─ valid = [t for t if all SLO satisfied]       │
│     │  │  │ Example check:                                  │
│     │  │  │ ├─ vm1: latency 15 < 50 ✓, cpu 45 < 75 ✓    │
│     │  │  │ ├─ vm3: latency 8 < 50 ✓, cpu 32 < 75 ✓     │
│     │  │  │ └─ vm4: latency 28 < 50 ✓, cpu 58 < 75 ✓    │
│     │  │  │ → valid = [vm1, vm3, vm4]                     │
│     │  │  │                                                   │
│     │  │  └─ If valid.empty: use all targets (fallback)   │
│     │  │                                                      │
│     │  ├─ Score each target:                               │
│     │  │  ├─ best = min(final_pool,                        │
│     │  │  │           key=λt: _compute_vm_score(t, ...))  │
│     │  │  │                                                   │
│     │  │  ├─ _compute_vm_score(target, preds_map, slos,   │
│     │  │  │                    weights):                    │
│     │  │  │  ├─ pred_score = Σ (weight[m] * wmean(preds)) │
│     │  │  │  │                                               │
│     │  │  │  │  Example:                                    │
│     │  │  │  │  ├─ weights = {latency: 0.34, cpu: 0.33}  │
│     │  │  │  │  ├─ preds_vm1 = {latency: [17..28] wmean 20.3,
│     │  │  │  │  │                cpu: [45..52] wmean 48.2}
│     │  │  │  │  ├─ pred_score = 0.34*20.3 + 0.33*48.2    │
│     │  │  │  │  │              = 6.9 + 15.9 = 22.8        │
│     │  │  │  │                                               │
│     │  │  │  ├─ budget_bonus = Σ (weight[slo] *            │
│     │  │  │  │                    (val ≤ threshold ?       │
│     │  │  │  │                     budget/100 : 0))        │
│     │  │  │  │                                               │
│     │  │  │  │  Example:                                    │
│     │  │  │  │  ├─ latency SLO: 15 < 50, budget 95%      │
│     │  │  │  │  │  → bonus += 0.34 * 0.95 = 0.323        │
│     │  │  │  │  ├─ cpu SLO: 45 < 75, budget 88%         │
│     │  │  │  │  │  → bonus += 0.33 * 0.88 = 0.290        │
│     │  │  │  │  └─ budget_bonus = min(0.613, 1.0) = 0.613
│     │  │  │  │                                               │
│     │  │  │  └─ score = pred_score - budget_bonus         │
│     │  │  │          = 22.8 - 0.613 = 22.19              │
│     │  │  │                                                   │
│     │  │  └─ Scores for targets:                            │
│     │  │     ├─ vm1: 22.19 (lowest → selected) ✓          │
│     │  │     ├─ vm3: 18.52                                 │
│     │  │     └─ vm4: 25.63                                 │
│     │  │                                                      │
│     │  ├─ best = vm3 (min score)                           │
│     │  └─ reason = "vm2 cpu 82 > SLO 75 (Severity: 0.09)" │
│     │                                                         │
│     └─ Return decision:                                      │
│        {                                                      │
│          "decision": "migrate",                              │
│          "from_vm": "vm2",                                  │
│          "to_vm": "vm3",                                    │
│          "reason": "vm2 cpu 82 > SLO 75"                 │
│        }                                                      │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│     STEP 8: EXECUTION & STORAGE LAYER                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  _execute_command(dec, "autonomous"):                       │
│  │                                                            │
│  ├─ Calculate avg budget_remaining:                         │
│  │  ├─ budget = avg([slo["budget_remaining"]                │
│  │  │              for slo in current_slos])               │
│  │  ├─ Example: (95 + 88) / 2 = 91.5%                     │
│  │  └─ dec["budget_remaining"] = 91.5                      │
│  │                                                            │
│  ├─ Send to Master:                                         │
│  │  └─ _send_to_master(dec, "autonomous"):                 │
│  │     ├─ If decision == "stay":                            │
│  │     │  └─ return True (no master call)                  │
│  │     │                                                      │
│  │     ├─ Else (migrate):                                   │
│  │     │  ├─ Update state:                                  │
│  │     │  │  ├─ with self._lock:                           │
│  │     │  │  │  ├─ self.last_migration_ts = time.time()   │
│  │     │  │  │  │  └─ Start cooldown (60s)                 │
│  │     │  │  │  └─ self.service_vm = dec["to_vm"]         │
│  │     │  │  │     └─ Track new VM hosting service        │
│  │     │  │  │                                               │
│  │     │  ├─ Call Master Cloud:                             │
│  │     │  │  ├─ URL: https://master-cloud/api/v1/migrate  │
│  │     │  │  ├─ Payload:                                    │
│  │     │  │  │  {                                            │
│  │     │  │  │    "decision": "migrate",                    │
│  │     │  │  │    "from_vm": "vm2",                        │
│  │     │  │  │    "to_vm": "vm3",                          │
│  │     │  │  │    "reason": "...",                         │
│  │     │  │  │    "service": "my_service",                │
│  │     │  │  │    "mode": "autonomous",                    │
│  │     │  │  │    "timestamp": "2024-05-28T10:00:05Z"    │
│  │     │  │  │  }                                            │
│  │     │  │  │                                               │
│  │     │  │  ├─ Timeout: 10s                               │
│  │     │  │  ├─ On success (200): ack = True              │
│  │     │  │  └─ On fail/timeout: ack = False              │
│  │     │  │                                                   │
│  │     │  └─ Visualization update:                          │
│  │     │     └─ viz.update_decision(dec)                   │
│  │     │        ├─ last_decision = dec                     │
│  │     │        └─ Highlight service_vm in green           │
│  │     │                                                      │
│  │     └─ return ack                                         │
│  │                                                            │
│  ├─ Save Decision to DB:                                    │
│  │  └─ db.save_decision(dec, "autonomous", ack):           │
│  │     ├─ INSERT INTO decisions                             │
│  │     │  {                                                   │
│  │     │    decision: "migrate",                             │
│  │     │    from_vm: "vm2",                                │
│  │     │    to_vm: "vm3",                                  │
│  │     │    reason: "...",                                  │
│  │     │    mode: "autonomous",                             │
│  │     │    master_ack: 1 (if success) or 0               │
│  │     │    budget_remaining: 91.5,                        │
│  │     │    timestamp: "2024-05-28T10:00:05Z"             │
│  │     │  }                                                  │
│  │     └─ Persistent record created                         │
│  │                                                            │
│  ├─ Update visualization (if stay):                         │
│  │  └─ viz.update_decision(dec)                            │
│  │                                                            │
│  └─ Log final:                                              │
│     └─ _log_final(dec):                                     │
│        └─ Color-coded output:                               │
│           ├─ If migrate: RED                                │
│           │  "══ DÉCISION : MIGRATE | reason ══"           │
│           └─ If stay: GREEN                                 │
│              "══ DÉCISION : STAY | Nominal ══"             │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│         RESPONSE LAYER                                       │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Return from LatencyManager.receive_rtt():                  │
│  {                                                            │
│    "status": "received",                                    │
│    "decision": {                                             │
│      "decision": "migrate",                                 │
│      "from_vm": "vm2",                                      │
│      "to_vm": "vm3",                                        │
│      "reason": "vm2 cpu 82 > SLO 75",                     │
│      "budget_remaining": 91.5                              │
│    }                                                          │
│  }                                                            │
│                                                               │
│  Response sent to PiCar (HTTP 200)                          │
│                                                               │
│  State Changes:                                              │
│  ├─ self.service_vm = "vm3"                                │
│  ├─ self.last_migration_ts = time.time()                  │
│  ├─ current_slos[*].budget_remaining updated               │
│  ├─ decisions table: new row inserted                       │
│  └─ visualization: updated with new decision               │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

## 5️⃣ ANALYSE DÉTAILLÉE AUTONOMOUS (RÉSUMÉ)

**8 Étapes clés:**

| # | Étape | Input | Output | Validation | Décision |
|---|-------|-------|--------|-----------|----------|
| 1 | ANALYZE METRICS | SLOs, 300s data | active_metrics, mi_scores | MI ≥ 0.05 | Sélect intelligente |
| 2 | COLLECTION REQUEST | vm_id list | {vm_id, cpu, ram} | timeout 1.5s | Fallback: simulated |
| 3 | STORE METRICS | enriched data | violations count | SLO thresholds | Save + refresh budgets |
| 4 | LOAD HISTORY | [unused in autonomous] | N/A | N/A | Placeholder step |
| 5 | ML PREDICTION | current_val, metric | [5 predictions] | timeout 10s | Fallback: simulation |
| 6 | DECISION REQUEST | enriched + preds | {decision, from_vm, to_vm, reason} | Cooldown check | Weighted scoring |
| 7 | UPDATE OBSERVABILITY | enriched data | Dashboard refresh | None | Visual feedback |
| 8 | EXECUTE COMMAND | decision | HTTP response | Master ack | Save decision record |

---

**Transformations données:**

```
[PiCar RTT] 
  ↓ (Collector enriches)
[RTT + CPU + RAM]
  ↓ (ML predicts)
[Current + Predictions]
  ↓ (Decision engine)
[Migration decision]
  ↓ (Master cloud)
[Service relocated]
```

---

**Gestion mémoire autonomous:**
- Input buffer: unbounded (PiCar rate-limited to 5s)
- History buffer: 50 points sliding window
- MI calculation: requires min 5 points (300s data)
- Budget tracking: in-memory with DB persistence

---

**Retry/Fallback Autonomous:**

| Composant | Fail Condition | Fallback |
|-----------|-----------------|----------|
| Collector | VM unreachable (timeout 1.5s) | Simulate random CPU/RAM |
| ML API | Timeout 10s or parse error | Local simulation (exponential growth) |
| Master Cloud | Timeout 10s or HTTP error | Log + continue (ack=False) |
| LLM | N/A in autonomous | N/A |

---

## 6️⃣ PIPELINE MODE ENHANCED (TABLEAU COMPARATIF)

**ENHANCED vs AUTONOMOUS:**

| Aspect | AUTONOMOUS | ENHANCED |
|--------|------------|----------|
| **SLOs** | Defaults (50, 75, 80) | User-defined via LLM intent |
| **Metric Selection** | MI scoring only | MI + SLO requirements |
| **Contexte** | Minimal | Full RAG (historical + percentiles) |
| **LLM** | Non | Oui (Ollama qwen2.5) |
| **Fallback Intent** | N/A | 4-tier (LLM→Regex→Keywords→Default) |
| **Decision Logic** | Severity prioritization | Severity + weighted multi-criteria |
| **Budget Override** | Non | Oui (exhaust budget → override cooldown) |
| **Mode Switch** | Manually via config | Automatically via POST /intent |
| **SLO Weights** | Fixed (0.34, 0.33, 0.33) | Extracted + normalized via LLM |

---

**ENHANCED PIPELINE (9 étapes):**

```
[PiCar RTT] →

[1. Analyze SLOs] (MI + user intent)
↓
[2. Collect CPU/RAM]
↓
[3. Store + Budget]
↓
[4. Load History] (HistoryLoader)
↓
[5. ML Predict] (multi-metric)
↓
[6. Decision] (severity + scoring)
↓
[7. Retour Décision]
↓
[8. Update Viz]
↓
[9. Execute] (Master + save)
```

**Key Differences:**
- **Step 1** includes RAG context building
- **Step 4** explicitly loads history (for LLM context prep)
- **SLOs** user-defined + validated + normalized
- **Cooldown override** enabled if budget exhausted
- **Prompt context** injected into LLM

---

**LLM Prompt Context Example (RAG):**

```
Derniers SLOs actifs:
- latency < 50ms (weight: 0.34)
- cpu_usage < 75% (weight: 0.33)
- ram_usage < 80% (weight: 0.33)

Performance actuelle des VMs:
vm1 → RTT: 15.3ms | CPU: 45.2% | RAM: 62.3%
vm2 → RTT: 55.2ms | CPU: 82.1% | RAM: 78.5%
vm3 → RTT: 8.9ms | CPU: 32.0% | RAM: 48.7%
vm4 → RTT: 28.5ms | CPU: 58.3% | RAM: 65.2%

Violations récentes (5 dernières min):
vm2: 4 violation(s) latency, 3 violation(s) cpu_usage
vm4: 1 violation(s) latency

Seuils historiquement bons (sur 1h):
- latency: P10=8.2ms | P25=14.5ms | P30=18.3ms
- cpu_usage: P10=30.1% | P25=42.3% | P30=48.9%
- ram_usage: P10=45.2% | P25=56.7% | P30=62.4%
```

---

**Enhanced Decision Example:**

```
Input: "Je veux éviter les ralentissements"
  ↓ (LLM with context)
LLM Response:
[
  {
    "metric": "latency",
    "operator": "<",
    "threshold": 14.5,  ← P25 from context
    "unit": "ms",
    "weight": 0.7
  },
  {
    "metric": "cpu_usage",
    "operator": "<",
    "threshold": 42.3,  ← P25 from context
    "unit": "%",
    "weight": 0.3
  }
]
  ↓ (Validation + weight normalization)
Corrected SLOs: weights sum = 1.0 ✓
  ↓ (Mode switch to enhanced)
self.mode = "enhanced"
self.current_slos = corrected_slos
```

---

## 7️⃣ DIAGRAMME ENHANCED PIPELINE

```
┌─────────────────────────────────────────────────────────────┐
│                        INPUT LAYER                          │
├─────────────────────────────────────────────────────────────┤
│ POST /rtt (PiCar measurements)                             │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                   ENHANCEMENT LAYER                         │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  A. User POST /intent (optional, switches to enhanced)      │
│  └─ IntentManager.receive_intent()                          │
│     ├─ set_user_intent(text)                                │
│     ├─ query_intent_engine(text)                            │
│     │  ├─ Tier 1: LLM (Ollama)                              │
│     │  ├─ Tier 2: Regex                                     │
│     │  ├─ Tier 3: Keywords                                  │
│     │  └─ Tier 4: Fallback                                  │
│     ├─ Validate SLOs (_SLOCoherenceValidator)              │
│     └─ self.mode = "enhanced" + update current_slos        │
│                                                               │
│  B. LatencyManager routes to run_enhanced_flow()            │
│  └─ (mode was previously set to "enhanced" by /intent)     │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                    CONTEXT LAYER                            │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  RAGContextBuilder.build_context():                         │
│  ├─ Fetch recent metrics (5 min window) via hub             │
│  ├─ Calculate latest VM performance                         │
│  ├─ Count violations per SLO                                │
│  ├─ Get percentiles (P10, P25, P30) via hub                │
│  └─ Format into textual context → inject into LLM prompt   │
│                                                               │
│  Context content:                                            │
│  ├─ Previous SLOs                                           │
│  ├─ Current VM performance                                  │
│  ├─ Recent violations                                       │
│  └─ Historical percentiles (1h window)                      │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                  PLANNING LAYER                             │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Step 1-3 (Same as autonomous):                             │
│  ├─ Analyze metrics → MI scores                             │
│  ├─ Collector → CPU/RAM                                     │
│  ├─ Store → DB + refresh budgets                            │
│  └─ Output: enriched data, active_metrics, mi_scores       │
│                                                               │
│  Step 4 (Enhanced addition):                                │
│  └─ Load History (HistoryLoader)                            │
│     └─ Fetch metric window (used for context if needed)    │
│                                                               │
│  Step 5:                                                     │
│  └─ ML Predictions (same as autonomous)                     │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                  EXECUTION LAYER                            │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Step 6: Decision Logic (Enhanced version)                  │
│  └─ _check_cooldown() with budget exhaustion override       │
│     └─ If enhanced mode + budget=0: override cooldown       │
│                                                               │
│  └─ evaluate_enhanced_decision():                           │
│     ├─ Input: enriched (with user SLOs), preds, service_vm │
│     ├─ Phase 1: Collect violations (severity scoring)       │
│     ├─ Phase 2: Process worst violation                     │
│     │  ├─ Find valid targets (respecting user SLOs)        │
│     │  ├─ Score via _compute_vm_score():                    │
│     │  │  ├─ pred_score = Σ(weight[m] * wmean(preds))     │
│     │  │  └─ budget_bonus = Σ(weight * budget/100)        │
│     │  └─ Select best target (min score)                    │
│     └─ Output: {decision, from_vm, to_vm, reason}         │
│                                                               │
│  Key differences from autonomous:                            │
│  ├─ SLOs are user-defined (from LLM)                        │
│  ├─ Weights reflect user intent (0.7 latency + 0.3 cpu)   │
│  ├─ Budget exhaustion override enabled                      │
│  └─ Severity + weighted scoring (not just threshold)        │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────────┐
│               RESPONSE LAYER                                │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  Step 7-9 (Same as autonomous):                             │
│  ├─ Update visualization                                    │
│  ├─ Execute command (Master + DB save)                      │
│  └─ Log decision                                             │
│                                                               │
│  Final response to PiCar:                                    │
│  {                                                            │
│    "status": "received",                                    │
│    "decision": {decision_object}                            │
│  }                                                            │
│                                                               │
└─────────────────────────────────────────────────────────────┘
```

---

**(Continuer à la prochaine section...)**

---

## 8️⃣ DIFFÉRENCES EXACTES: ENHANCED vs AUTONOMOUS

### **Tableau Comparatif Détaillé**

| Critère | AUTONOMOUS | ENHANCED |
|---------|-----------|----------|
| **SLOs** | Hardcoded: latency<50, cpu<75, ram<80 | User intent → LLM extracted |
| **Weights** | Fixed: 0.34, 0.33, 0.33 | LLM determined + normalized |
| **Mode** | Default startup | Activated by POST /intent |
| **Context** | None | RAG (recent data + percentiles + violations) |
| **LLM** | No | Yes (Ollama qwen2.5, timeout 90s) |
| **Fallback LLM** | N/A | 4-tier (LLM→Regex→Keywords→Default) |
| **Metric Selection** | MI scoring (0.05 threshold) | MI + SLO requirements |
| **Decision Logic** | Severity prioritization | Severity + weighted multi-criteria + budget |
| **Budget Override** | No | Yes (cooldown override if budget=0) |
| **History Load** | Skipped (step 4) | Explicitly loaded (step 4) |
| **Target Selection** | Min RTT (classic) or scored (enhanced) | Weighted scoring with budget bonus |
| **Validation** | No (uses defaults) | Yes (_SLOCoherenceValidator) |
| **SLO Normalization** | N/A (fixed weights) | Auto: weights sum = 1.0 |

### **Transition Mode Lifecycle**

```
STARTUP: mode = "autonomous"
         current_slos = defaults

User sends: POST /intent {"intention": "Keep latency < 30ms"}
            ↓
IntentManager.receive_intent()
  ├─ set_user_intent(text)
  ├─ query_intent_engine(text) with RAG context
  │  └─ LLM: extract SLOs from intent
  ├─ Validate SLOs (bounds + weights)
  ├─ Normalize weights: sum = 1.0
  └─ Update core state:
     └─ with self._lock:
        ├─ self.current_slos = validated_slos
        ├─ self.mode = "enhanced"
        ├─ self.viz.current_slos = slos
            └─ Dashboard shows new SLOs

NEXT /rtt from PiCar:
  └─ Route to run_enhanced_flow() (mode is "enhanced")

RESULT:
  ├─ Decisions use user SLOs + weights
  ├─ Budget exhaustion can override cooldown
  └─ Context-aware intent interpretation
```

### **Example: Behavioral Difference**

**Scenario:** vm2 latency spike to 65ms

**AUTONOMOUS (defaults):**
```
SLO: latency < 50ms, weight 0.34
Decision:
├─ Breach detected (65 > 50)
├─ Severity = (65-50)/50 = 0.3
├─ Migrate from vm2 to best alternative
└─ Reason: "vm2 latency 65ms > 50ms"
```

**ENHANCED (after "Keep latency < 30ms"):**
```
SLO: latency < 30ms, weight 0.7 (user intent weighted higher)
Decision:
├─ Breach detected (65 > 30)
├─ Severity = (65-30)/30 = 1.17 (much worse!)
├─ Prioritize latency fix (higher weight)
├─ Consider CPU/RAM less (lower weights)
└─ Reason: "vm2 latency 65ms > SLO 30 (Severity: 1.17)"

PLUS: If budget exhausted, override 60s cooldown
```

---

## 9️⃣ FLUX GLOBAL DES DONNÉES

```
┌──────────────────────────────────────────────────────────────┐
│                    ENTRY POINT                                │
├──────────────────────────────────────────────────────────────┤
│                                                                │
│  1. External Systems:                                         │
│  ├─ PiCar → HTTP POST /rtt:8010                              │
│  │  Payload: {timestamp, source: "picar",                    │
│  │            measurements: [{vm_id, rtt_ms}, ...]}          │
│  │                                                              │
│  └─ User → HTTP POST /intent:8014                            │
│     Payload: {intent_id, intention: "natural language"}      │
│                                                                │
│  2. Simulators → Polled by Collector:                        │
│  ├─ VM simulators:8101-8104                                  │
│  │  GET /health → {rtt_ms}                                   │
│  │  GET /metrics → {cpu_usage, ram_usage}                    │
│  │                                                              │
│  └─ ML APIs:5001-5003                                        │
│     GET /predict → {prediction: "[0.017, ...]"}             │
│                                                                │
│  3. External Services → Accessed:                            │
│  ├─ Ollama:11434                                             │
│  │  POST /api/chat → {message.content: JSON array}          │
│  │                                                              │
│  └─ Master Cloud                                             │
│     POST /api/v1/migrate → {status: 200}                    │
│                                                                │
└──────────────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────────────┐
│                  INPUT PROCESSOR                              │
├──────────────────────────────────────────────────────────────┤
│                                                                │
│  LatencyManager.receive_rtt()                                │
│  ├─ Parse JSON                                               │
│  ├─ Validate structure                                       │
│  ├─ Extract: measurements list                              │
│  │  {                                                          │
│  │    "vm1": {rtt_ms: 15.3},                                │
│  │    "vm2": {rtt_ms: 55.2},                                │
│  │    "vm3": {rtt_ms: 8.9},                                 │
│  │    "vm4": {rtt_ms: 28.5}                                 │
│  │  }                                                          │
│  │                                                              │
│  └─ Update: last_real_data_ts = now                          │
│                                                                │
└──────────────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────────────┐
│               CONTEXT BUILDER                                 │
├──────────────────────────────────────────────────────────────┤
│                                                                │
│  If mode == "enhanced":                                       │
│  ├─ RAGContextBuilder.build_context()                        │
│  │  ├─ Fetch db.get_recent_metrics(300)                     │
│  │  │  └─ Last 5min of all metrics                          │
│  │  │                                                          │
│  │  ├─ Fetch db.get_historical_percentiles()                │
│  │  │  └─ P10, P25, P30 over 1 hour                         │
│  │  │                                                          │
│  │  └─ Format context string (1500 chars)                   │
│  │     └─ Inject into LLM system prompt                     │
│  │                                                              │
│  └─ Else: skip context (autonomous mode)                     │
│                                                                │
└──────────────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────────────┐
│                DECISION ENGINE                                │
├──────────────────────────────────────────────────────────────┤
│                                                                │
│  1. Metrics Selection:                                        │
│  ├─ MetricsManager.analyze_needed_metrics()                 │
│  │  ├─ MI scoring on historical data                        │
│  │  ├─ Select metrics with score ≥ 0.05                    │
│  │  └─ active_metrics = ["latency", "cpu_usage"]           │
│  │                                                              │
│  └─ Collector.collect_vm_metrics() for active metrics        │
│     └─ Enrich with CPU/RAM from VM simulators               │
│                                                                │
│  2. Storage:                                                  │
│  └─ db.save_metrics(enriched, mode, SLOs)                   │
│     ├─ Calculate is_violation per SLO                        │
│     ├─ INSERT into metrics table                             │
│     └─ _refresh_slo_budgets()                                │
│        └─ Recalculate budget_remaining %                     │
│                                                                │
│  3. Predictions:                                              │
│  └─ MLPredictor.get_enhanced_prediction()                    │
│     ├─ Call ML APIs (5001-5003)                             │
│     ├─ Parse numpy strings → floats                         │
│     ├─ Calculate weighted mean                               │
│     └─ preds_map = {vm: {metric: [5-step preds]}}          │
│                                                                │
│  4. SLO Extraction (enhanced only):                           │
│  └─ IntentManager.query_intent_engine()                      │
│     ├─ 4-tier fallback: LLM→Regex→Keywords→Default         │
│     ├─ Validate bounds + normalize weights                   │
│     └─ current_slos = validated user SLOs                    │
│                                                                │
│  5. Decision:                                                 │
│  └─ DecisionIntelligence.evaluate_*_decision()               │
│     ├─ Check cooldown (with budget override)                 │
│     ├─ Collect violations + severity                         │
│     ├─ Score targets (pred_score - budget_bonus)            │
│     └─ Return: {decision, from_vm, to_vm, reason}           │
│                                                                │
└──────────────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────────────┐
│              EXECUTION ENGINE                                 │
├──────────────────────────────────────────────────────────────┤
│                                                                │
│  1. Update State:                                             │
│  ├─ If migrate:                                              │
│  │  └─ last_migration_ts = now (start cooldown)             │
│  │  └─ service_vm = to_vm (track new host)                 │
│  │                                                              │
│  └─ Else: keep state                                         │
│                                                                │
│  2. Master Cloud:                                             │
│  └─ POST /migrate (if migrate decision)                      │
│     ├─ Payload with decision + metadata                      │
│     ├─ Timeout: 10s                                          │
│     └─ Response: ack (200) or fail                           │
│                                                                │
│  3. Persistence:                                              │
│  ├─ db.save_decision(dec, mode, ack)                        │
│  │  └─ INSERT into decisions table                          │
│  │                                                              │
│  └─ Visualization:                                            │
│     └─ viz.update_decision(dec)                              │
│        └─ Dashboard shows last decision                      │
│                                                                │
└──────────────────────────────────────────────────────────────┘
                         ↓
┌──────────────────────────────────────────────────────────────┐
│              FINAL RESPONSE                                   │
├──────────────────────────────────────────────────────────────┤
│                                                                │
│  HTTP Response to PiCar:                                      │
│  {                                                             │
│    "status": "received",                                     │
│    "decision": {                                              │
│      "decision": "migrate" | "stay",                         │
│      "from_vm": "vm2" | null,                               │
│      "to_vm": "vm3" | null,                                 │
│      "reason": "...",                                        │
│      "budget_remaining": 91.5                               │
│    }                                                           │
│  }                                                             │
│                                                                │
│  Data stored in orchestrator.db:                             │
│  ├─ metrics table (with is_violation, budget)               │
│  └─ decisions table (with ack status)                        │
│                                                                │
│  Dashboard updated:                                           │
│  ├─ Live plots (history + predictions)                       │
│  ├─ Service VM highlighted green                             │
│  └─ Last decision displayed (STAY/MIGRATE)                  │
│                                                                │
└──────────────────────────────────────────────────────────────┘
```

### **Sérialisations & Transformations**

```
Data Format Transformations:

1. INPUT VALIDATION:
   JSON → Python dict (measurements list)

2. ENRICHMENT:
   {vm_id, rtt_ms} + {cpu, ram} → enriched dict

3. STORAGE:
   Python dict → SQL INSERT (metrics table)

4. RETRIEVAL:
   SQL Row → Python dict (violation counting)

5. ML:
   float value → GET params → JSON response → numpy parse
   → List[float] → weighted mean

6. LLM:
   string intent → JSON payload → Ollama → JSON array → 
   pydantic validation → SLO objects

7. DECISION:
   SLO objects + predictions → decision dict

8. PERSISTENCE:
   decision dict → SQL INSERT (decisions table)

9. RESPONSE:
   Python dict → JSON serialization → HTTP response
```

---

## 🔟 INTÉGRATION LLM COMPLÈTE

### **LLM Utilisé**

- **Model**: Ollama qwen2.5 (local deployment)
- **Endpoint**: http://localhost:11434/api/chat
- **Timeout**: 90 seconds
- **Mode**: Synchrone (blocking request)

### **Architecture LLM**

```
OrchestratorCore
  │
  └─ IntentManagerSpoke
      │
      ├─ query_intent_engine(text)
      │  │
      │  └─ TIER 1: LLM Call
      │     ├─ RAGContextBuilder.build_context()
      │     │  ├─ hub.get_recent_context(300s)
      │     │  ├─ hub.get_metric_percentile(P10, P25, P30)
      │     │  └─ Format context string
      │     │
      │     ├─ _build_system_prompt(context)
      │     │  ├─ Expert SLO instructions
      │     │  ├─ Output format (JSON only)
      │     │  ├─ Few-shot examples (3)
      │     │  ├─ Inference rules
      │     │  └─ Context injection
      │     │
      │     ├─ Payload construction:
      │     │  {
      │     │    "model": "qwen2.5",
      │     │    "messages": [
      │     │      {"role": "system", "content": system_prompt},
      │     │      {"role": "user", "content": user_intention}
      │     │    ],
      │     │    "stream": false
      │     │  }
      │     │
      │     ├─ requests.post(OLLAMA_URL, json=payload, timeout=90)
      │     │
      │     └─ Response parsing:
      │        {"message": {"role": "assistant", "content": "[...]"}}
      │        → extract content
      │        → _parse_slos_from_text()
      │
      │  [TIER 2-4 Fallback cascade on LLM fail]
      │
      └─ _SLOCoherenceValidator.validate()
         ├─ Check physical bounds
         ├─ Auto-normalize weights
         └─ Return corrected_slos
```

### **Prompts Détaillés**

#### **System Prompt Template**

```
Tu es un expert SLO pour systèmes distribués.
Tu DOIS répondre UNIQUEMENT avec un JSON array. 
Aucun texte avant ou après le JSON.

Chaque objet JSON contient exactement :
{
  "metric": "latency"|"cpu_usage"|"ram_usage",
  "operator": "<"|"<="|">"|">=",
  "threshold": <float>,
  "unit": "ms"|"%",
  "weight": <float entre 0.0 et 1.0>
}

Somme des weights DOIT être 1.0.

Règle d'inférence :
Si l'intention ne contient pas de chiffres :
  → Utilise les valeurs P25 du contexte comme seuils
  → Si P25 non disponible : utilise les valeurs actuelles des VMs réduites de 20%
  → Priorise les métriques selon le vocabulaire :
    "rapide/réactif/délai" → latency en priorité
    "charge/surcharge/CPU" → cpu_usage en priorité
    "mémoire/RAM" → ram_usage en priorité
    intention générale → toutes métriques

Exemple 1 — Intention abstraite :
Contexte : RTT moyen=38ms, P25 latency=24ms, CPU moyen=58%, P25 cpu=45%
Intention : "Je veux éviter les ralentissements"
Réponse : [{"metric":"latency","operator":"<","threshold":24,"unit":"ms","weight":0.7},
           {"metric":"cpu_usage","operator":"<","threshold":45,"unit":"%","weight":0.3}]

Exemple 2 — Intention numérique :
Contexte : (quelconque)
Intention : "latence < 15ms et CPU < 60%"
Réponse : [{"metric":"latency","operator":"<","threshold":15,"unit":"ms","weight":0.5},
           {"metric":"cpu_usage","operator":"<","threshold":60,"unit":"%","weight":0.5}]

Exemple 3 — Intention mixte :
Contexte : P25 ram=51%
Intention : "CPU < 70% et garde la RAM stable"
Réponse : [{"metric":"cpu_usage","operator":"<","threshold":70,"unit":"%","weight":0.6},
           {"metric":"ram_usage","operator":"<","threshold":51,"unit":"%","weight":0.4}]

[CONTEXT SYSTEM ACTUEL INJECTÉ ICI]
```

#### **Context Injection**

```
Contexte système actuel :

Derniers SLOs actifs :
- latency < 50ms (weight: 0.34)
- cpu_usage < 75% (weight: 0.33)
- ram_usage < 80% (weight: 0.33)

Performance actuelle des VMs :
vm1 -> RTT: 15.1ms | CPU: 45.2% | RAM: 62.3%
vm2 -> RTT: 55.2ms | CPU: 82.1% | RAM: 78.5%
vm3 -> RTT: 8.9ms | CPU: 32.0% | RAM: 48.7%
vm4 -> RTT: 28.5ms | CPU: 58.3% | RAM: 65.2%

Violations récentes (5 dernières min) :
vm2 : 4 violation(s) latency, 3 violation(s) cpu_usage
vm4 : 1 violation(s) latency

Seuils historiquement bons (sur 1h) :
- latency: P10=8.2ms | P25=14.5ms | P30=18.3ms
- cpu_usage: P10=30.1% | P25=42.3% | P30=48.9%
- ram_usage: P10=45.2% | P25=56.7% | P30=62.4%
```

### **Message Flow**

```
User: POST /intent {"intention": "Keep latency below 30ms"}
  ↓
IntentManager receives
  ├─ validate JSON
  ├─ extract "intention" field
  ├─ call set_user_intent(intention)
  │
  └─ query_intent_engine(intention):
      ├─ RAGContextBuilder builds context
      ├─ System prompt with context
      ├─ Payload:
      │  {
      │    "model": "qwen2.5",
      │    "messages": [
      │      {
      │        "role": "system",
      │        "content": "[system_prompt + context]"
      │      },
      │      {
      │        "role": "user",
      │        "content": "Keep latency below 30ms"
      │      }
      │    ],
      │    "stream": false
      │  }
      │
      ├─ requests.post("http://localhost:11434/api/chat", 
      │                json=payload, timeout=90)
      │
      ├─ Response (Ollama):
      │  {
      │    "message": {
      │      "role": "assistant",
      │      "content": "[{\"metric\":\"latency\",\"operator\":\"<\",\"threshold\":30,\"unit\":\"ms\",\"weight\":1.0}]"
      │    },
      │    "done": true
      │  }
      │
      ├─ _parse_slos_from_text(response):
      │  ├─ Find JSON array: [...]
      │  ├─ json.loads() → list
      │  └─ return slos, success=True
      │
      ├─ _SLOCoherenceValidator.validate(slos):
      │  ├─ Check bounds: latency 5-2000ms ✓
      │  ├─ Check weights: sum = 1.0 ✓
      │  └─ return corrected_slos
      │
      └─ Update core state:
         ├─ with self._lock:
         │  ├─ self.current_slos = [{metric: latency, threshold: 30, weight: 1.0}]
         │  └─ self.mode = "enhanced"
         │
         └─ Response HTTP 200:
            {
              "status": "received",
              "intent_id": "...",
              "slos": [
                {
                  "metric": "latency",
                  "operator": "<",
                  "threshold": 30,
                  "unit": "ms",
                  "weight": 1.0
                }
              ]
            }
```

### **Fallback Cascade (Tier 1-4)**

```
TIER 1: LLM (Ollama qwen2.5)
  Input: "Keep latency below 30ms"
  Attempt: POST /api/chat
  Fail Conditions:
  ├─ Network timeout (90s)
  ├─ HTTP error response
  ├─ Empty response body
  ├─ JSON parsing error
  └─ Validation error
  
  On Fail → log warning → next tier

TIER 2: Regex Matching
  Patterns:
  ├─ r"latency\s*<\s*(\d+)" → extract threshold
  ├─ r"cpu\s*<\s*(\d+)" → extract threshold
  └─ r"ram\s*<\s*(\d+)" → extract threshold
  
  Example: "latency < 30 and cpu < 60%"
  ├─ Extract: latency=30, cpu=60
  ├─ Create SLOs:
  │  [{metric: latency, threshold: 30, ...},
  │   {metric: cpu_usage, threshold: 60, ...}]
  ├─ Distribute weights equally
  │  → [0.5, 0.5]
  └─ Validate + return
  
  On No Match → next tier

TIER 3: Keywords Matcher
  4 Semantic Profiles:
  ├─ ux_sensitive
  │  Keywords: "rapide, réactif, délai, lent, streaming, latence"
  │  Metrics: [latency, cpu_usage]
  │  Weights: {latency: 0.7, cpu_usage: 0.3}
  │  Percentile: 25
  │
  ├─ resource_heavy
  │  Keywords: "surcharge, saturation, charge, processeur, cpu"
  │  Metrics: [cpu_usage, ram_usage]
  │  Weights: {cpu: 0.5, ram: 0.5}
  │  Percentile: 25
  │
  ├─ edge_critical
  │  Keywords: "edge, proximité, critiques, temps réel, iot"
  │  Metrics: [latency]
  │  Weights: {latency: 1.0}
  │  Percentile: 10
  │
  └─ stability
     Keywords: "stabilité, continuité, qualité, fiable, robuste"
     Metrics: [latency, cpu_usage, ram_usage]
     Weights: {latency: 0.34, cpu: 0.33, ram: 0.33}
     Percentile: 30
  
  Example: "Je veux une bonne performance"
  ├─ Normalize text: "je veux une bonne performance"
  ├─ Count keywords per profile:
  │  ├─ ux_sensitive: 0
  │  ├─ resource_heavy: 0
  │  ├─ edge_critical: 0
  │  └─ stability: 0 (no keywords match)
  ├─ Best profile: default
  └─ Return default SLOs + fallback flag
  
  On Match → build SLOs from profile + percentiles

TIER 4: Absolute Fallback
  Default SLOs:
  [
    {metric: "latency", operator: "<", threshold: 50, unit: "ms", weight: 0.34},
    {metric: "cpu_usage", operator: "<", threshold: 75, unit: "%", weight: 0.33},
    {metric: "ram_usage", operator: "<", threshold: 80, unit: "%", weight: 0.33}
  ]
  
  Guaranteed to always return valid SLOs
  success = False (indicates fallback was used)

ALWAYS: Validation
  _SLOCoherenceValidator.validate():
  ├─ Check physical bounds
  ├─ Check weight sum ≈ 1.0
  ├─ Auto-normalize weights if needed
  └─ Return corrected_slos + errors + warnings
```

### **Context Management**

```
RAGContextBuilder:
├─ Called at every LLM query
├─ Fetches fresh data:
│  ├─ hub.get_recent_context(300s) → recent metrics
│  ├─ hub.get_metric_percentile() → P10, P25, P30 (1h)
│  └─ Calculate violations in real-time
│
├─ Context persistence:
│  └─ NOT stored → rebuilt every query
│
└─ Purpose:
   └─ Provide up-to-date system state to LLM
      for informed SLO extraction

Memory System:
├─ No explicit memory storage
├─ State stored in:
│  ├─ orchestrator.db (metrics + decisions)
│  ├─ OrchestratorCore (current_slos)
│  ├─ ObservabilitySpoke (history buffers)
│  └─ LatencyManagerSpoke (last_real_data_ts)
│
└─ RAG operates on:
   └─ Database historical data only
      (no external memory embeddings)
```

### **Tool Calling & Function Calling**

- **Tool Calling**: Not used (no OpenAI tools)
- **Function Calling**: Not used (traditional approach)
- **HTTP-based**: Direct REST API calls to Ollama

---

## 1️⃣1️⃣ CONFIGURATION & INFRASTRUCTURE

### **Tableau Services & Configuration**

| Service | Rôle | Port | Config | Dépendances | Health Check |
|---------|------|------|--------|------------|--------------|
| **Core Orchestrator** | Hub central | 8000 | Flask host:0.0.0.0 | OrchestratorCore | GET /status |
| **LatencyManager** | RTT ingestion | 8010 | Flask threaded | PiCar connector | POST /rtt |
| **MLPredictor** | Predictions | (8011) | Config only | External APIs:5001-5003 | Timeout fallback |
| **Collector** | Metrics gather | (8012) | Config only | VM Simulators:8101-8104 | Timeout 1.5s |
| **DecisionIntelligence** | Decision logic | (8013) | Config only | None (pure logic) | Algorithm correctness |
| **IntentManager** | LLM interface | 8014 | Flask threaded | Ollama:11434 | LLM availability |
| **Observability** | Dashboard | (8016) | Matplotlib GUI | Pyplot | Window display |
| **Database** | SQLite persist | (8020) | orchestrator.db | None | Health check @ start |
| **HistoryLoader** | Data retrieval | (8021) | Config only | Database | Query latency |
| **MetricsManager** | MI scoring | (8022) | Config only | Database | Min 5 data points |
| **PiCar Simulator** | RTT reporter | N/A | Async, 5s cycle | VM Simulators | Connectivity check |
| **VM Simulators** | Resource sim | 8101-8104 | FastAPI Uvicorn | Random generators | GET /health + /metrics |
| **ML APIs** | Predictions | 5001-5003 | External | GRU/RNN/LSTM models | Availability check |
| **Ollama LLM** | Text generation | 11434 | Docker container | GPU (optional) | Health check @ start |
| **Master Cloud** | Migration target | (443) | HTTPS | Token auth | Deployment system |

### **Variables Configuration (Config Dataclass)**

```python
Config:
├─ CORE_PORT = 8000
├─ LATENCY_PORT = 8010
├─ ML_PREDICTOR_PORT = 8011
├─ COLLECTOR_PORT = 8012
├─ DECISION_PORT = 8013
├─ INTENT_PORT = 8014
├─ CONFIG_PORT = 8015
├─ OBSERVABILITY_PORT = 8016
├─ DATABASE_PORT = 8020
├─ HISTORY_LOADER_PORT = 8021
├─ METRICS_MANAGER_PORT = 8022
│
├─ INTENT_ENGINE_URL = "http://localhost:11434/api/chat"
├─ ML_RTT_URL = "http://localhost:5001/predict"
├─ ML_CPU_URL = "http://localhost:5002/predict"
├─ ML_RAM_URL = "http://localhost:5003/predict"
│
├─ VM_LIST = ["vm1", "vm2", "vm3", "vm4"]
├─ VM_PORTS = {vm1: 8101, vm2: 8102, vm3: 8103, vm4: 8104}
│
├─ DEFAULT_LATENCY_THRESHOLD = 50.0 (ms)
├─ DEFAULT_CPU_THRESHOLD = 75.0 (%)
├─ DEFAULT_RAM_THRESHOLD = 80.0 (%)
│
├─ COLLECTION_INTERVAL = 5 (seconds)
├─ HISTORY_WINDOW = 10 (unused)
├─ DB_NAME = "orchestrator.db"
│
├─ MASTER_URL = "https://master-cloud/api/v1/migrate"
├─ MASTER_TOKEN = "changeme"
├─ MASTER_TIMEOUT = 10 (seconds)
├─ COOLDOWN_SECONDS = 60
│
└─ (No environment variables used - hardcoded)
```

### **Environment Variables Recommended**

```bash
# Database
ORCHESTRATOR_DB_PATH=orchestrator.db

# Master Cloud
MASTER_URL=https://master-cloud/api/v1/migrate
MASTER_TOKEN=<token>

# LLM
OLLAMA_URL=http://localhost:11434
LLM_MODEL=qwen2.5
LLM_TIMEOUT=90

# ML APIs
ML_RTT_URL=http://localhost:5001/predict
ML_CPU_URL=http://localhost:5002/predict
ML_RAM_URL=http://localhost:5003/predict
ML_TIMEOUT=10

# Logging
LOG_LEVEL=INFO

# Debug
DEBUG=False
```

### **Infrastructure Stack**

```
┌─────────────────────────────────────────────┐
│           ORCHESTRATOR CORE                  │
│  (Python 3.12, Flask, Threading, Sqlite3)  │
└─────────────────────────────────────────────┘
        │                │                │
        ↓                ↓                ↓
┌──────────┐  ┌──────────────┐  ┌─────────────┐
│ Database │  │   Logging    │  │ Visualization
│ SQLite3  │  │  (Colorama)  │  │ (Matplotlib)
│          │  │   (local)    │  │ (local GUI)
└──────────┘  └──────────────┘  └─────────────┘

┌─────────────────────────────────────────────┐
│        EXTERNAL SERVICES                     │
├─────────────────────────────────────────────┤
│                                              │
│  ┌─────────────┐  ┌──────────────────────┐ │
│  │ VM Sims     │  │  ML Prediction APIs  │ │
│  │ FastAPI     │  │  (GRU/RNN/LSTM)      │ │
│  │ :8101-8104  │  │  :5001-5003          │ │
│  └─────────────┘  └──────────────────────┘ │
│                                              │
│  ┌──────────────┐  ┌──────────────────────┐ │
│  │ Ollama LLM   │  │  Master Cloud        │ │
│  │ Docker       │  │  (HTTPS endpoint)    │ │
│  │ :11434       │  │  /api/v1/migrate     │ │
│  └──────────────┘  └──────────────────────┘ │
│                                              │
│  ┌──────────────────────────────────────┐   │
│  │      PiCar Simulator                 │   │
│  │  (async client, 5s polling cycle)    │   │
│  │  Reports to /rtt:8010                │   │
│  └──────────────────────────────────────┘   │
│                                              │
└─────────────────────────────────────────────┘

┌─────────────────────────────────────────────┐
│        DEPLOYMENT OPTIONS                    │
├─────────────────────────────────────────────┤
│                                              │
│ LOCAL (Development):                         │
│ ├─ Python venv (Python 3.12)               │
│ ├─ SQLite file-based                        │
│ ├─ Ollama Docker (local GPU optional)       │
│ └─ All services on localhost               │
│                                              │
│ KUBERNETES (Production):                     │
│ ├─ Orchestrator pod (port 8000-8022)       │
│ ├─ ML APIs sidecars (5001-5003)            │
│ ├─ Ollama deployment (11434)                │
│ ├─ PersistentVolume for SQLite              │
│ ├─ Service mesh for routing                │
│ └─ ConfigMap for configuration              │
│                                              │
│ Docker Compose (Testing):                    │
│ ├─ orchestrator service                     │
│ ├─ ml-api service (x3)                      │
│ ├─ ollama service                           │
│ ├─ vm-simulators service                    │
│ └─ picar-simulator service                  │
│                                              │
└─────────────────────────────────────────────┘
```

### **CI/CD Pipeline Recommended**

```yaml
.github/workflows/orchestrator.yml:
├─ Trigger: on push to main/develop
├─ Jobs:
│  ├─ Test
│  │  ├─ pytest test_orchestrator.py
│  │  ├─ coverage report
│  │  └─ fail if < 70%
│  │
│  ├─ Lint
│  │  ├─ flake8 . --max-line-length=100
│  │  └─ black --check .
│  │
│  ├─ Build
│  │  ├─ docker build -t orchestrator:latest .
│  │  └─ push to registry
│  │
│  └─ Deploy
│     ├─ kubectl apply -f k8s/orchestrator.yaml
│     └─ helm upgrade orchestrator ./chart
│
└─ Artifacts: test reports, coverage, docker image
```

### **Monitoring & Observability**

```
Current (Built-in):
├─ Logging: Colorama terminal output
├─ Metrics: None (no Prometheus/Grafana)
├─ Tracing: None
├─ Dashboard: Matplotlib live plot
├─ Database: SQLite query logs (implicit)
└─ Health: Manual checks @ startup

Recommended Additions:
├─ Prometheus metrics
│  ├─ decision_count{mode, decision}
│  ├─ migration_latency_seconds
│  ├─ slo_violation_count{metric}
│  └─ orchestrator_cycle_duration_seconds
│
├─ Grafana dashboards
│  ├─ Decision distribution
│  ├─ Performance trends
│  ├─ SLO compliance %
│  └─ Alert heatmaps
│
├─ Centralized logging (ELK/Loki)
│  ├─ Aggregate logs from all spokes
│  ├─ Full-text search
│  └─ Trend analysis
│
├─ Distributed tracing (Jaeger)
│  ├─ Trace /rtt request flow
│  ├─ Identify bottlenecks
│  └─ Latency breakdowns
│
└─ Alerting
   ├─ SLO violation thresholds
   ├─ Cooldown active alerts
   ├─ Master cloud failures
   └─ LLM unavailability
```

---

## 1️⃣2️⃣ PROBLÈMES ARCHITECTURAUX & RECOMMANDATIONS

### **Problèmes Identifiés**

| Catégorie | Problème | Sévérité | Impact | Solution |
|-----------|----------|----------|--------|----------|
| **Architecture** | Monolithic orchestrator (1722 lignes) | HIGH | Difficult to test/modify | Microservices ou modularization |
| **Scalabilité** | Thread-per-request Flask | MEDIUM | Max ~200 concurrent | FastAPI + async/await |
| **Scalabilité** | Polling-based (5s interval) | MEDIUM | Latency ~5s | Event-driven or push |
| **State Mgmt** | Global state in-memory | MEDIUM | Not distributed | Redis/etcd for distributed state |
| **Persistance** | Single SQLite instance | MEDIUM | No replication | PostgreSQL + replication |
| **Concurrency** | RLock for all state | MEDIUM | Lock contention | Fine-grained locking per resource |
| **Testing** | Minimal test coverage | HIGH | 12 tests only | Comprehensive mocking |
| **Observability** | No metrics/tracing | HIGH | Blind to production | Add Prometheus + Jaeger |
| **Availability** | No redundancy | HIGH | Single point of failure | N+1 orchestrators |
| **Configuration** | Hardcoded thresholds | MEDIUM | No dynamic config | ConfigMap + watch |
| **Deployment** | Manual setup required | HIGH | Error-prone | Docker + Kubernetes |
| **LLM Integration** | LLM failure → fallback | MEDIUM | Service degradation | Circuit breaker pattern |
| **Security** | No authentication | CRITICAL | Unauthorized access | OAuth2 + API keys |
| **Security** | HTTP to master cloud | CRITICAL | Man-in-middle risk | HTTPS enforcement |
| **Validation** | Weak input validation | MEDIUM | Injection risks | Comprehensive validation |
| **Debt** | Regex-based SLO parsing | MEDIUM | Brittle | Dedicated parser |
| **Performance** | SQL N+1 queries | MEDIUM | Database load | Query optimization + caching |
| **Performance** | No query caching | MEDIUM | Repeated DB hits | Redis cache layer |

### **Duplications Identifiées**

```
1. Flow execution logic (3 versions: classic/autonomous/enhanced)
   └─ Common steps: analyze → collect → decide → execute
   └─ Could abstract into pipeline pattern

2. Fallback logic scattered
   ├─ MLPredictor._get_api_prediction()
   ├─ Collector.collect_vm_metrics()
   ├─ IntentManager (4-tier cascade)
   └─ Could centralize into retry library

3. Validation repeated
   ├─ SLO validation in IntentManager
   ├─ Bounds checking separate from weight checking
   └─ Could use schema validation (Pydantic consistently)

4. Database query patterns
   ├─ get_recent_metrics (similar to get_metrics_with_violations)
   ├─ Could parameterize query builders
   └─ Use SQLAlchemy ORM

5. Logging format repeated
   ├─ log_step() function called many times
   ├─ ColoredFormatter for all logs
   └─ Could use structured logging (json logs)
```

### **Risques Identifiés**

```
1. MISSION-CRITICAL RISKS:
   ├─ Master cloud failure → migrations never execute
   │  └─ Mitigation: Queue + retry mechanism
   │
   ├─ LLM unavailable → cannot parse user intents
   │  └─ Mitigation: Circuit breaker + cache intent results
   │
   ├─ Database corruption → loss of all history + decisions
   │  └─ Mitigation: Backup strategy + WAL mode
   │
   └─ Cascading loop (rapid migrations) → system instability
      └─ Mitigation: Already has 60s cooldown, but inadequate

2. DATA INTEGRITY RISKS:
   ├─ Race condition: set_last_real_data_ts() unsynchronized
   │  └─ Should use @property with lock
   │
   ├─ SLO weight normalization not idempotent
   │  └─ Could introduce rounding errors
   │
   └─ Violation counting may miss data
      └─ Query design could miss edge cases

3. PERFORMANCE RISKS:
   ├─ Full scan of metrics table for percentiles
   │  └─ Use database indexes
   │
   ├─ No connection pooling
   │  └─ SQLite opens new connection per query
   │
   └─ Python eval() equivalent in regex parsing
      └─ Potential DoS

4. SECURITY RISKS:
   ├─ No input sanitization → SQL injection via vm_id
   │  └─ Use parameterized queries (already done, good)
   │
   ├─ No rate limiting on /rtt, /intent endpoints
   │  └─ Add Flask-Limiter
   │
   ├─ CORS not restricted
   │  └─ Add CORS headers
   │
   └─ Master cloud token hardcoded
      └─ Use environment variable + secrets manager
```

### **Bottlenecks Identifiés**

```
1. LATENCY BOTTLENECKS:
   ├─ Ollama LLM call: 90s timeout (async would help)
   │  └─ Current: 5s + 90s LLM = 95s worst case
   │  └─ Target: < 1s total latency per cycle
   │  └─ Fix: Async/await, query caching, model quantization
   │
   ├─ ML API calls: 3 sequential calls (5s each timeout)
   │  └─ Current: 5s + 10s*3 = 35s worst case
   │  └─ Fix: Parallel requests (asyncio.gather)
   │
   └─ Database percentile calculation
      └─ Full table scan on every query
      └─ Fix: Materialized views + periodic aggregation

2. THROUGHPUT BOTTLENECKS:
   ├─ Flask threaded mode (limited pool)
   │  └─ Can handle ~200 concurrent /rtt requests
   │  └─ Fix: Use Gunicorn with multiple workers
   │
   ├─ Single orchestrator instance
   │  └─ Cannot scale horizontally
   │  └─ Fix: Distributed state + multiple instances
   │
   └─ SQLite file-based (no concurrent writes)
      └─ Fix: PostgreSQL for production

3. MEMORY BOTTLENECKS:
   ├─ History buffer: 50 points per VM per metric
   │  └─ 3 metrics * 4 VMs * 50 points = 600 entries
   │  └─ Acceptable (~1MB)
   │
   └─ Dashboard: Matplotlib stores all points in memory
      └─ Could swap to web-based dashboard (Plotly)
```

### **Recommandations Prioritaires**

#### **P1: CRITICAL (Do First)**

1. **Add Authentication**
   ```python
   from flask_httpauth import HTTPBearerAuth
   auth = HTTPBearerAuth()
   
   @app.before_request
   @auth.login_required
   def verify_token():
       pass
   ```

2. **HTTPS Enforcement**
   ```python
   # Enforce HTTPS to master cloud
   requests.post(MASTER_URL, verify=True, cert=CA_BUNDLE)
   ```

3. **Input Validation**
   ```python
   from pydantic import ValidationError
   
   @app.route('/rtt', methods=['POST'])
   def receive_rtt():
       try:
           data = RTTMeasurementRequest(**request.json)
       except ValidationError as e:
           return {"error": str(e)}, 400
   ```

4. **Database Backup**
   ```bash
   # Automatic backups every hour
   0 * * * * cp orchestrator.db orchestrator.db.backup
   ```

#### **P2: HIGH (Next Priority)**

1. **Async/Await for Spokes**
   ```python
   # Migrate Flask to FastAPI
   from fastapi import FastAPI
   app = FastAPI()
   
   @app.post("/rtt")
   async def receive_rtt(payload: RTTMeasurement):
       await asyncio.gather(
           collect_vm_metrics(vm1),
           collect_vm_metrics(vm2),
           ...
       )
   ```

2. **Prometheus Metrics**
   ```python
   from prometheus_client import Counter, Histogram
   
   migration_counter = Counter(
       'orchestrator_migrations_total',
       'Total migrations',
       ['decision', 'mode']
   )
   ```

3. **Redis Cache for Percentiles**
   ```python
   import redis
   cache = redis.Redis(host='localhost', port=6379)
   
   percentile_key = f"{metric}:p25"
   cached = cache.get(percentile_key)
   ```

4. **Distributed State**
   ```python
   # Use etcd for orchestrator state
   import etcd3
   
   etcd.put("/orchestrator/service_vm", "vm3")
   etcd.put("/orchestrator/last_migration_ts", "1234567890")
   ```

#### **P3: MEDIUM (Nice to Have)**

1. **Refactor into Microservices**
   ```
   core-orchestrator/
   decision-engine/
   ml-predictor/
   collector/
   intent-manager/
   ```

2. **Web Dashboard** (replace Matplotlib)
   ```python
   from dash import Dash, dcc, html, Input, Output
   import plotly.express as px
   # Real-time plots, historical analysis
   ```

3. **Circuit Breaker for LLM**
   ```python
   from pybreaker import CircuitBreaker
   
   llm_breaker = CircuitBreaker(fail_max=5, timeout_duration=60)
   
   @llm_breaker
   def query_ollama():
       # Auto-fail after 5 failures
   ```

4. **Structured Logging**
   ```python
   import json
   import sys
   
   structured_log = {
       "timestamp": datetime.now().isoformat(),
       "level": "INFO",
       "event": "decision_made",
       "mode": "enhanced",
       "decision": "migrate",
       ...
   }
   print(json.dumps(structured_log), file=sys.stdout)
   ```

---

## 1️⃣3️⃣ RÉSUMÉ EXÉCUTIF

### **Vue d'Ensemble**

Le **VM Migration Orchestrator v2.1** est un système autonome de prise de décision temps-réel pour services de streaming. Basé sur une **architecture Hub-and-Spoke** sophistiquée, il assure une haute qualité de service (QoS) en migrant proactivement les services entre machines virtuelles basé sur:
- **Prédictions ML** (GRU, RNN, LSTM)
- **Intentions utilisateur** (LLM Ollama)
- **Budgets SLO** (violation tracking)
- **Sélection intelligente de métriques** (Mutual Information)

### **Points Forts**

✅ **Architecture bien structurée** — Hub-and-Spoke avec séparation claire des responsabilités
✅ **3 modes d'exécution** — Adapté à différents scénarios (classic/autonomous/enhanced)
✅ **RAG intelligent** — Contexte historique injecté dans LLM
✅ **Fallback robuste** — 4-tier cascade (LLM→Regex→Keywords→Default)
✅ **SLO normalization** — Poids auto-équilibrés
✅ **Anti-flapping** — Cooldown 60s + budget override
✅ **Persistance** — SQLite avec queries ACID
✅ **Visualisation** — Dashboard Matplotlib temps-réel
✅ **Thread-safe** — RLock pour concurrence
✅ **MI scoring** — Sélection intelligente des métriques à monitorer

### **Points Faibles**

❌ **Monolithic code** — 1722 lignes dans un seul fichier
❌ **Pas de scaling** — Flask thread-mode limité, SQLite mono-instance
❌ **Pas de distributions** — State management en-mémoire uniquement
❌ **Absence d'observabilité** — Pas de Prometheus/Grafana/Jaeger
❌ **Couverture test** — 12 tests seulement (< 30%)
❌ **Pas d'authentification** — Endpoints publics sans sécurité
❌ **Polling-based** — 5s latency (event-driven serait mieux)
❌ **Pas de CI/CD** — Déploiement manuel
❌ **Hardcoded config** — Thresholds non dynamiques
❌ **Regex parsing fragile** — SLO extraction brittle

### **Risques Principaux**

⚠️ **CRITICAL**: Pas d'authentification API
⚠️ **CRITICAL**: HTTP non-chiffré vers Master Cloud
⚠️ **HIGH**: Base de données centralisée (pas de réplication)
⚠️ **HIGH**: Single orchestrator instance (point of failure)
⚠️ **HIGH**: Logs non centralisés (blind to production)
⚠️ **MEDIUM**: LLM failure → service degradation
⚠️ **MEDIUM**: No rate limiting → DoS possible

### **Recommandations Prioritaires**

**Immédiat (< 1 week):**
1. Ajouter authentification API (Bearer tokens)
2. HTTPS enforcement vers master cloud
3. Input validation complète (Pydantic)
4. Database backups automatiques

**Court terme (1-4 weeks):**
1. Migrer vers FastAPI + async/await
2. Prometheus metrics + Grafana dashboard
3. Redis cache pour percentiles
4. etcd pour distributed state

**Moyen terme (1-3 months):**
1. Refactoriser en microservices
2. Déploiement Kubernetes
3. Circuit breakers pour résilience
4. Web dashboard (Plotly/Dash)

### **Conclusion**

Le projet démontre une **excellente compréhension des patterns d'architecture distribuée** avec une implémentation sophistiquée de Hub-and-Spoke, RAG, et décision multi-critères. Cependant, il montre des **limitations évidentes de scalabilité** et de production-readiness.

**Verdict:** ✅ **Bon pour POC/R&D**, ❌ **Pas prêt pour production**.

Avec les corrections P1+P2 proposées, le système pourrait être viable pour déploiement en environnement contrôlé. Les corrections P3 sont nécessaires pour véritable scalabilité.

