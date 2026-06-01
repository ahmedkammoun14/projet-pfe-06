# 🚀 VM Migration Orchestrator v2.1 (Hub-and-Spoke Edition)

## 📝 Description
The **VM Migration Orchestrator v2.1** is a high-performance, autonomous decision-making engine designed for real-time edge computing and streaming services. Built on a strict **Hub-and-Spoke architecture**, it leverages predictive Machine Learning models (**GRU, RNN, LSTM**) and Large Language Models (**Ollama qwen2.5**) to maintain high Quality of Service (QoS) through proactive service migration across distributed Virtual Machines.

This version features a sophisticated **Multi-Criteria Decision Engine**, an **SLO Budgeting System**, and **Mutual Information (MI) based metric selection**.

---

## 🏗️ Architecture

The system follows a modular **Star Topology** where the **OrchestratorCore (Hub)** coordinates **9 specialized Spokes**.

### Spokes & Responsibilities
| Spoke | Port | Responsibility |
| :--- | :--- | :--- |
| **Core (Hub)** | `8000` | Central state management, flow routing, and health synchronization. |
| **LatencyManager** | `8010` | Ingestion of real-time RTT measurements from mobile sensors (e.g., PiCar). |
| **IntentManager** | `8014` | Natural Language processing via Ollama to extract structured SLOs. |
| **MetricsManager** | N/A | Intelligent selection of critical metrics using Mutual Information (MI). |
| **Collector** | N/A | Targeted extraction of CPU/RAM metrics from active and candidate VMs. |
| **MLPredictor** | `500x` | Multi-step prediction interface (GRU for Latency, RNN for CPU, LSTM for RAM). |
| **DecisionIntelligence**| N/A | Multi-criteria evaluation using Severity, SLO Budgets, and ML forecasts. |
| **Database** | N/A | Thread-safe SQLite persistence for telemetry, decisions, and RAG context. |
| **Observability** | `GUI` | Real-time Matplotlib dashboard and visual terminal logging. |

---

## 🛠️ Intelligent Features (v2.1)

- **🔄 Dual Operational Pipelines**:
    - **Autonomous Flow (8-Step)**: Default mode using standard SLOs and MI-based metric collection.
    - **Enhanced Flow (9-Step)**: User-centric mode where a LLM translates natural language "intentions" into dynamic SLOs.
- **🧠 MI-Driven Collection**: To save bandwidth and processing power, the system only collects metrics (CPU/RAM) that show high Mutual Information with latency violations.
- **📉 SLO Budgeting & Cooldown**: Each SLO has a "violation budget" (e.g., 99% uptime). If a budget is exhausted, the system overrides the 60s safety cooldown to perform emergency migrations.
- **📺 Visual Terminal Logging**: Every decision cycle is visualized in the terminal with a boxed, color-coded step-by-step breakdown (1. Analyze -> 2. Collect -> ... -> 9. Command).
- **🩺 Automated Health Check**: On startup, the Hub verifies the status of SQLite, Ollama, ML APIs, and port availability.
- **🧬 Weighted ML Predictions**: Migration decisions are based on a decreasing weighted mean of the next 5 predicted steps, prioritizing immediate future stability.

---

## 🚀 Startup & Simulation

### 1. Prerequisites
- **Python 3.12+**
- **Ollama** serving `qwen2.5`
- **Machine Learning APIs** (running on ports 5001, 5002, 5003)

### 2. Launching the Ecosystem
```bash
# Terminal 1: VM Infrastructure
python vm_simulator.py

# Terminal 2: Sensor Simulation (PiCar)
python picar_simulator.py

# Terminal 3: Orchestrator Core
python orchestrator.py
```

---

## 📡 API Interaction Examples

### **Update Intention (LLM)**
```bash
curl -X POST http://localhost:8014/intent \
     -H "Content-Type: application/json" \
     -d '{"intention": "Keep latency under 25ms and prioritize low CPU usage"}'
```

### **Check System Status**
```bash
curl http://localhost:8000/status
```

---

## 📂 Project Structure
- `orchestrator.py`: The main Hub-and-Spoke implementation (1700+ lines).
- `vm_simulator.py`: Simulates 4 VMs with health/metrics REST endpoints.
- `picar_simulator.py`: Simulates a mobile sensor reporting RTT.
- `test_orchestrator.py`: Comprehensive test suite (Unit + Integration).

---

## 🛠️ Tech Stack
| Component | Technology |
| :--- | :--- |
| **Backend** | Python 3.12 / Flask (Hub) / FastAPI (Simulators) |
| **Intelligence** | Ollama (LLM) / PyTorch (ML) / Scikit-learn (MI) |
| **Persistence** | SQLite 3 (Thread-safe) |
| **Visualization** | Matplotlib / Colorama (TUI) |
| **Testing** | Pytest / Unittest |
