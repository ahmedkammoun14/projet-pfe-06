# 🚀 VM Migration Orchestrator v2.1 (Overhaul)

## 📝 Description
The **VM Migration Orchestrator v2.1** is an advanced autonomous decision-making system designed for real-time streaming services. Built on a strict **Hub-and-Spoke architecture**, it leverages predictive Machine Learning models (**GRU, RNN, LSTM**) and Large Language Models (**Ollama qwen2.5**) to ensure high Quality of Service (QoS) through proactive service migration.

This version features a complete overhaul of the core logic, introducing a **Multi-Criteria Predictive Engine**, a **Cooldown Mechanism**, and a **Full Simulation Ecosystem**.

---

## 🏗️ Architecture

The project follows a modular **Hub-and-Spoke** design where a central **Hub (Core)** coordinates specialized **10 Spokes** (peripheral services).

### Spokes & Responsibilities
| Spoke | Port | Responsibility |
| :--- | :--- | :--- |
| **Core** | `8000` | Central orchestration, state management (Service VM tracking), and flow routing. |
| **LatencyManager** | `8010` | Receives RTT measurements from sensors (e.g., Picar). |
| **MLPredictor** | `8011` | Interface for QoS prediction APIs (Normalization/Denormalization). |
| **Collector** | `8012` | Asynchronous collection of physical metrics (CPU/RAM) from VMs. |
| **DecisionIntelligence** | `8013` | Service-centric algorithms for threshold-based and SLO-based decisions. |
| **IntentManager** | `8014` | LLM interface for extracting and normalizing SLOs from natural language. |
| **Config** | `8015` | Dynamic runtime configuration and threshold management. |
| **Observability** | `8016` | Real-time visual dashboard with service location and decision tracking. |
| **Database** | `8020` | Thread-safe SQLite persistence for metrics and decisions. |
| **HistoryLoader** | `8021` | Historical data window extraction for ML and Analysis. |
| **MetricsManager** | `8022` | Dependency analysis for SLO metrics. |

---

## 🛠️ Simulation Ecosystem

To facilitate testing and demonstration, two simulation scripts are provided:

### 1. VM Simulator (`vm_simulator.py`)
Simulates 4 distinct Virtual Machines (`vm1` to `vm4`) running on ports `8101` to `8104`.
- **Endpoints**: `/health` (returns RTT) and `/metrics` (returns CPU/RAM).
- **Behavior**: Each VM has a unique profile (e.g., `vm2` is intentionally "heavy" to trigger migrations).

### 2. PiCar Simulator (`picar_simulator.py`)
Acts as a mobile sensor reporter (e.g., a car moving through the network).
- **Action**: Polls all 4 VMs every 5 seconds and reports the measurements to the Orchestrator's **LatencyManager** (`:8010`).

---

## ⚙️ Prerequisites
- **Python 3.12+**
- **Ollama** installed and serving `qwen2.5`
- **QoS_Pred_API_V2** instances (Machine Learning APIs)
- Windows/Linux/MacOS environment

---

## 📥 Installation

1. **Clone the repository**:
   ```bash
   git clone https://github.com/ahmedkammoun14/simulation-with-arch-hub-v1-26-05-2026.git
   cd simulation-with-arch-hub-v1-26-05-2026
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Set up Ollama**:
   ```bash
   ollama serve
   ollama run qwen2.5
   ```

---

## 🚀 Startup Procedure

### 1. Launch ML Prediction APIs (3 separate terminals)
The orchestrator requires three distinct ML API instances:
- **Terminal 1 (Latency)**: `uvicorn app.auto:auto_app --port 5001`
- **Terminal 2 (CPU)**: `uvicorn app.auto:auto_app --port 5002`
- **Terminal 3 (RAM)**: `uvicorn app.auto:auto_app --port 5003`

### 2. Launch the Simulation Infrastructure (2 separate terminals)
- **Terminal 4 (VMs)**: `python vm_simulator.py`
- **Terminal 5 (PiCar)**: `python picar_simulator.py`

### 3. Start the Orchestrator
```bash
python orchestrator.py
```

---

## 🔄 New Features (v2.1)

- **Service-Centric Decision Logic**: The system now tracks the specific VM hosting the service and only evaluates migration for that node, improving efficiency.
- **Automated SLO Normalization**: Integrated logic to ensure that SLO weights extracted by the LLM always sum to 1.0, preventing biased decision scoring.
- **Enhanced Observability Dashboard**: Real-time Matplotlib visualization now highlights the active Service VM (Green) and displays live migration decisions (STAY/MIGRATE) with reasons.
- **Migration Cooldown**: A 60-second cooldown is enforced between migrations to prevent "flapping" (unstable oscillation between nodes).
- **Robust Testing**: Comprehensive test suite in `test_orchestrator.py` covering core logic, weight normalization, and service tracking.

---

## 📡 API Endpoints

| Method | Endpoint | Port | Description |
| :--- | :--- | :--- | :--- |
| `POST` | `/rtt` | `8010` | Send real RTT measurements (Picar format). |
| `POST` | `/intent` | `8014` | Send natural language intent to the LLM. |
| `GET` | `/status` | `8000` | Retrieve current mode, uptime, SLOs, and active **Service VM**. |
| `POST` | `/mode` | `8000` | Manually switch between `classic` and `enhanced`. |
| `POST` | `/slos` | `8000` | Manually update SLO objects. |

---

## ⌨️ Request Examples (PowerShell)

**1. Send Intent to LLM**:
```powershell
$body = @{ intention = "Keep latency below 20ms and CPU below 60%" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://localhost:8014/intent" -Body $body -ContentType "application/json"
```

**2. Check Core Status**:
```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8000/status"
```

---

## 📁 Project Structure
- `orchestrator.py`: The main Hub-and-Spoke script (700+ lines).
- `vm_simulator.py`: Simulator for 4 VMs with REST health/metrics endpoints.
- `picar_simulator.py`: Reporter script simulating a mobile sensor.
- `test_orchestrator.py`: Robust test suite (12+ Unit and Integration tests).
- `intent_engine/`: Module for Ollama LLM communication.
- `models/`: Pydantic data schemas.
- `orchestrator.db`: Auto-managed SQLite database.

---

## 🛠️ Tech Stack
| Component | Technology |
| :--- | :--- |
| **Orchestrator** | Python 3.12 / Flask |
| **Simulators** | FastAPI / Uvicorn / Httpx |
| **LLM** | Ollama (qwen2.5) |
| **ML Latency** | GRU (Gated Recurrent Unit) |
| **ML CPU** | RNN (Recurrent Neural Network) |
| **ML RAM** | LSTM (Long Short-Term Memory) |
| **Database** | SQLite 3 |
| **Visualisation** | Matplotlib (Real-time dashboard with Decision Feedback) |
| **Terminal UI** | Colorama (Color-coded logging) |
| **Testing** | Pytest |
