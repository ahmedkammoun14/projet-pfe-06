# 🚀  VM Migration Orchestrator v2.0

## 📝 Description
The **VM Migration Orchestrator v2.0** is an advanced autonomous decision-making system designed for real-time streaming services. Built on a strict **Hub-and-Spoke architecture**, it leverages predictive Machine Learning models (**GRU, RNN, LSTM**) and Large Language Models (**Ollama qwen2.5**) to ensure high Quality of Service (QoS) through proactive service migration.

The system continuously monitors Virtual Machine (VM) performance and triggers migrations based on Service Level Objectives (SLOs) extracted from natural language user intentions.

---

## 🏗️ Architecture

The project follows a modular **Hub-and-Spoke** design where a central **Hub (Core)** coordinates specialized **10 Spokes** (peripheral services).

### Spokes & Responsibilities
| Spoke | Port | Responsibility |
| :--- | :--- | :--- |
| **Core** | `8000` | Central orchestration, state management, and flow routing. |
| **LatencyManager** | `8010` | Receives RTT measurements from sensors (e.g., Picar). |
| **MLPredictor** | `8011` | Interface for QoS prediction APIs (Normalization/Denormalization). |
| **Collector** | `8012` | Asynchronous collection of physical metrics (CPU/RAM) from VMs. |
| **DecisionIntelligence** | `8013` | Algorithms for threshold-based and SLO-based decisions. |
| **IntentManager** | `8014` | LLM interface for extracting SLOs from natural language. |
| **Config** | `8015` | Dynamic runtime configuration and threshold management. |
| **Observability** | `8016` | Real-time visual dashboard using Matplotlib. |
| **Database** | `8020` | Thread-safe SQLite persistence for metrics and decisions. |
| **HistoryLoader** | `8021` | Historical data window extraction for ML and Analysis. |
| **MetricsManager** | `8022` | Dependency analysis for SLO metrics. |

### Data Flow Diagram (ASCII)
```text
[ User / Intent ]        [ Picar / RTT ]
       |                       |
       v                       v
[ IntentManager ]       [ LatencyManager ]
       |                       |
       +-------[ CORE ]--------+
                 |
    +------------+------------+------------+
    |            |            |            |
[  ML  ]    [ Decision ]    [  DB  ]    [ MASTER ]
```

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
   git clone https://github.com/ahmedkammoun14/simulation-with-arch-hub-v0
   cd simulation-with-arch-hub-v0
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

### 2. Train Models (Initial Setup Only)
Initialize and train the models with historical datasets:
```bash
curl -X GET "http://localhost:5001/train?file_name=node1_delay.csv"
curl -X GET "http://localhost:5002/train?file_name=node1_cpu.csv"
curl -X GET "http://localhost:5003/train?file_name=node1_ram.csv"
```

### 3. Start the Orchestrator
```bash
python orchestrator.py
```

---

## 🔄 Usage Modes

### Classic Mode (Default)
Starts automatically in simulation mode. It generates random RTT values every 5 seconds until real data is received via the API.
- **Trigger**: Migration occurs if `RTT > 50ms`.

### Enhanced Mode (LLM-Activated)
Activated automatically when a natural language intention is sent to the system. It enables full observability (CPU/RAM/RTT).
- **Trigger**: Migration occurs if any **AI-defined SLO** is violated.

---

## 📡 API Endpoints

| Method | Endpoint | Port | Description |
| :--- | :--- | :--- | :--- |
| `POST` | `/rtt` | `8010` | Send real RTT measurements (Picar format). |
| `POST` | `/intent` | `8014` | Send natural language intent to the LLM. |
| `GET` | `/status` | `8000` | Retrieve current mode, uptime, and SLOs. |
| `POST` | `/mode` | `8000` | Manually switch between `classic` and `enhanced`. |
| `POST` | `/slos` | `8000` | Manually update SLO objects. |

---

## ⌨️ Request Examples (PowerShell)

**1. Send Real RTT Measurements**:
```powershell
$body = @{
    source = "picar"
    measurements = @(
        @{vm_id="vm1"; rtt_ms=12.3},
        @{vm_id="vm2"; rtt_ms=45.2},
        @{vm_id="vm3"; rtt_ms=8.9},
        @{vm_id="vm4"; rtt_ms=27.6}
    )
} | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://localhost:8010/rtt" -Body $body -ContentType "application/json"
```

**2. Send Intent to LLM**:
```powershell
$body = @{ intention = "Keep latency below 20ms and CPU below 60%" } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "http://localhost:8014/intent" -Body $body -ContentType "application/json"
```

**3. Check Core Status**:
```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8000/status"
```

---

## ⚡ Execution Flows

### Classic Flow (7 Steps)
1. **Store**: Core stores received RTTs in the Database.
2. **Load**: Core requests history from the HistoryLoader.
3. **Request**: Core sends data to the MLPredictor.
4. **Return**: MLPredictor returns RTT predictions to the Core.
5. **Analyze**: Core requests a decision from DecisionIntelligence.
6. **Return**: DecisionIntelligence returns `migrate` or `stay`.
7. **Command**: Core sends the migration command to the Master Cloud.

### Enhanced Flow (9 Steps)
1. **Analyze**: Core asks MetricsManager to analyze LLM-extracted SLOs.
2. **Request**: MetricsManager asks Collector to gather physical metrics.
3. **Store**: Collector stores CPU/RAM metrics in the Database.
4. **Load**: Core retrieves the full history context (RTT+CPU+RAM).
5. **Request**: Core sends the full context to the MLPredictor.
6. **Return**: MLPredictor returns multi-variable predictions.
7. **Analyze**: Core requests a decision based on strict SLO thresholds.
8. **Return**: DecisionIntelligence returns the final migration decision.
9. **Command**: Core sends the migration command to the Master Cloud.

---

## 📁 Project Structure
- `orchestrator.py`: The main Hub-and-Spoke script (600+ lines).
- `test_orchestrator.py`: Robust test suite (12+ Unit and Integration tests).
- `requirements.txt`: Project dependencies.
- `intent_engine/`: Module for Ollama LLM communication.
- `models/`: Pydantic data schemas.
- `QoS_Pred_API_V2/`: Machine Learning API source code.
- `orchestrator.db`: Auto-managed SQLite database.

---

## 🛠️ Tech Stack
| Component | Technology |
| :--- | :--- |
| **Orchestrator** | Python 3.12 / Flask |
| **LLM** | Ollama (qwen2.5) |
| **ML Latency** | GRU (Gated Recurrent Unit) |
| **ML CPU** | RNN (Recurrent Neural Network) |
| **ML RAM** | LSTM (Long Short-Term Memory) |
| **Database** | SQLite 3 |
| **Visualisation** | Matplotlib (Real-time dashboard) |
| **Terminal UI** | Colorama (Color-coded logging) |
| **Testing** | Pytest |


