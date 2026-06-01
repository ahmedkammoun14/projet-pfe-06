# 📄 Documentation Technique : VM Migration Orchestrator v2.1

Ce document détaille l'architecture, le fonctionnement du code et le pipeline opérationnel du projet **VM Migration Orchestrator**.

---

## 🏗️ 1. Architecture Globale : Hub-and-Spoke

Le projet repose sur une architecture en **étoile (Hub-and-Spoke)**. Un cœur central (**Hub**) coordonne plusieurs services spécialisés (**Spokes**). Cette approche permet une modularité maximale et une séparation claire des responsabilités.

### Le Cœur (Hub) : `OrchestratorCore`
Situé dans `orchestrator.py`, il gère l'état global du système (mode de fonctionnement, SLOs actifs, VM de service actuelle) et orchestre les échanges entre les spokes.

### Les Spokes (Services Périphériques)
| Spoke | Rôle |
| :--- | :--- |
| **LatencyManager** | Réceptionne les mesures RTT en temps réel du PiCar. |
| **IntentManager** | Transforme les intentions en langage naturel (via Ollama) en SLOs structurés. |
| **MetricsManager** | Détermine intelligemment les métriques nécessaires via l'Information Mutuelle (MI). |
| **Collector** | Récupère les métriques physiques (CPU/RAM) directement auprès des VMs. |
| **MLPredictor** | Interface avec les APIs de Machine Learning (GRU, RNN, LSTM) pour les prédictions. |
| **DecisionIntelligence** | Moteur de décision multi-critères basé sur les seuils et les prédictions. |
| **Database** | Persistance thread-safe des métriques et des décisions dans SQLite. |
| **Observability** | Dashboard temps réel avec Matplotlib. |
| **HistoryLoader** | Extraction des fenêtres de données historiques pour le ML. |

---

## 🛠️ 2. Écosystème de Simulation

Le projet inclut des simulateurs pour reproduire un environnement réseau dynamique :

1.  **VM Simulator (`vm_simulator.py`)** :
    *   Lance 4 serveurs FastAPI (`vm1` à `vm4`).
    *   Expose des endpoints `/health` (RTT), `/metrics` (CPU/RAM), `/activate` et `/deactivate`.
    *   Simule des profils de charge différents (ex: `vm2` est volontairement instable).
2.  **PiCar Simulator (`picar_simulator.py`)** :
    *   Simule un capteur mobile (voiture) mesurant la latence vers les 4 VMs toutes les 5 secondes.
    *   Envoie ces données au `LatencyManager` de l'orchestrateur.

---

## 🚀 3. Pipeline Opérationnel (Workflows)

L'orchestrateur fonctionne principalement selon deux flux : le flux **Autonome** et le flux **Enhanced** (centré sur l'intention).

### Flux Autonome (Le "Cerveau" du système)
Ce cycle est déclenché à chaque réception de données RTT :

1.  **Analyse des métriques (MI)** : Le `MetricsManager` calcule le score d'Information Mutuelle pour voir quelles métriques (CPU, RAM) influencent le plus les violations de latence.
2.  **Collecte** : Le `Collector` récupère uniquement les métriques jugées nécessaires sur les VMs.
3.  **Stockage** : Les données sont sauvegardées dans `orchestrator.db`.
4.  **Prédiction ML** : L'orchestrateur demande des prédictions à 5 étapes au `MLPredictor` (ex: "Quelle sera la latence dans 10 secondes ?").
5.  **Décision** :
    *   Le moteur évalue si la VM actuelle viole un SLO ou si une violation est prédite.
    *   Il calcule un score pour chaque VM cible en combinant prédictions et "budget SLO" restant.
6.  **Exécution** : Si une migration est décidée, l'orchestrateur appelle `/deactivate` sur la source et `/activate` sur la cible.

### Flux d'Intention (LLM & RAG)
Lorsqu'un utilisateur envoie une phrase (ex: *"Je veux une latence faible pour le streaming"*), le système :
1.  **RAG (Retrieval Augmented Generation)** : Extrait les performances récentes et les percentiles historiques de la base de données.
2.  **LLM (Ollama qwen2.5)** : Utilise ces données comme contexte pour transformer la phrase en SLOs précis (ex: `latency < 25ms`).
3.  **Validation** : Vérifie la cohérence physique des SLOs et rééquilibre les poids pour qu'ils somment à 1.0.

---

## 📊 4. Logique de Décision Avancée

Le système ne se contente pas de réagir à un dépassement de seuil. Il utilise :
*   **Priorisation par Sévérité** : Traite en priorité la métrique la plus critique (celle qui dépasse le plus son seuil).
*   **Budget SLO** : Chaque SLO a un budget de violation (ex: 99% de respect). Si le budget s'épuise, le système devient plus agressif dans ses migrations.
*   **Mécanisme de Cooldown** : Empêche les migrations incessantes ("flapping") en imposant une attente (ex: 60s) entre deux actions, sauf urgence absolue (budget épuisé).

---

## 📂 5. Structure du Code

*   `orchestrator.py` : Code monolithique organisé par classes (Spokes) et une classe maîtresse (`OrchestratorCore`).
*   `test_orchestrator.py` : Tests unitaires et d'intégration validant la logique de normalisation et de décision.

---

## 💡 Résumés des Pipelines de Données

Le système adapte son pipeline selon le mode actif :

### A. Mode Autonome (Standard)
`PiCar` ➔ `LatencyManager` ➔ `MetricsManager (Filtrage MI)` ➔ `Collector (CPU/RAM)` ➔ `MLPredictor (Prédiction)` ➔ `DecisionIntelligence (Seuils statiques/prédits)` ➔ `Hub (Action)` ➔ `VMs`.

### B. Mode Enhanced (Centré sur l'Intention)
Ce mode ajoute une couche d'intelligence cognitive :
1.  **Phase d'Extraction (Asynchrone)** :
    `Utilisateur (Phrase)` ➔ `IntentManager` ➔ `RAG (Context Database)` ➔ `LLM (Ollama)` ➔ `SLOs Dynamiques`.
2.  **Phase Opérationnelle (Cycle)** :
    `Données Temps Réel` ➔ `Analyse des SLOs Dynamiques` ➔ `MLPredictor (Multi-métriques)` ➔ `DecisionIntelligence (Sévérité & Budget SLO)` ➔ `Hub (Action)` ➔ `VMs`.

