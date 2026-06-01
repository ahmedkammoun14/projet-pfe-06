import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx

# Configuration
VM_PORTS = {"vm1": 8101, "vm2": 8102, "vm3": 8103, "vm4": 8104}
LATENCY_MANAGER_URL = "http://localhost:8010/rtt"
INTERVAL_SECONDS = 5
# Retry / startup tuning to handle race with Flask startup
MAX_RETRIES = 3
RETRY_DELAY = 2.0  # seconds between retries when posting
INITIAL_DELAY = 3.0  # initial wait to let Flask apps start

# Configuration du logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s"
)
logger = logging.getLogger("picar_simulator")

async def fetch_rtt(client: httpx.AsyncClient, vm_id: str, port: int) -> Optional[float]:
    """Récupère le RTT d'une VM via son endpoint /health."""
    url = f"http://localhost:{port}/health"
    try:
        resp = await client.get(url, timeout=2.0)
        resp.raise_for_status()
        return resp.json().get("response_time_ms")
    except Exception:
        return None

async def run_cycle(client: httpx.AsyncClient, cycle_num: int):
    """Exécute un cycle complet de mesure et de rapport."""
    
    # 1 & 2. Mesures RTT en parallèle
    tasks = [fetch_rtt(client, vm_id, port) for vm_id, port in VM_PORTS.items()]
    results = await asyncio.gather(*tasks)
    
    measurements = []
    log_parts = []
    
    for vm_id, rtt in zip(VM_PORTS.keys(), results):
        if rtt is not None:
            measurements.append({"vm_id": vm_id, "rtt_ms": rtt})
            log_parts.append(f"{vm_id}={rtt:.1f}ms")
    
    logger.info(f"[PICAR] Cycle #{cycle_num} → RTT mesurés : {' '.join(log_parts)}")
    
    if not measurements:
        return

    # 3. Construction du payload
    payload = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": "picar",
        "measurements": measurements
    }

    # 4. Envoi au Latency Manager avec retry/backoff
    sent = False
    for attempt in range(MAX_RETRIES):
        try:
            resp = await client.post(LATENCY_MANAGER_URL, json=payload, timeout=5.0)
            resp.raise_for_status()
            logger.info(f"[PICAR] Envoi → POST {LATENCY_MANAGER_URL} → status {resp.status_code}")
            sent = True
            break
        except (httpx.ConnectError, httpx.HTTPStatusError, httpx.TimeoutException) as e:
            if attempt < MAX_RETRIES - 1:
                logger.warning(f"[PICAR] Envoi échoué (attempt {attempt+1}/{MAX_RETRIES}): {e} — retry dans {RETRY_DELAY}s")
                await asyncio.sleep(RETRY_DELAY)
            else:
                logger.error(f"[PICAR] Erreur envoi (Latency Manager indisponible) après {MAX_RETRIES} tentatives")
    # sent indique si l'envoi a réussi au moins une fois

async def main():
    """Boucle principale du simulateur PiCar."""
    logger.info("Démarrage du simulateur PiCar (RTT Reporter)...")
    
    # Laisser le temps aux services Flask (LatencyManager, VMs) de démarrer
    logger.info(f"[PICAR] Attente initiale de {INITIAL_DELAY}s pour les services")
    await asyncio.sleep(INITIAL_DELAY)

    async with httpx.AsyncClient() as client:
        cycle = 1
        while True:
            start_time = asyncio.get_event_loop().time()
            
            await run_cycle(client, cycle)
            
            cycle += 1
            # Calcul du sleep pour maintenir l'intervalle exact
            elapsed = asyncio.get_event_loop().time() - start_time
            sleep_time = max(0, INTERVAL_SECONDS - elapsed)
            await asyncio.sleep(sleep_time)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Arrêt du simulateur PiCar.")
