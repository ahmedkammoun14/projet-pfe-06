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
