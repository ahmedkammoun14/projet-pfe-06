import threading
import time
import random
from flask import Flask, jsonify, request
import asyncio
import logging

logging.basicConfig(level=logging.INFO, format='%(message)s')

# Latency manager mock
app_latency = Flask('latency_mock')

@app_latency.route('/rtt', methods=['POST'])
def rtt():
    data = request.json
    print('[MOCK LATENCY] Received', data.get('measurements'))
    return jsonify({'status':'ok'}), 200

# VM health mocks
def make_vm_app(port):
    app = Flask(f'vm_{port}')

    @app.route('/health')
    def health():
        # return a pseudo rtt
        rtt = random.uniform(10, 100)
        return jsonify({'response_time_ms': rtt}), 200

    def run():
        app.run(host='0.0.0.0', port=port, threaded=True, use_reloader=False)

    t = threading.Thread(target=run, daemon=True)
    t.start()
    return t

def run_latency_app():
    app_latency.run(host='0.0.0.0', port=8010, threaded=True, use_reloader=False)

if __name__ == '__main__':
    # start latency manager mock
    tlat = threading.Thread(target=run_latency_app, daemon=True)
    tlat.start()
    # start VM health mocks
    vm_threads = []
    for p in [8101, 8102, 8103, 8104]:
        vm_threads.append(make_vm_app(p))

    # Give servers time to start
    time.sleep(1.5)

    # Run a single picar cycle
    import picar_simulator
    import httpx

    async def single_cycle():
        async with httpx.AsyncClient() as client:
            await picar_simulator.run_cycle(client, 1)

    asyncio.run(single_cycle())

    print('Integration test finished')
