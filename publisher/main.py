import os
import time
import uuid
import random
import requests
import json
import logging
from datetime import datetime
from collections import deque
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

# --- Konfigurasi ---
TARGET_URL = os.getenv("TARGET_URL", "http://aggregator:8080/publish")
HEALTH_URL = os.getenv("HEALTH_URL", "http://aggregator:8080/stats") # Endpoint untuk cek status
TOTAL_EVENTS = int(os.getenv("TOTAL_EVENTS", "25000")) 
DUPLICATE_RATIO = float(os.getenv("DUPLICATE_RATIO", "0.3")) 

# Variasi Topic untuk pengujian filtering
TOPICS = ["user-activity", "payment-log", "system-alert", "auth-trace"]

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("Publisher")

def get_session():
    """Membuat session dengan retry logic otomatis."""
    session = requests.Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def wait_for_service(session):
    """Looping sampai Aggregator siap menerima koneksi."""
    logger.info("⏳ Waiting for Aggregator service...")
    while True:
        try:
            resp = session.get(HEALTH_URL, timeout=2)
            if resp.status_code == 200:
                logger.info("✅ Aggregator is UP! Starting simulation...")
                break
        except requests.exceptions.RequestException:
            pass
        time.sleep(1)

def generate_event(event_id=None):
    return {
        "topic": random.choice(TOPICS),
        "event_id": event_id or str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "source": "publisher-simulator",
        "payload": {
            "action": random.choice(["click", "view", "purchase", "error"]),
            "value": random.randint(1, 100),
            "meta": "random-data-string"
        }
    }

def run_simulation():
    session = get_session()
    
    # 1. Pastikan Aggregator nyala dulu
    wait_for_service(session)

    logger.info(f"🚀 Starting Simulation: {TOTAL_EVENTS} events.")
    logger.info(f"📊 Config: {DUPLICATE_RATIO*100}% duplicate chance.")
    
    # Deque lebih efisien memori & performa dibanding list biasa untuk queue
    sent_ids = deque(maxlen=10000) 
    
    start_time = time.time()
    
    for i in range(1, TOTAL_EVENTS + 1):
        # 2. Logika Duplikasi
        if len(sent_ids) > 0 and random.random() < DUPLICATE_RATIO:
            # Ambil ID lama untuk disimulasikan sebagai duplikat (Retry Storm)
            event_id = random.choice(sent_ids)
            is_duplicate = True
        else:
            # Buat ID baru
            event_id = str(uuid.uuid4())
            sent_ids.append(event_id)
            is_duplicate = False
            
        event = generate_event(event_id)
        
        try:
            # Menggunakan session jauh lebih cepat (Reuse TCP Connection)
            resp = session.post(TARGET_URL, json=event, timeout=5)
            
            # Logging berkala agar terlihat progresnya di video demo
            if i % 1000 == 0:
                elapsed = time.time() - start_time
                rate = i / elapsed
                logger.info(f"[{i}/{TOTAL_EVENTS}] Sent. Dup: {is_duplicate}. Rate: {rate:.2f} req/s")
                
        except Exception as e:
            logger.error(f"❌ Error sending event {i}: {e}")
            
        # Optional: Sleep sangat kecil jika ingin mengatur ritme, 
        # tapi untuk stress test, biarkan secepat mungkin.
        # time.sleep(0.0001) 

    total_time = time.time() - start_time
    logger.info(f"✅ Simulation Finished. Sent {TOTAL_EVENTS} events in {total_time:.2f}s ({TOTAL_EVENTS/total_time:.2f} req/s).")

if __name__ == "__main__":
    run_simulation()