import asyncio
import json
import os
import logging
from contextlib import asynccontextmanager
from typing import Optional, List, Dict, Any

import asyncpg
import redis.asyncio as redis
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

# --- Konfigurasi ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@storage:5432/db")
BROKER_URL = os.getenv("BROKER_URL", "redis://broker:6379")
QUEUE_NAME = "event_queue"
STATS_DUPLICATE_KEY = "stats:duplicates"
WORKER_COUNT = 5  # Concurrency: 5 worker berjalan paralel

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("Aggregator")

# Global Connectors
redis_client: Optional[redis.Redis] = None
db_pool: Optional[asyncpg.Pool] = None

# --- Model Data ---
class EventPayload(BaseModel):
    topic: str
    event_id: str
    timestamp: str
    source: str
    payload: Dict[str, Any] # Flexible dict

# --- Database Setup & Logic ---
async def init_db():
    """
    Inisialisasi tabel sesuai dengan Laporan Bab 3.2.
    Menggunakan CONSTRAINT UNIQUE untuk menjamin Idempotency.
    """
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)
    
    async with db_pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS processed_events (
                id SERIAL PRIMARY KEY,
                event_id VARCHAR(255) NOT NULL,
                topic VARCHAR(100) NOT NULL,
                payload JSONB NOT NULL,
                status VARCHAR(50) DEFAULT 'processed',
                created_at TIMESTAMP DEFAULT NOW(),
                
                -- CONSTRAINT UNIQUE: Kunci utama pencegahan duplikasi
                CONSTRAINT unique_event_dedup UNIQUE (topic, event_id)
            );
            -- Index untuk mempercepat pencarian/filtering endpoint GET
            CREATE INDEX IF NOT EXISTS idx_topic ON processed_events(topic);
        """)
        logger.info("✅ Database initialized (Schema matches Report Table 3.1).")

async def process_event_transactionally(event: dict):
    """
    Menyimpan event dengan jaminan ACID & Idempotency.
    Isolation Level: READ COMMITTED (Cukup karena mengandalkan Unique Constraint).
    """
    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction(isolation='read_committed'): 
                # Gunakan json.dumps untuk kolom JSONB
                payload_json = json.dumps(event.get('payload', {}))
                
                result = await conn.execute("""
                    INSERT INTO processed_events (event_id, topic, payload)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (topic, event_id) DO NOTHING
                """, event['event_id'], event['topic'], payload_json)
                
                # "INSERT 0 1" = Sukses, "INSERT 0 0" = Konflik/Duplikat
                if result == "INSERT 0 1":
                    return "PROCESSED"
                else:
                    return "DUPLICATE"
    except Exception as e:
        logger.error(f"❌ DB Transaction Error: {e}")
        return "ERROR"

# --- Background Worker ---
async def consumer_worker(worker_id: int):
    """Worker asinkron yang mengambil pesan dari Redis."""
    logger.info(f"👷 Worker-{worker_id} started.")
    processed_count = 0
    
    while True:
        try:
            # BRPOP: Blocking wait (2 detik timeout agar bisa shutdown)
            task = await redis_client.brpop(QUEUE_NAME, timeout=2)
            
            if task:
                _, data_json = task
                event_data = json.loads(data_json)
                
                status = await process_event_transactionally(event_data)
                
                if status == "PROCESSED":
                    processed_count += 1
                    if processed_count % 500 == 0:
                        logger.info(f"[Worker-{worker_id}] Processed {processed_count} events...")
                        
                elif status == "DUPLICATE":
                    # Atomic Increment di Redis untuk statistik
                    await redis_client.incr(STATS_DUPLICATE_KEY)
                
        except asyncio.CancelledError:
            logger.info(f"Worker-{worker_id} stopping...")
            break
        except Exception as e:
            logger.error(f"Worker-{worker_id} Error: {e}")
            await asyncio.sleep(1)

# --- FastAPI Lifecycle ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    # -- Startup --
    global redis_client
    redis_client = redis.from_url(BROKER_URL, decode_responses=True)
    await init_db()
    
    # Menjalankan worker di background (Concurrency Proof)
    workers = [asyncio.create_task(consumer_worker(i)) for i in range(WORKER_COUNT)]
    
    yield
    
    # -- Shutdown --
    for w in workers:
        w.cancel()
    await redis_client.aclose()
    await db_pool.close()
    logger.info("System Shutdown.")

app = FastAPI(lifespan=lifespan)

# --- Endpoints ---

@app.post("/publish", status_code=202)
async def publish_event(event: EventPayload):
    """
    Menerima data dari Publisher.
    Validasi otomatis oleh Pydantic (return 422 jika format salah).
    """
    try:
        # Masukkan ke antrean Redis (Asynchronous Processing)
        # model_dump() adalah standar baru Pydantic v2
        await redis_client.lpush(QUEUE_NAME, json.dumps(event.model_dump()))
        return {"status": "queued", "event_id": event.event_id}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/events")
async def get_events(topic: Optional[str] = Query(None)):
    """
    Mengambil data tersimpan.
    Mendukung filtering: GET /events?topic=user-login
    """
    query = "SELECT event_id, topic, payload, created_at FROM processed_events"
    args = []
    
    # Filter Logic (SQL Injection Safe)
    if topic:
        query += " WHERE topic = $1"
        args.append(topic)
    
    query += " ORDER BY created_at DESC LIMIT 100"
    
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch(query, *args)
            # Konversi record object ke dict biasa agar bisa di-return JSON
            return {"data": [dict(row) for row in rows]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats")
async def get_stats():
    """Endpoint untuk monitoring dan validasi test."""
    try:
        async with db_pool.acquire() as conn:
            unique_count = await conn.fetchval("SELECT COUNT(*) FROM processed_events")
            
        dup_count = await redis_client.get(STATS_DUPLICATE_KEY)
        dup_count = int(dup_count) if dup_count else 0
        queue_len = await redis_client.llen(QUEUE_NAME)
        
        return {
            "received": unique_count + dup_count + queue_len, # Total traffic masuk
            "unique_processed": unique_count,                 # Masuk DB
            "duplicate_dropped": dup_count,                   # Ditolak DB
            "queue_backlog": queue_len,
            "status": "healthy"
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))