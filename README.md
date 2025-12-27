cat << 'EOF' > README.md
# UAS Sistem Terdistribusi: Pub-Sub Log Aggregator

**Nama:** Isnaini Zayyana Fitri  
**NIM:** 11221072  
**Mata Kuliah:** Sistem Terdistribusi  

---

## 🎥 Video Demo
> **[VIDEO SEDANG DI-UPLOAD - LINK AKAN MUNCUL DI SINI]** > *(Link video akan diupdate segera setelah proses recording selesai)*

---

## 📖 Deskripsi Proyek
Sistem **Log Aggregator Terdistribusi** yang dirancang untuk menangani trafik tinggi dengan prinsip **Fault Tolerance** dan **Data Consistency**. Sistem ini menggunakan arsitektur Microservices yang diorkestrasi menggunakan Docker Compose.

### Fitur Utama
1.  **Idempotent Consumer:** Mencegah pemrosesan ganda menggunakan *Composite Unique Constraint* pada Database.
2.  **Deduplication:** Menjamin data unik (Exactly-Once Processing) meskipun Publisher mengirim data berulang kali (*Retry Storm*).
3.  **Data Persistence:** Menggunakan Docker Volumes (`pg_data`) untuk menjamin data aman meskipun container crash/restart.
4.  **Asynchronous Processing:** Menggunakan Redis sebagai Message Broker untuk *decoupling* antara Publisher dan Aggregator.

---

## 🛠️ Teknologi yang Digunakan
* **Bahasa:** Python 3.11 (FastAPI)
* **Broker:** Redis 7 (Alpine)
* **Database:** PostgreSQL 16 (Alpine)
* **Infrastructure:** Docker & Docker Compose
* **Testing:** Pytest

---

## 🚀 Cara Menjalankan Aplikasi

Pastikan Docker Desktop sudah berjalan, lalu ikuti perintah ini:

**1. Clone Repository**
\`\`\`bash
git clone https://github.com/zayfitri/uas-sister-log-aggregator.git
cd uas-sister-log-aggregator
\`\`\`

**2. Jalankan Sistem (Build & Run)**
\`\`\`bash
docker compose up --build -d
\`\`\`

**3. Cek Status Container**
Pastikan semua container (aggregator, publisher, broker, storage) berstatus \`Up\` / \`Healthy\`.
\`\`\`bash
docker compose ps
\`\`\`

---

## 📡 API Endpoints & Monitoring

| Method | Endpoint | Deskripsi |
| :--- | :--- | :--- |
| \`POST\` | \`/publish\` | Mengirim log event baru (JSON) |
| \`GET\` | \`/stats\` | Melihat statistik (Received vs Unique Processed) |
| \`GET\` | \`/events\` | Melihat daftar data yang berhasil disimpan |

**Akses Cepat:**
* **Statistik:** [http://localhost:8080/stats](http://localhost:8080/stats)
* **Swagger UI:** [http://localhost:8080/docs](http://localhost:8080/docs)

---

## ✅ Skenario Pengujian (Testing)

Untuk menjalankan *Unit Test* guna memvalidasi logika deduplikasi:

\`\`\`bash
# Jika dijalankan dari luar container (pastikan library terinstall)
pytest

# ATAU jalankan dari dalam container (Rekomendasi)
docker compose exec aggregator pytest
\`\`\`
EOF