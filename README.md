# Airflow Assignment – STT Billing Pipeline

Project ini dibuat sebagai bagian dari technical test posisi **Data Engineer**, dengan fokus pada pembuatan pipeline otomatis menggunakan **Apache Airflow** dan **Python (SQLite + Pandas)**.

---

## 1. Overview

Pipeline ini digunakan untuk memproses dua file CSV:

- `M+ Software Airflow Assignment - STT1.csv`
- `M+ Software Airflow Assignment - STT2.csv`

Kedua file berisi data transaksi (resi/STT) yang perlu digabung dan diolah menjadi summary billing per tanggal dan client code.  
Jika ada nomor STT yang sama di kedua file, maka data dari **STT2** akan menggantikan (override) data dari **STT1**.

Hasil akhirnya berupa file CSV baru dengan format berikut:

| Date | Client Code | STT Count | Debit | Credit |
|------|--------------|-----------|--------|--------|

---

## 2. Alur Proses

Berikut tahapan proses utama dalam pipeline:

1. **Inisialisasi Database**
   - Membuat SQLite database lokal (`localdb/stt.db`).
   - Menyiapkan tabel: `stt1_raw`, `stt2_raw`, `stt_merged`, dan `billing_summary`.

2. **Load dan Cleaning CSV**
   - Membaca kedua file CSV (STT1 dan STT2).
   - Menghapus baris dengan data tidak lengkap.
   - Normalisasi kolom agar seragam: `stt`, `date`, `client_code`, `client_type`, `amount`.

3. **Stage ke Database**
   - Menyimpan hasil pembersihan data ke tabel staging (`stt1_raw`, `stt2_raw`).

4. **Merge Data**
   - Data digabung ke tabel `stt_merged`.
   - Jika STT yang sama ada di STT2, nilainya akan menggantikan data dari STT1.

5. **Aggregate ke Billing**
   - Data dikelompokkan berdasarkan `date` dan `client_code`.
   - `client_type = 'C'` dihitung ke kolom **Debit**.
   - `client_type = 'V'` dihitung ke kolom **Credit**.
   - Hitung juga jumlah STT unik untuk setiap kombinasi `date` dan `client_code`.

6. **Export Hasil**
   - Hasil akhir disimpan ke file CSV:
     ```
     output/billing_summary.csv
     ```

## 3. Struktur Project

```bash
airflow-assignment/
├── airflow_home/
│   └── dags/
│       └── stt_pipeline_dag.py         # DAG utama untuk orkestrasi di Airflow
│       └── run_local.py                # Jalankan pipeline manual tanpa Airflow
│
├── scripts/
│   └── process_stt.py                  # Script utama berisi logic ETL (clean, merge, aggregate)
│
├── tests/
│   └── test_pipeline.py                # Unit test untuk validasi end-to-end
│
├── requirements.txt                    # Daftar dependency Python
├── README.md                           # Dokumentasi project
│
└── data/
    ├── M+ Software Airflow Assignment - STT1.csv
    └── M+ Software Airflow Assignment - STT2.csv


````

---

## 4. Cara Menjalankan

### 4.1. Setup Environment

Buat virtual environment dan install dependencies:

```bash
python -m venv venv
source venv/bin/activate   # Linux / Mac
venv\Scripts\activate      # Windows

pip install -r requirements.txt
````

---

### 4.2. Jalankan Manual (tanpa Airflow)

Untuk menjalankan pipeline lokal:

```bash
python run_local.py
```

Hasilnya akan tersimpan di:

```
output/billing_summary.csv
```

---

### 4.3. Jalankan via Airflow

1. Pastikan Airflow sudah terinstall dan environment aktif.
2. Inisialisasi database Airflow:

   ```bash
   airflow db migrate
   ```
3. Jalankan webserver dan scheduler:

   ```bash
   airflow webserver
   airflow scheduler
   ```
4. Buka Airflow UI di browser (`http://localhost:8080`)
   Aktifkan dan jalankan DAG `stt_billing_pipeline`.

---

## 5. Unit Test

Gunakan `pytest` untuk memastikan pipeline bekerja sesuai ekspektasi:

```bash
pytest -q
```

Test akan memverifikasi:

* Hasil aggregate (Debit, Credit, dan STT Count) sesuai.
* Data STT2 berhasil override STT1.
* Tidak ada data kosong atau error pada proses merge dan aggregate.

---

## 6. Requirements

File `requirements.txt` berisi dependensi utama:

```
apache-airflow==2.9.1
pandas>=2.1.0,<3.0.0
python-dateutil>=2.8.2
```

---

## 7. Contoh Output

| Date       | Client Code | STT Count | Debit  | Credit  |
|-------------|-------------|-----------|--------|---------|
| 2025-11-01  | MS10001     | 5         | 143601 | 0       |
| 2025-11-01  | MS11708     | 5         | 0      | 874330  |

File hasil tersimpan otomatis di `output/billing_summary.csv`.

---

## 8. Catatan

* Seluruh proses dijalankan lewat Python atau Airflow, tanpa Excel.
* Baris dengan kolom tidak lengkap otomatis dilewati.
* SQLite digunakan untuk penyimpanan lokal dan validasi sederhana.
* Log tampil langsung di terminal untuk memudahkan debugging.

---

## 9. Ringkasan

Project ini mencakup kemampuan dasar dalam:

* Data ingestion dan cleaning
* Deduplication dan override logic
* Aggregation dan export ke CSV
* Orkestrasi menggunakan Airflow

Struktur kode dibuat sederhana dan mudah dibaca, agar reviewer dapat menjalankan pipeline tanpa konfigurasi tambahan.

---

**Author:** M. Taufiq Hariadi
**Tanggal:** November 2025

