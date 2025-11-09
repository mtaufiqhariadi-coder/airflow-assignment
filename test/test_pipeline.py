import os
import shutil
import sqlite3
import pandas as pd
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from process_stt import (
    DB_PATH, DB_DIR, init_db, stage_csv_to_db,
    merge_stage_to_unique, aggregate_to_billing
)

def _reset_db():
    if os.path.exists(DB_DIR):
        shutil.rmtree(DB_DIR)
    os.makedirs(DB_DIR, exist_ok=True)

def _write_csv(path, rows, columns):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    pd.DataFrame(rows, columns=columns).to_csv(path, index=False)

def test_end_to_end_tmp(tmp_path):
    # Arrange
    base = tmp_path
    data_dir = base / "data"
    out_dir = base / "output"
    localdb = base / "localdb" / "stt.db"

    os.makedirs(data_dir, exist_ok=True)
    os.makedirs(out_dir, exist_ok=True)
    os.makedirs(localdb.parent, exist_ok=True)

    # CSV minimal
    stt1 = data_dir / "STT1.csv"
    stt2 = data_dir / "STT2.csv"

    _write_csv(
        str(stt1),
        rows=[
            ["A001", "2025-11-01", "CL001", "C", 1000],
            ["A002", "2025-11-01", "CL001", "V", 200],
            ["A003", "2025-11-02", "CL002", "C", 300],
        ],
        columns=["stt", "date", "client_code", "client_type", "amount"]
    )
    # Override A002 + tambah baris baru
    _write_csv(
        str(stt2),
        rows=[
            ["A002", "2025-11-01", "CL001", "V", 500],  # override amount 200 -> 500
            ["A004", "2025-11-02", "CL002", "V", 100],
        ],
        columns=["stt", "date", "client_code", "client_type", "amount"]
    )

    # Patch path DB sementara via env (sqlite3 pakai argumen path absolut di test)
    # -> copy-paste fungsi dengan path override (sederhana: symlink file setelah selesai)
    # Di test ini, kita langsung manipulasi DB_PATH via symlink.
    # 1) Inisialisasi DB
    conn = sqlite3.connect(str(localdb))
    conn.close()

    # Jalankan pipeline manual tapi diarahkan ke DB test:
    # Trik: buat symlink/relokasi DB default ke DB test
    # (Windows mungkin perlu copy; fallback: hapus DB default lalu link file)
    default_db_dir = os.path.dirname(DB_PATH)
    os.makedirs(default_db_dir, exist_ok=True)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    # Copy kosong
    shutil.copyfile(str(localdb), DB_PATH)

    # Act
    init_db()
    stage_csv_to_db(str(stt1), "stt1_raw")
    stage_csv_to_db(str(stt2), "stt2_raw")
    merge_stage_to_unique()
    aggregate_to_billing()

    # Assert
    with sqlite3.connect(DB_PATH) as conn2:
        df = pd.read_sql_query("SELECT * FROM billing_summary ORDER BY date, client_code", conn2)

    # Cek override: A002 harus hitung credit = 500 (bukan 200)
    row = df[(df["date"]=="2025-11-01") & (df["client_code"]=="CL001")].iloc[0]
    assert row["credit"] == 500
    # Debit di 2025-11-01 untuk CL001 = 1000 (dari A001)
    assert row["debit"] == 1000
    # STT count utk 2025-11-01, CL001 = 2 (A001 & A002)
    assert row["stt_count"] == 2

    # Cek tanggal 2025-11-02, CL002: debit=300 (A003), credit=100 (A004), stt_count=2
    row2 = df[(df["date"]=="2025-11-02") & (df["client_code"]=="CL002")].iloc[0]
    assert row2["debit"] == 300
    assert row2["credit"] == 100
    assert row2["stt_count"] == 2
