import os
import sqlite3
import logging
from datetime import datetime
import pandas as pd

# =========================================================
# Setup basic logger
# =========================================================
def _setup_logger():
    logger = logging.getLogger("stt_pipeline")
    logger.setLevel(logging.INFO)
    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        ch = logging.StreamHandler()
        ch.setFormatter(fmt)
        logger.addHandler(ch)
    return logger

logger = _setup_logger()

# =========================================================
# Path setup
# =========================================================
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_DIR = os.path.join(BASE_DIR, "output")
DB_DIR = os.path.join(BASE_DIR, "localdb")
DB_PATH = os.path.join(DB_DIR, "stt.db")

# =========================================================
# CSV → DataFrame
# =========================================================
def read_and_clean_csv(file_path: str) -> pd.DataFrame:
    """
    Membaca file CSV, membersihkan data yang tidak lengkap,
    dan memastikan nama serta tipe kolom seragam.
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File tidak ditemukan: {file_path}")

    df = pd.read_csv(file_path)
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

    # Map 'number' ke 'stt' jika perlu
    if 'number' in df.columns and 'stt' not in df.columns:
        df = df.rename(columns={'number': 'stt'})

    required_cols = ['stt', 'date', 'client_code', 'client_type', 'amount']
    missing_cols = [col for col in required_cols if col not in df.columns]
    if missing_cols:
        raise KeyError(f"Kolom berikut wajib ada di CSV: {missing_cols}")

    # Drop baris dengan nilai kosong di kolom penting
    df = df.dropna(subset=required_cols)

    # Normalisasi tipe data
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    df = df.dropna(subset=['date'])
    df['amount'] = pd.to_numeric(df['amount'], errors='coerce').fillna(0)

    df['client_type'] = df['client_type'].astype(str).str.upper().str.strip()
    df['stt'] = df['stt'].astype(str).str.strip()
    df['client_code'] = df['client_code'].astype(str).str.strip()

    logger.info(f"Read OK: {os.path.basename(file_path)} -> {len(df)} rows")
    return df

# =========================================================
# SQLite helpers
# =========================================================
def get_conn(db_path: str = DB_PATH):
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    return sqlite3.connect(db_path)

def init_db():
    """Membuat struktur tabel SQLite yang dibutuhkan untuk pipeline."""
    with get_conn() as conn:
        cur = conn.cursor()
        # Tabel staging
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stt1_raw (
                stt TEXT,
                date TEXT,
                client_code TEXT,
                client_type TEXT,
                amount REAL
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stt2_raw (
                stt TEXT,
                date TEXT,
                client_code TEXT,
                client_type TEXT,
                amount REAL
            )
        """)
        # Tabel gabungan hasil deduplikasi STT
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stt_merged (
                stt TEXT PRIMARY KEY,
                date TEXT,
                client_code TEXT,
                client_type TEXT,
                amount REAL
            )
        """)
        # Tabel hasil akhir
        cur.execute("""
            CREATE TABLE IF NOT EXISTS billing_summary (
                date TEXT,
                client_code TEXT,
                stt_count INTEGER,
                debit INTEGER,
                credit INTEGER
            )
        """)
        conn.commit()
    logger.info("Database initialized and tables are ready")

def _df_to_table(df: pd.DataFrame, table: str):
    """Helper untuk insert DataFrame ke SQLite table."""
    with get_conn() as conn:
        df2 = df.copy()
        df2['date'] = df2['date'].dt.strftime("%Y-%m-%d")
        df2.to_sql(table, conn, if_exists="append", index=False)
    logger.info(f"Inserted {len(df)} rows into {table}")

def stage_csv_to_db(stt_csv_path: str, target_table: str):
    """Membaca file CSV, membersihkan, lalu masukkan ke tabel staging."""
    df = read_and_clean_csv(stt_csv_path)
    _df_to_table(df, target_table)

def merge_stage_to_unique():
    """
    Menggabungkan data dari STT1 dan STT2 ke tabel final:
      - STT2 menimpa STT1 jika ada nomor STT yang sama.
      - Gunakan REPLACE INTO agar aman di SQLite.
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM stt_merged")

        # Insert dari STT1, satu baris per STT
        cur.execute("""
            INSERT INTO stt_merged (stt, date, client_code, client_type, amount)
            SELECT stt, date, client_code, client_type, amount
            FROM (
                SELECT stt, date, client_code, client_type, amount,
                       ROW_NUMBER() OVER (PARTITION BY stt ORDER BY date DESC) AS rn
                FROM stt1_raw
            ) tmp
            WHERE rn = 1
        """)

        # STT2 override data yang sama dari STT1
        cur.execute("""
            REPLACE INTO stt_merged (stt, date, client_code, client_type, amount)
            SELECT stt, date, client_code, client_type, amount
            FROM (
                SELECT stt, date, client_code, client_type, amount,
                       ROW_NUMBER() OVER (PARTITION BY stt ORDER BY date DESC) AS rn
                FROM stt2_raw
            ) tmp
            WHERE rn = 1
        """)
        conn.commit()

    logger.info("Merge complete: data dari STT1 & STT2 sudah digabung")

def aggregate_to_billing():
    """
    Membuat summary per tanggal dan client_code.
    - client_type 'C' → total masuk di kolom debit
    - client_type 'V' → total masuk di kolom credit
    - hitung juga jumlah STT unik per kombinasi date + client_code
    """
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM billing_summary")
        conn.commit()

        pairs = pd.read_sql_query("""
            SELECT date, client_code FROM stt_merged GROUP BY date, client_code
        """, conn)

        if pairs.empty:
            logger.info("Tidak ada data untuk diaggregate")
            return

        rows = []
        for _, r in pairs.iterrows():
            date_ = r["date"]
            code_ = r["client_code"]

            debit = pd.read_sql_query("""
                SELECT COALESCE(SUM(amount),0) AS v
                FROM stt_merged
                WHERE date = ? AND client_code = ? AND client_type = 'C'
            """, conn, params=[date_, code_]).iloc[0]["v"]

            credit = pd.read_sql_query("""
                SELECT COALESCE(SUM(amount),0) AS v
                FROM stt_merged
                WHERE date = ? AND client_code = ? AND client_type = 'V'
            """, conn, params=[date_, code_]).iloc[0]["v"]

            stt_count = pd.read_sql_query("""
                SELECT COUNT(DISTINCT stt) AS c
                FROM stt_merged
                WHERE date = ? AND client_code = ?
            """, conn, params=[date_, code_]).iloc[0]["c"]

            rows.append((date_, code_, int(stt_count), int(debit), int(credit)))

        cur.executemany("""
            INSERT INTO billing_summary (date, client_code, stt_count, debit, credit)
            VALUES (?, ?, ?, ?, ?)
        """, rows)
        conn.commit()
    logger.info(f"Aggregate done: {len(rows)} rows inserted")

def export_billing_to_csv(output_path: str):
    """Ekspor hasil summary ke file CSV output."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with get_conn() as conn:
        df = pd.read_sql_query("""
            SELECT date, client_code, stt_count AS "STT Count",
                   debit AS Debit, credit AS Credit
            FROM billing_summary
            ORDER BY date, client_code
        """, conn)
    df.to_csv(output_path, index=False)
    logger.info(f"Hasil disimpan di: {output_path}")

# =========================================================
# Fungsi legacy-friendly (untuk DAG & testing)
# =========================================================
def merge_and_clean_data(stt1_path: str, stt2_path: str) -> pd.DataFrame:
    """Versi non-DB untuk kebutuhan logging atau XCom di DAG."""
    df1 = read_and_clean_csv(stt1_path)
    df2 = read_and_clean_csv(stt2_path)
    combined = pd.concat([df1, df2], ignore_index=True)
    combined = combined.sort_values(by=['stt', 'date'], ascending=[True, True])
    combined = combined.drop_duplicates(subset=['stt'], keep='last')
    return combined

def aggregate_billing(df: pd.DataFrame) -> pd.DataFrame:
    """Versi pandas jika ingin aggregate tanpa SQLite."""
    agg_df = (
        df.groupby(['date', 'client_code', 'client_type'])
          .agg(total_amount=('amount', 'sum'), stt_count=('stt', 'count'))
          .reset_index()
    )
    debit_df = agg_df[agg_df['client_type'] == 'C'][['date', 'client_code', 'stt_count', 'total_amount']]
    debit_df = debit_df.rename(columns={'total_amount': 'Debit'})

    credit_df = agg_df[agg_df['client_type'] == 'V'][['date', 'client_code', 'stt_count', 'total_amount']]
    credit_df = credit_df.rename(columns={'total_amount': 'Credit'})

    result = pd.merge(debit_df, credit_df, on=['date', 'client_code'], how='outer').fillna(0)
    result['Debit'] = result['Debit'].astype(int)
    result['Credit'] = result['Credit'].astype(int)
    result['stt_count_x'] = result['stt_count_x'].fillna(0).astype(int)
    result['stt_count_y'] = result['stt_count_y'].fillna(0).astype(int)
    result['STT Count'] = result[['stt_count_x', 'stt_count_y']].max(axis=1)
    result = result[['date', 'client_code', 'STT Count', 'Debit', 'Credit']].sort_values(by=['date', 'client_code'])
    return result

def save_output(df: pd.DataFrame, output_path: str):
    """Simpan hasil ke file CSV."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    logger.info(f"Hasil disimpan di: {output_path}")

# =========================================================
# End-to-end (manual run / unit test)
# =========================================================
def run_end_to_end():
    """Pipeline lengkap: baca CSV, simpan ke DB, merge, aggregate, dan ekspor."""
    stt1_path = os.path.join(DATA_DIR, 'M+ Software Airflow Assignment - STT1.csv')
    stt2_path = os.path.join(DATA_DIR, 'M+ Software Airflow Assignment - STT2.csv')
    output_path = os.path.join(OUTPUT_DIR, 'billing_summary.csv')

    logger.info("Start ETL (SQLite mode)")
    init_db()
    stage_csv_to_db(stt1_path, "stt1_raw")
    stage_csv_to_db(stt2_path, "stt2_raw")
    merge_stage_to_unique()
    aggregate_to_billing()
    export_billing_to_csv(output_path)
    logger.info("ETL selesai tanpa error")

def main():
    run_end_to_end()

if __name__ == "__main__":
    main()
