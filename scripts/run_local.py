import os
import sys

# ===== PATH FIX =====
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS_DIR = os.path.join(BASE_DIR, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.append(SCRIPTS_DIR)

from process_stt import run_end_to_end, logger

if __name__ == "__main__":
    logger.info("Running local ETL (no Airflow)...")
    run_end_to_end()
