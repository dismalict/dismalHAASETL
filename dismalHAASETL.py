#dismalHAASETL
#Mason Snyder 2025

import mysql.connector
from datetime import datetime
import logging
import time

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --- Source DB (sfmysql01) ---
source_config = {
    "host": "sfmysql01.sf.local",
    "port": 3306,
    "user": "etl_user",
    "password": "8%gfWi7G2X$9EL",
    "database": "sfcncpool"
}

# --- Target DB (sfmysql03) ---
target_config = {
    "host": "sfmysql03.sf.local",
    "port": 3306,
    "user": "etl_user",
    "password": "8%gfWi7G2X$9EL",
    "database": "alerting"
}

# List all CNC tables you want to pull from
cnc_tables = ["sfcnc01", "sfcnc02", "sfcnc03"]  # add all machines here

def etl_loop():
    while True:
        try:
            src_conn = mysql.connector.connect(**source_config)
            src_cursor = src_conn.cursor(dictionary=True)

            tgt_conn = mysql.connector.connect(**target_config)
            tgt_cursor = tgt_conn.cursor()

            for table in cnc_tables:
                src_cursor.execute(f"""
                    SELECT Timestamp, Mode, RunStatus, Program, M30Counter1, ActiveAlarms, EmergencyStop
                    FROM {table}
                    ORDER BY Timestamp DESC
                    LIMIT 1
                """)
                row = src_cursor.fetchone()
                if not row:
                    logging.warning(f"No data found in {table}")
                    continue

                machine_name = table
                timestamp = row["Timestamp"]

                insert_sql = """
                    INSERT INTO cnc_alerts 
                    (machine_name, timestamp, mode, run_status, program, m30counter1, active_alarms, emergency_stop)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE timestamp=timestamp
                """
                values = (
                    machine_name,
                    timestamp,
                    row["Mode"],
                    row["RunStatus"],
                    row["Program"],
                    row["M30Counter1"],
                    row["ActiveAlarms"],
                    row["EmergencyStop"]
                )

                try:
                    tgt_cursor.execute(insert_sql, values)
                    tgt_conn.commit()
                    logging.info(f"Inserted/Skipped row for {machine_name} @ {timestamp}")
                except mysql.connector.Error as err:
                    logging.error(f"Error inserting {machine_name}: {err}")

        except Exception as e:
            logging.error(f"ETL loop failure: {e}")

        finally:
            if src_cursor:
                src_cursor.close()
            if src_conn:
                src_conn.close()
            if tgt_cursor:
                tgt_cursor.close()
            if tgt_conn:
                tgt_conn.close()

        time.sleep(15)  # adjust sleep interval as needed

if __name__ == "__main__":
    logging.info("Starting continuous CNC ETL process...")
    etl_loop()
