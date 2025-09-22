# dismalHAASETL
# Mason Snyder 2025

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
cnc_tables = [
    'sfcnc01', 'sfcnc02', 'sfcnc03', 'sfcnc04', 'sfcnc05', 'sfcnc06',
    'sfcnc07', 'sfcnc08', 'sfcnc09', 'sfcnc10', 'sfcnc11', 'sfcnc12',
    'sfcnc13', 'sfcnc14', 'sfcnc15', 'sfcnc16', 'sfcnc17', 'sfcnc18',
    'sfcnc19', 'sfcnc20', 'sfcnc21', 'sfcnc22', 'sfcnc23', 'sfcnc24'
]

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

                # --- Blanket table insert ---
                insert_sql = """
                    INSERT INTO cnc_alerts 
                    (machine_name, timestamp, mode, run_status, program, m30counter1, active_alarms, emergency_stop)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        timestamp = VALUES(timestamp),
                        mode = VALUES(mode),
                        run_status = VALUES(run_status),
                        program = VALUES(program),
                        m30counter1 = VALUES(m30counter1),
                        active_alarms = VALUES(active_alarms),
                        emergency_stop = VALUES(emergency_stop)
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
                    logging.info(f"Inserted/Updated blanket row for {machine_name} @ {timestamp}")
                except mysql.connector.Error as err:
                    logging.error(f"Error inserting {machine_name} into cnc_alerts: {err}")
                    continue

                # --- Individual machine table ---
                individual_table = f"cnc_{machine_name.lower()}"
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {individual_table} (
                        machine_name VARCHAR(50) PRIMARY KEY,
                        timestamp DATETIME,
                        mode VARCHAR(50),
                        run_status VARCHAR(50),
                        program VARCHAR(50),
                        m30counter1 INT,
                        active_alarms TEXT,
                        emergency_stop VARCHAR(50)
                    )
                """
                try:
                    tgt_cursor.execute(create_table_sql)
                except mysql.connector.Error as err:
                    logging.error(f"Error creating table {individual_table}: {err}")
                    continue

                # --- Create/Insert into individual machine table ---
                individual_table = f"cnc_{machine_name.lower()}"
                create_table_sql = f"""
                    CREATE TABLE IF NOT EXISTS {individual_table} (
                        machine_name VARCHAR(50) NOT NULL,
                        timestamp DATETIME NOT NULL,
                        mode VARCHAR(50) DEFAULT NULL,
                        run_status VARCHAR(50) DEFAULT NULL,
                        program VARCHAR(255) DEFAULT NULL,
                        m30counter1 INT DEFAULT NULL,
                        active_alarms TEXT,
                        emergency_stop VARCHAR(50) DEFAULT NULL,
                        PRIMARY KEY (machine_name)
                    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci
                """
                try:
                    tgt_cursor.execute(create_table_sql)
                except mysql.connector.Error as err:
                    logging.error(f"Error creating table {individual_table}: {err}")

                insert_individual_sql = f"""
                    INSERT INTO {individual_table} 
                    (machine_name, timestamp, mode, run_status, program, m30counter1, active_alarms, emergency_stop)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    ON DUPLICATE KEY UPDATE 
                        timestamp = VALUES(timestamp),
                        mode = VALUES(mode),
                        run_status = VALUES(run_status),
                        program = VALUES(program),
                        m30counter1 = VALUES(m30counter1),
                        active_alarms = VALUES(active_alarms),
                        emergency_stop = VALUES(emergency_stop)
                """

                try:
                    tgt_cursor.execute(insert_individual_sql, values)
                    tgt_conn.commit()
                    logging.info(f"Inserted/Updated row in {individual_table}")
                except mysql.connector.Error as err:
                    logging.error(f"Error inserting {machine_name} into {individual_table}: {err}")

        except Exception as e:
            logging.error(f"ETL loop failure: {e}")

        finally:
            try:
                if src_cursor: src_cursor.close()
                if src_conn: src_conn.close()
                if tgt_cursor: tgt_cursor.close()
                if tgt_conn: tgt_conn.close()
            except Exception:
                pass

        time.sleep(15)  # adjust sleep interval as needed

if __name__ == "__main__":
    logging.info("Starting continuous CNC ETL process...")
    etl_loop()
