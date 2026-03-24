import mysql.connector
import json
import random
import os
from datetime import datetime, timedelta

# --- Configuration ---
MYSQL_CONFIG = {
    'host': 'localhost',
    "user": "root",
    "password": "MySQL_Student123",
    'database': 'employees'
}

# Path where JSON "API" files will be stored
JSON_STORAGE_PATH = r"D:\KSE\Data_Engineering\Airflow_project\include\telephony_mock_api"

if not os.path.exists(JSON_STORAGE_PATH):
    os.makedirs(JSON_STORAGE_PATH)


def generate_mock_data(num_calls=5):
    try:
        conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()

        # Get existing employee IDs
        cursor.execute("SELECT employee_id FROM employees")
        employee_ids = [row[0] for row in cursor.fetchall()]

        if not employee_ids:
            print("Error: No employees found in MySQL. Run the SQL insert script first.")
            return

        for _ in range(num_calls):
            # Introduce a 20% chance to generate bad data for Data Quality testing
            is_anomaly = random.random() < 0.2

            if is_anomaly:
                # Disable the "guard" (Foreign Key Checks) temporarily
                cursor.execute("SET FOREIGN_KEY_CHECKS=0;")
                emp_id = 99999  # Fake employee ID that doesn't exist
                duration = random.randint(-500, -10)  # Negative duration
            else:
                emp_id = random.choice(employee_ids)  # Real employee ID
                duration = random.randint(30, 1200)   # Normal duration

            # Prepare general call data
            call_time = datetime.now() - timedelta(minutes=random.randint(1, 60))
            phone = f"+1-{random.randint(100, 999)}-{random.randint(1000, 9999)}"
            direction = random.choice(['inbound', 'outbound'])
            status = 'completed'

            # Insert new call into MySQL
            insert_query = """
                           INSERT INTO calls (employee_id, call_time, phone, direction, status)
                           VALUES (%s, %s, %s, %s, %s)
                           """
            cursor.execute(insert_query, (emp_id, call_time, phone, direction, status))
            call_id = cursor.lastrowid

            if is_anomaly:
                cursor.execute("SET FOREIGN_KEY_CHECKS=1;")

            # Create JSON file (Mock Telephony API)
            telephony_data = {
                "call_id": call_id,
                "duration_sec": duration, # Uses the negative or positive duration from above
                "short_description": random.choice([
                    "Customer complained about billing issues.",
                    "Inquiry regarding premium plan features.",
                    "Technical support for login problems.",
                    "Request for refund processed.",
                    "General feedback about the mobile app."
                ])
            }

            # Save the JSON file
            os.makedirs(JSON_STORAGE_PATH, exist_ok=True)
            json_file_path = os.path.join(JSON_STORAGE_PATH, f"{call_id}.json")
            with open(json_file_path, 'w') as f:
                json.dump(telephony_data, f, indent=4)

        conn.commit()
        print(f"Successfully generated {num_calls} calls and JSON files.")

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

if __name__ == "__main__":
    generate_mock_data(50)