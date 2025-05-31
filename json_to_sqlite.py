import os
import json
import sqlite3
import random
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

# SQLite database file
db_file_og = os.getenv("DB_FILE_OG")
json_directory = os.getenv("JSON_DIRECTORY")

# Mapping JSON keys to table names
table_mapping = {
    "teams": "credocommon_team",
    "devices": "credocommon_device",
    "users": "credocommon_user",
    "detections": "credocommon_detection",
    "pings": "credocommon_ping",
}

detections_directory = f"{json_directory}/detections/"
pings_directory = f"{json_directory}/pings/"

required_user_fields = {
    "password": "varchar",
    "last_login": "datetime",
    "is_superuser": "bool",
    "first_name": "varchar",
    "is_staff": "bool",
    "is_active": "bool",
    "date_joined": "datetime",
    "key": "varchar",
    "email": "varchar",
    "email_confirmation_token": "varchar",
    "language": "varchar",
    "team_id": "integer",
    "last_name": "varchar"
}


def generate_random_value(data_type, id=None):
    """
    Generate random or default values based on the column type.
    Optionally include `id` in the random varchar values to satisfy unique constraint.
    """
    if data_type == "bool":
        return random.choice([True, False])
    elif data_type == "datetime":
        return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    elif data_type == "varchar":
        return f"random_{id}_{random.randint(1000, 9999)}" if id else f"random_{random.randint(1000, 9999)}"
    elif data_type == "integer":
        return random.randint(1, 1000)
    elif data_type == "id":
        return id
    return None


def handle_missing_fields(table, data):
    """
    Handle missing fields for specific tables by adding default or random values.
    Optionally include `id` in random varchar fields to satisfy unique constraint.
    """
    if table == "credocommon_device":
        if "device_id" not in data:
            data["device_id"] = data["id"]
    elif table == "credocommon_user":
        for field, field_type in required_user_fields.items():
            if field not in data:
                data[field] = generate_random_value(field_type, data["id"])
    return data


def json_to_insert(table, data, cursor):
    """
        Insert data into table
    """
    data = handle_missing_fields(table, data)
    
    keys = data.keys()
    columns = ", ".join(keys)
    insert_values = ", ".join(["?" for _ in keys])
    query = f"INSERT INTO {table} ({columns}) VALUES ({insert_values})"
    cursor.execute(query, list(data.values()))


def insert_data(directory, cursor):
    """
        Insert data from json files in given directory
        change the line in insert for the sqlite - mysql
    """
    for filename in os.listdir(directory):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(directory, filename)
        with open(filepath, "r", encoding="utf-8") as file:
            json_data = json.load(file)

            for key, table_name in table_mapping.items():
                if key not in json_data:
                    continue
                for entry in json_data[key]:
                    try:
                        json_to_insert(table_name, entry, cursor)
                    except sqlite3.IntegrityError as e:
                        print(f"IntegrityError for table {table_name}: {e}")
                    except Exception as e:
                        print(f"Error inserting data into {table_name}: {e}")


def main():
    conn = sqlite3.connect(db_file_og)
    cursor = conn.cursor()

    insert_data(json_directory, cursor)
    insert_data(detections_directory, cursor)
    insert_data(pings_directory, cursor)

    conn.commit()
    conn.close()



if __name__ == "__main__":
    main()
    