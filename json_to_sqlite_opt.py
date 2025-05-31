import os
import json
import sqlite3
import random
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

# SQLite database file
db_file_opt = os.getenv("DB_FILE_OPT")
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
    "is_superuser": "bool",
    "is_staff": "bool",
    "last_login": "datetime",
    "is_active": "bool",
    "key": "varchar",
    "team_id": "integer",
    "user_info_id": "id",
}

required_user_info_fields = {
    "first_name": "varchar",
    "last_name": "varchar",
    "date_joined": "datetime",
    "email": "varchar",
    "email_confirmation_token": "varchar",
    "language": "lang",
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
    elif data_type == "lang":
        return "en"
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
    if table == "credocommon_user_info":
        for field, field_type in required_user_info_fields.items():
            if field not in data:
                data[field] = generate_random_value(field_type, data["id"])
    elif table == "credocommon_user":
            for field, field_type in required_user_fields.items():
                if field not in data:
                    data[field] = generate_random_value(field_type, data["id"])
    elif table == "credocommon_detection":
        if "detection_info_id" not in data:
            data["detection_info_id"] = data["id"]
    elif table == "credocommon_ping":
        if "user_id" in data:
            del data["user_id"]
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
                        if table_name == "credocommon_user":
                            user_info = {
                                "id": entry["id"],
                            }
                            json_to_insert(f"{table_name}_info" , user_info, cursor)
                            json_to_insert(table_name, entry, cursor)
                        elif table_name == "credocommon_device":
                            device_info = {
                                "id": entry["id"],
                                "device_identifier": entry["id"],
                                "device_type": entry["device_type"],
                                "device_model": entry["device_model"],
                                "user_id":  entry["user_id"]
                            }
                            device_version = {
                                "id": entry["id"],
                                "device_id": entry["id"],
                                "system_version": entry["system_version"],
                                "recorded_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                            }
                            json_to_insert(table_name, device_info, cursor)
                            json_to_insert(f"{table_name}_version", device_version, cursor)
                        elif table_name == "credocommon_detection":
                            detection_info = {
                                "id": entry["id"],
                                "accuracy": entry["accuracy"],
                                "altitude": entry["altitude"],
                                "height": entry["height"],
                                "width": entry["width"],
                                "latitude": entry["latitude"],
                                "longitude": entry["longitude"],
                                "provider": entry["provider"],
                                "source": entry["source"],
                                "x": entry["x"],
                                "y": entry["y"],
                                "metadata": entry["metadata"]
                            }
                            detection_main = {
                                "id": entry["id"],
                                "frame_content": entry["frame_content"],
                                "timestamp": entry["timestamp"],
                                "time_received": entry["time_received"],
                                "visible": entry["visible"],
                                "device_id": entry["device_id"],
                            }
                            json_to_insert(f"{table_name}_info", detection_info, cursor)
                            json_to_insert(table_name, detection_main, cursor)
                        else:
                            json_to_insert(table_name, entry, cursor)
                    except sqlite3.IntegrityError as e:
                        print(f"IntegrityError for table {table_name}: {e}  ;  {entry["id"]}")
                    except Exception as e:
                        print(f"Error inserting data into {table_name}: {e}")


def main():
    conn = sqlite3.connect(db_file_opt)
    cursor = conn.cursor()

    insert_data(json_directory, cursor)
    insert_data(detections_directory, cursor)
    insert_data(pings_directory, cursor)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
    