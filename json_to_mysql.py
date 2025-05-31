import os
import json
import mysql.connector
import random
import base64
from dotenv import load_dotenv
from datetime import datetime


load_dotenv()

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


def json_to_insert(table, data, cursor, conn):
    """
        Insert data into table
    """
    data = handle_missing_fields(table, data)
    
    keys = data.keys()
    columns = ", ".join(f"`{k}`" for k in keys)
    insert_values = ", ".join(["%s" for _ in keys])
    query = f"INSERT INTO {table} ({columns}) VALUES ({insert_values})"
    try:
        cursor.execute(query, list(data.values()))
        conn.commit()
    except mysql.connector.Error as e:
        print(f"MySQL insert error on table {table}: {e}")


def insert_data_teams(directory, cursor, conn):
    filepath = os.path.join(directory, "team_mapping.json")
    if not os.path.exists(filepath):
        return

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        for entry in data.get("teams", []):
            try:
                json_to_insert("credocommon_team" , entry, cursor, conn)
            except Exception as e:
                print(f"Error inserting team: {e}")


def insert_data_users(directory, cursor, conn):
    filepath = os.path.join(directory, "user_mapping.json")
    if not os.path.exists(filepath):
        return
    
    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)
        for entry in data.get("users", []):
            try:
                user_info = {
                    "id": entry["id"],
                }
                json_to_insert("credocommon_user_info" , user_info, cursor, conn)
                json_to_insert("credocommon_user" , entry, cursor, conn)
            except Exception as e:
                print(f"Error inserting user: {e}")


def insert_data(directory, cursor, conn):
    """
        Insert data from json files in given directory
        change the line in insert for the sqlite - mysql
    """
    for filename in os.listdir(directory):
        if not filename.endswith(".json") or filename == "user_mapping.json" or filename == "team_mapping.json":
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
                            json_to_insert(f"{table_name}_info" , user_info, cursor, conn)
                            json_to_insert(table_name, entry, cursor, conn)
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
                            json_to_insert(table_name, device_info, cursor, conn)
                            json_to_insert(f"{table_name}_version", device_version, cursor, conn)
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
                                "timestamp": datetime.fromtimestamp(entry["timestamp"] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                                "time_received": datetime.fromtimestamp(entry["time_received"] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                                "visible": entry["visible"],
                                "device_id": entry["device_id"],
                            }
                            json_to_insert(f"{table_name}_info", detection_info, cursor, conn)
                            json_to_insert(table_name, detection_main, cursor, conn)
                        elif table_name == "credocommon_ping":
                            ping_info = {
                                "id": entry["id"],
                                "timestamp": datetime.fromtimestamp(entry["timestamp"] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                                "delta_time": entry["delta_time"],
                                "device_id": entry["device_id"],
                                "on_time": entry["on_time"],
                                "time_received": datetime.fromtimestamp(entry["time_received"] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
                                "metadata": entry["metadata"],
                            }
                            json_to_insert(table_name, ping_info, cursor, conn)
                    except Exception as e:
                        print(f"Error inserting data into {table_name}: {e}")


def save_image_from_blob(blob_data, detection_id, output_dir="images"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    image_path = os.path.join(output_dir, f"detection_{detection_id}.jpg")
    with open(image_path, "wb") as f:
        f.write(base64.b64decode(blob_data))
    return image_path


def process_detection_entry(entry, cursor, conn, output_dir="images"):
    """
    Process a single detection entry:
    - Extracts detection_info
    - Converts BLOB to image file and saves it
    - Inserts detection_info and detection_main (without BLOB) into respective tables
    """
    # Save image and get the relative file path
    image_path = save_image_from_blob(entry["frame_content"], entry["id"], output_dir)

    detection_main = {
        "id": entry["id"],
        "frame_path": image_path,
        "timestamp": datetime.fromtimestamp(entry["timestamp"] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
        "time_received": datetime.fromtimestamp(entry["time_received"] / 1000).strftime('%Y-%m-%d %H:%M:%S'),
        "visible": entry["visible"],
        "device_id": entry["device_id"],
        "detection_info_id": entry["id"]
    }

    # Insert into database
    json_to_insert("credocommon_detection_v2", detection_main, cursor, conn)


def insert_detections_with_image_paths(directory, cursor, conn, output_dir="images"):
    """
    Reads all JSON files and processes detection entries.
    Converts BLOBs to images and saves them locally.
    """
    for filename in os.listdir(directory):
        if not filename.endswith(".json"):
            continue

        filepath = os.path.join(directory, filename)
        with open(filepath, "r", encoding="utf-8") as file:
            json_data = json.load(file)

        if "detections" not in json_data:
            continue

        for entry in json_data["detections"]:
            try:
                process_detection_entry(entry, cursor, conn, output_dir)
            except Exception as e:
                print(f"Error processing detection {entry.get('id')}: {e}")


def main():
    config = {
        'host': os.getenv("MYSQL_HOST"),
        'port': os.getenv("MYSQL_PORT"),
        'user': os.getenv("MYSQL_USER"),
        'password': os.getenv("MYSQL_PASSWORD"),
        'database': os.getenv("MYSQL_DB")
    }
    conn = mysql.connector.connect(**config)
    cursor = conn.cursor()

    insert_data_teams(json_directory, cursor, conn)
    print("finished teams")
    insert_data_users(json_directory, cursor, conn)
    print("finished users")
    insert_data(json_directory, cursor, conn)
    print("finished rest")
    insert_data(detections_directory, cursor, conn)
    print("finished detections")
    insert_data(pings_directory, cursor, conn)
    print("finished pings")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
    