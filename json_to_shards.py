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
    return data


class ShardManager:
    def __init__(self, lookup_config, shard_configs):
        # Connect to lookup DB
        self.lookup_conn = mysql.connector.connect(**lookup_config)
        self.lookup_cursor = self.lookup_conn.cursor(dictionary=True)
        
        # Connect to shards
        self.shards = {}
        for shard_id, config in shard_configs.items():
            conn = mysql.connector.connect(**config)
            self.shards[shard_id] = conn


    def get_shard_for_user(self, user_id):
        query = "SELECT shard_id FROM user_shard WHERE user_id = %s"
        self.lookup_cursor.execute(query, (user_id,))
        result = self.lookup_cursor.fetchone()
        if result:
            return result['shard_id']
        else:
            raise Exception(f"No shard mapping found for user_id={user_id}")


    def insert_user_shard_mapping(self, user_id, shard_id):
        cursor = self.lookup_conn.cursor()
        try:
            sql = "INSERT INTO user_shard (user_id, shard_id) VALUES (%s, %s)"
            cursor.execute(sql, (user_id, shard_id))
            self.lookup_conn.commit()
        except mysql.connector.Error as e:
            print(f"Error inserting shard mapping for user {user_id}: {e}")
            raise
        finally:
            cursor.close()


    def insert_generic(self, table, data, user_id=None):
        data = handle_missing_fields(table, data)
        keys = data.keys()
        columns = ", ".join(f"`{k}`" for k in keys)
        insert_values = ", ".join(["%s" for _ in keys])
        query = f"INSERT INTO {table} ({columns}) VALUES ({insert_values})"

        if user_id:
            shard_id = self.get_shard_for_user(user_id)
            conn = self.shards[shard_id]

        cursor = conn.cursor()
        try:
            cursor.execute(query, list(data.values()))
            conn.commit()
        except mysql.connector.Error as e:
            print(f"MySQL insert error on table {table}: {e} with data: {data}")
        finally:
            cursor.close()


    def close(self):
        self.lookup_cursor.close()
        self.lookup_conn.close()
        for conn in self.shards.values():
            try:
                conn.cursor().close()
            except:
                pass
            conn.close()


def insert_data_teams(sm: ShardManager, directory):
    """
        Insert teams from json files in given directory to all shards.
    """    
    filepath = os.path.join(directory, "team_mapping.json")
    if not os.path.exists(filepath):
        return

    with open(filepath, "r", encoding="utf-8") as f:
        data = json.load(f)

        for entry in data.get("teams", []):
            for shard_id, conn in sm.shards.items():
                try:
                    cursor = conn.cursor()
                    data = handle_missing_fields("credocommon_team", entry)
                    keys = data.keys()
                    columns = ", ".join(keys)
                    insert_values = ", ".join(["%s"] * len(keys))
                    query = f"INSERT INTO credocommon_team ({columns}) VALUES ({insert_values})"
                    cursor.execute(query, list(data.values()))
                    conn.commit()
                    cursor.close()
                except Exception as e:
                    print(f"Error inserting team {entry['id']} into shard {shard_id}: {e}")


def insert_data_users(sm: ShardManager, directory):
    """
        Insert users from json files in given directory.
        Users are split to shards based on round-robin assignment,
    """    
    user_mapping_path = os.path.join(directory, "user_mapping.json")
    if os.path.exists(user_mapping_path):
        with open(user_mapping_path, "r", encoding="utf-8") as f:
            user_json = json.load(f)
            if "users" in user_json:
                shard_ids = list(sm.shards.keys())
                for i, entry in enumerate(user_json["users"]):
                    try:
                        user_id = entry["id"]
                        shard_id = shard_ids[i % len(shard_ids)]

                        sm.insert_user_shard_mapping(user_id, shard_id)

                        user_info = {"id": user_id}
                        sm.insert_generic("credocommon_user_info", user_info, user_id=user_id)
                        sm.insert_generic("credocommon_user", entry, user_id=user_id)
                    except Exception as e:
                        print(f"Error inserting user {entry.get('id')}: {e}")



def insert_data(sm: ShardManager, directory):
    """
        Insert data from json files in given directory to rest of the tables.
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
                        if table_name == "credocommon_device":
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
                            sm.insert_generic(table_name, device_info, user_id=entry["user_id"])
                            sm.insert_generic(f"{table_name}_version", device_version, user_id=entry["user_id"])
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
                            user_id_for_shard = entry["user_id"]
                            sm.insert_generic(f"{table_name}_info", 
                            detection_info, user_id=user_id_for_shard)
                            sm.insert_generic(table_name, detection_main, user_id=user_id_for_shard)
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
                            sm.insert_generic(table_name, ping_info, user_id=entry["user_id"])
                    except Exception as e:
                        print(f"Error inserting data into {table_name}: {e}")


def save_image_from_blob(blob_data, detection_id, output_dir="images"):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
    image_path = os.path.join(output_dir, f"detection_{detection_id}.jpg")
    with open(image_path, "wb") as f:
        f.write(base64.b64decode(blob_data))
    return image_path


def process_sharded_detection_entry(entry, sm: ShardManager, output_dir="images"):
    """
    Process a single detection entry:
    - Saves image from BLOB
    - Prepares detection record
    - Inserts into sharded DB
    """
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

    sm.insert_generic("credocommon_detection_v2", detection_main, user_id=entry["user_id"])


def insert_detections_with_paths_sharded(sm: ShardManager, directory, image_dir="images"):
    """
    Reads all JSON files and processes detection entries for sharded DB.
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
                process_sharded_detection_entry(entry, sm, image_dir)
            except Exception as e:
                print(f"Error processing detection {entry.get('id')}: {e}")


def main():
    lookup_db_config = {
        'host': os.getenv("MYSQL_HOST"),
        'port': os.getenv("MYSQL_LOOKUP_PORT"),
        'user': os.getenv("MYSQL_USER"),
        'password': os.getenv("MYSQL_PASSWORD"),
        'database': os.getenv("MYSQL_LOOKUP_DB")
    }

    shard_db_configs = {
        1: {'host': os.getenv("MYSQL_HOST"), 'port': os.getenv("MYSQL_SHARD1_PORT"), 'user': os.getenv("MYSQL_USER"), 'password': os.getenv("MYSQL_PASSWORD"), 'database': os.getenv("MYSQL_SHARD1_DB")},
        2: {'host': os.getenv("MYSQL_HOST"), 'port': os.getenv("MYSQL_SHARD2_PORT"), 'user': os.getenv("MYSQL_USER"), 'password': os.getenv("MYSQL_PASSWORD"), 'database': os.getenv("MYSQL_SHARD2_DB")},
        3: {'host': os.getenv("MYSQL_HOST"), 'port': os.getenv("MYSQL_SHARD3_PORT"), 'user': os.getenv("MYSQL_USER"), 'password': os.getenv("MYSQL_PASSWORD"), 'database': os.getenv("MYSQL_SHARD3_DB")},
        4: {'host': os.getenv("MYSQL_HOST"), 'port': os.getenv("MYSQL_SHARD4_PORT"), 'user': os.getenv("MYSQL_USER"), 'password': os.getenv("MYSQL_PASSWORD"), 'database': os.getenv("MYSQL_SHARD4_DB")},
    }
    sm = ShardManager(lookup_db_config, shard_db_configs)

    insert_data_teams(sm, json_directory)
    print("finished teams")
    insert_data_users(sm, json_directory)
    print("finished users")
    insert_data(sm, json_directory)
    print("finished rest")
    insert_data(sm, detections_directory)
    print("finished detections")
    insert_data(sm, pings_directory)
    print("finished pings")

    sm.close()


if __name__ == "__main__":
    main()
    