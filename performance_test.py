import os
import sqlite3
import mysql.connector
import subprocess
import time
import csv
import statistics
from dotenv import load_dotenv
from json_to_shards import ShardManager


load_dotenv()


def flush_os_cache_windows(dummy_file_path="huge_dummy_file"):
    with open(dummy_file_path, "rb") as f:
        while f.read(1024 * 1024):
            pass


def restart_mysql_container(container_name="credo_mysql", wait_time=5):
    """
    Restarts the Docker container running MySQL and waits for it to be ready.
    """
    print(f"🔄 Restarting MySQL container: {container_name}...")
    try:
        subprocess.run(["docker", "restart", container_name], check=True, stdout=subprocess.DEVNULL)
    except subprocess.CalledProcessError as e:
        print(f"❌ Error restarting container: {e}")
        return False

    print(f"⏳ Waiting {wait_time} seconds for MySQL to become available...")
    time.sleep(wait_time)
    return True


def restart_mysql_shards(wait_time=5):
    """
    Restarts the Docker container running MySQL and waits for it to be ready.
    """
    containers = [
        os.getenv("MYSQL_LOOKUP_CONTAINER"),
        os.getenv("MYSQL_SHARD1_CONTAINER"),
        os.getenv("MYSQL_SHARD2_CONTAINER"),
        os.getenv("MYSQL_SHARD3_CONTAINER"),
        os.getenv("MYSQL_SHARD4_CONTAINER")
    ]
    for container in containers:
        try:
            subprocess.run(["docker", "restart", container], check=True, stdout=subprocess.DEVNULL)
        except subprocess.CalledProcessError as e:
            print(f"❌ Error restarting container: {e}")
            return False
    
    print(f"⏳ Waiting {wait_time} seconds for MySQL to become available...")
    time.sleep(wait_time)
    return True


def log_results(times, stats, filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    with open(filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["Iteration", "Time (seconds)"])
        for i, t in enumerate(times, start=1):
            writer.writerow([i, f"{t:.6f}"])
        writer.writerow([])
        writer.writerow(["Average Time", f"{stats['mean']:.6f}"])
        writer.writerow(["Median Time", f"{stats['median']:.6f}"])
        writer.writerow(["Minimum Time", f"{stats['min']:.6f}"])
        writer.writerow(["Maximum Time", f"{stats['max']:.6f}"])
        writer.writerow(["Standard Deviation", f"{stats['stddev']:.6f}"])
    print(f"\n✅ Results saved to: {filename}")


def measure_performance_sqlite(query, db_path, iterations=10, output_file="results/query_times.csv"):
    """
    Measures query execution time for SQLite database over multiple iterations and logs stats.
    """
    times = []
    for i in range(iterations):
        flush_os_cache_windows()
        print(f"⏱️ Iteration {i + 1}...")

        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()

        start_time = time.time()
        cursor.execute(query)
        cursor.fetchall()
        end_time = time.time()

        elapsed = end_time - start_time
        times.append(elapsed)
        print(f"   Time: {elapsed:.6f} seconds")

        cursor.close()
        conn.close()

    stats = {
        "mean": statistics.mean(times),
        "stddev": statistics.stdev(times) if len(times) > 1 else 0.0,
        "median": statistics.median(times),
        "min": min(times),
        "max": max(times)
    }

    log_results(times, stats, output_file)


def measure_performance_mysql(query, db_config, iterations=10, output_file="results/query_times.csv"):
    """
    Measures query execution time for MySQL database over multiple iterations and logs stats.
    """
    times = []
    for i in range(iterations):
        flush_os_cache_windows()
        if not restart_mysql_container():
            print(f"⚠️ Skipping iteration {i+1} due to restart error.")
            continue
        
        print(f"⏱️ Iteration {i + 1}...")

        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        start_time = time.time()
        cursor.execute(query)
        cursor.fetchall()
        end_time = time.time()

        elapsed = end_time - start_time
        times.append(elapsed)
        print(f"   Time: {elapsed:.6f} seconds")

        cursor.close()
        conn.close()

    stats = {
        "mean": statistics.mean(times),
        "stddev": statistics.stdev(times) if len(times) > 1 else 0.0,
        "median": statistics.median(times),
        "min": min(times),
        "max": max(times)
    }

    log_results(times, stats, output_file)


def measure_performance_shards(query, lookup_db_config, shard_db_configs, iterations=10, output_file="results/query_times.csv"):
    """
    Measures query execution time for sharded MySQL database over multiple iterations and logs stats.
    """
    times = []
    for i in range(iterations):
        flush_os_cache_windows()
        if not restart_mysql_shards():
            print(f"⚠️ Skipping iteration {i+1} due to restart error.")
            continue

        print(f"⏱️ Iteration {i+1}...")

        sm = ShardManager(lookup_db_config, shard_db_configs)

        start_time = time.time()
        for conn in sm.shards.values():
            cursor = conn.cursor()
            cursor.execute(query)
            cursor.fetchall()
            cursor.close()
        end_time = time.time()

        elapsed = end_time - start_time
        times.append(elapsed)
        print(f"   Time: {elapsed:.6f} seconds")
        
        sm.close()
        del sm

    stats = {
        "mean": statistics.mean(times),
        "stddev": statistics.stdev(times) if len(times) > 1 else 0.0,
        "median": statistics.median(times),
        "min": min(times),
        "max": max(times)
    }

    log_results(times, stats, output_file)


def main():
    # MySQL config
    config_mysql = {
        'host': os.getenv("MYSQL_HOST"),
        'port': os.getenv("MYSQL_PORT"),
        'user': os.getenv("MYSQL_USER"),
        'password': os.getenv("MYSQL_PASSWORD"),
        'database': os.getenv("MYSQL_DB")
    }

    # MySQL Shards config
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

    iterations = 10
    output_file_sqlite_base = "performance_tests/time_sqlite.csv"
    output_file_sqlite_opt = "performance_tests/time_sqlite_opt.csv"
    output_file_mysql = "performance_tests/time_mysql.csv"
    output_file_mysql_shards = "performance_tests/time_shards.csv"

    query1 = """
    SELECT
        det.frame_content AS detection_frame_content,
        det.timestamp AS detection_timestamp,
        u.username AS user_username,
        u.display_name AS user_display_name,
        t.name AS user_team_name
    FROM 
        credocommon_detection det
    LEFT JOIN 
        credocommon_device d ON det.device_id = d.id
    LEFT JOIN
        credocommon_user u ON d.user_id = u.id
    LEFT JOIN 
        credocommon_team t ON u.team_id = t.id
    ORDER BY 
        det.timestamp DESC
    LIMIT 800000;
    """

    
    print("\nMeasuring query performance...\n")

    # SQLITE BASE
    measure_performance_sqlite(query1, os.getenv("DB_FILE_OG"), iterations=iterations, output_file=output_file_sqlite_base)

    # SQLITE OPTIMISED
    measure_performance_sqlite(query1, os.getenv("DB_FILE_OPT"), iterations=iterations, output_file=output_file_sqlite_opt)

    # MYSQL
    measure_performance_mysql(query1, config_mysql, iterations=iterations, output_file=output_file_mysql)

    # MYSQL SHARDS
    measure_performance_shards(query1, lookup_db_config, shard_db_configs, iterations=iterations, output_file=output_file_mysql_shards)

    print(f"Results saved to output files.")


if __name__ == "__main__":
    main()
