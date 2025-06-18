import subprocess
import threading
import time

import psycopg2
from psycopg2 import OperationalError

from utils.db_utils.db_util import DB_PARAMS


def check_db_exists(params, max_retries=5, delay=3):
    for attempt in range(1, max_retries + 1):
        try:
            conn = psycopg2.connect(**params)
            conn.close()
            print("Database is accessible!")
            return True
        except OperationalError as e:
            print(f"Attempt {attempt}: DB not accessible yet ({e})")
            time.sleep(delay)
    return False

def run_docker_compose():
    docker_compose_path = "docker"  # relative path to docker folder with docker-compose.yml

    try:
        # Run docker-compose in the correct folder
        subprocess.run(
            ["docker-compose", "up", "-d"],
            check=True,
            cwd=docker_compose_path  # set working directory so it finds docker-compose.yml
        )
        print("Docker Compose started successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error running docker-compose: {e}")
    except FileNotFoundError:
        print("docker-compose executable not found. Please install it or add it to your PATH.")


def stream_output(process, name):
    for line in iter(process.stdout.readline, b''):
        if line:
            print(f"[{name}] {line.decode().strip()}")

def run_pre_script():
    print(f"Running pre-script: postgressdb_insertion")
    result = subprocess.run(["python", "postgressdb_insertion.py"])
    if result.returncode != 0:
        raise RuntimeError(f"Pre-script postgressdb_insertion2.py failed with code {result.returncode}")
    print(f"Pre-script postgressdb_insertion2.py finished successfully.")

def run_parallel_scripts():
    # Start both processes
    process1 = subprocess.Popen(
        ["python", "nugraph_edge_file_generator.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )
    process2 = subprocess.Popen(
        ["python", "nugraph_vertext_file_generator.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT
    )

    # Start threads to stream outputs
    thread1 = threading.Thread(target=stream_output, args=(process1, "EDGE"))
    thread2 = threading.Thread(target=stream_output, args=(process2, "VERTEX"))

    thread1.start()
    thread2.start()

    # Wait for completion
    process1.wait()
    process2.wait()
    thread1.join()
    thread2.join()

    # Check exit codes
    if process1.returncode != 0:
        print("EDGE script failed.")
    if process2.returncode != 0:
        print("VERTEX script failed.")

    print("Both scripts finished.")

if __name__ == "__main__":
    if not check_db_exists(DB_PARAMS, max_retries=3, delay=5):
        run_docker_compose()
        # After starting docker-compose, wait more for DB
        if not check_db_exists(DB_PARAMS, max_retries=10, delay=5):
            print("Error: Database still not accessible after starting docker-compose.")
        else:
            print("Database is now accessible.")
    else:
        print("Database already accessible. No action needed.")
    try:
        run_pre_script()  # <-- replace with your pre script path
    except Exception as e:
        print(f"Aborting because pre-script failed: {e}")
        exit(1)

    run_parallel_scripts()