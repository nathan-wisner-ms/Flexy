import time
from multiprocessing import Process, Queue
import argparse
import helper
import argparse
from datetime import datetime
import csv
import logging
import sys

import logging
import sys
import uuid

logging.basicConfig(
    level=logging.DEBUG,
    format="[%(asctime)s] [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(f'migration_log_{datetime.now().strftime("%Y-%m-%d")}'),
        logging.StreamHandler(sys.stdout),
    ],
)

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--config-file",
    help="File Path of Source and Target Database Configuration, default: config.ini",
    required=True,
)
parser.add_argument(
    "-p",
    "--parallel-number",
    help="number of parallel threads, default: 20",
    type=int,
    required=True,
)
parser.add_argument(
    "-q", "--queue-file", help="File Path of tables for migration", required=True
)
args = parser.parse_args()


tasks_queue = Queue()
project_id = ""
job_number_dict = {}

MIGRATION_CONFIG = helper.build_config(args.config_file)
SOURCE_CONN_URL = helper.build_connection_string(MIGRATION_CONFIG["source"])
TARGET_CONN_URL = helper.build_connection_string(MIGRATION_CONFIG["target"])

target_connection_pool = helper.build_db_connection_pool(
    "target", MIGRATION_CONFIG["target"]
)

def execute_tasks(thread):
    global tasks_queue
    global project_id
    global target_connection_pool
    global job_id_dict

    num_thread_jobs = 0
    while not tasks_queue.empty():
        thread_start_time = time.time()
        message = tasks_queue.get()
        num_thread_jobs += 1
        job_number_dict[message] = len(job_number_dict) + 1
        helper.logging_thread(f"Pick up job number: {job_number_dict[message]} message: {message}", thread)
        for i in range(1, 4):
            start_time = time.time()
            try:
                helper.log_migration_jobs_status(message, "started", None)
                if "|" in message:
                    helper.migrate_copy_by_partition(
                        thread,
                        message,
                        SOURCE_CONN_URL,
                        TARGET_CONN_URL,
                        target_connection_pool,
                    )
                else:
                    helper.migrate_pg_dump_restore(
                        thread,
                        message,
                        SOURCE_CONN_URL,
                        TARGET_CONN_URL,
                        target_connection_pool,
                    )
                duration = helper.get_duration(start_time)
                helper.log_migration_jobs_status(message, "success", duration)
                helper.logging_thread(
                    f"Sent data for table: {message}, time took: {duration}.",
                    thread,
                )
                break
            except Exception as err:
                logging.error(err)
                duration = helper.get_duration(start_time)
                helper.log_migration_jobs_status(message, "failure", duration)
                helper.logging_thread(
                    f"Sleep 30 seconds for attempt {i+1} for {message} ...", thread
                )
                time.sleep(30)
    helper.logging_thread(
        f"No more job in queue, this thread process completed. Total jobs executed: {num_thread_jobs}, total time worked: {u.get_duration(thread_start_time)}",
        thread,
    )


def main():
    global tasks_queue
    global project_id

    migration_jobs = []
    logging.debug("Validating migraition tables queue file ...")
    try:
        migration_jobs = helper.parse_table_files(args.queue_file)
        if not migration_jobs:
            logging.error("Invalid tables to migrate. Exit.")
            quit()
    except:
        logging.error("Invalid file.")
        quit()

    migration_jobs_success = set()
    try:
        f = open("migration_jobs_status.tsv", "r")
        rows = list(csv.DictReader(f, delimiter="\t"))
        for row in rows:
            if row["status"] == "success":
                migration_jobs_success.add(row["message"])
        f.close()
    except Exception as error:
        logging.error(error)
        f = open("migration_jobs_status.tsv", "a+")
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(
            [
                "message",
                "status",
                "logged_at",
                "duration"
            ]
        )
        f.close()
    num_pending_jobs = 0
    for job in migration_jobs:
        if job in migration_jobs_success:
            logging.debug(f"Skip migration job: {job} - already success.")
        else:
            num_pending_jobs += 1
            logging.debug(f"Queue up migration job: {job}")
            tasks_queue.put(job)

    if num_pending_jobs > 0:
        logging.debug(f"{num_pending_jobs} migration job(s) pending in queue.")
    else:
        logging.debug("No migration job is needed. Exit.")
        quit()
    
    project_id = str(uuid.uuid4())
    start_time = time.time()
    logging.info(f"============== Start migration project: {project_id} ========== ")
    procs = [
        Process(target=execute_tasks, args=(i + 1,))
        for i in range(min(args.parallel_number, num_pending_jobs))
    ]
    for p in procs:
        p.start()
    for p in procs:
        p.join()

    logging.info(
        f"============== End of migration project: {project_id}. Total time took: {helper.get_duration(start_time)}. ============== "
    )


if __name__ == "__main__":
    main()
