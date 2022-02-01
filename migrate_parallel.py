from cmath import log
import time
from multiprocessing import Process, Queue
import argparse
import flexy_helper as h
import argparse
import logging
import uuid
from datetime import datetime
import csv

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--config-file",
    help="File Path of Source and Target Database Configuration",
    default="config.ini",
)
parser.add_argument(
    "-n",
    "--number-thread",
    help="number of parallel threads, default: 10",
    type=int,
    default=10,
)
parser.add_argument(
    "-q", "--queue-file", help="File Path of tables for migration", required=True
)
parser.add_argument(
    "-r", "--replication",
    help="Sync with logical replication",
    choices=("True", "False"),
    default=False,
)
args = parser.parse_args()


log_file = f'migration_logs_migrate_parallel_{datetime.now().strftime("%Y_%m_%d_%H_%M")}'
h.setup_logging(log_file)
logging.debug(f"Writing logs to file: {log_file}")

migration_queue = Queue()
project_id = ""

MIGRATION_CONFIG = h.build_config(args.config_file)

def execute_tasks(thread, snapshot_name):
    global migration_queue
    global project_id

    num_thread_jobs = 0
    thread_start_time = time.time()
    while not migration_queue.empty():
        message = migration_queue.get()
        num_thread_jobs += 1
        h.logging_thread(
            f"Pick up migration job {num_thread_jobs}: {message}",
            thread,
        )
        h.execute_migration_job(thread, message, MIGRATION_CONFIG, snapshot_name)

    h.logging_thread(
        f"No more job in queue, this thread finished. Total jobs executed: {num_thread_jobs}, total time worked: {h.get_duration(thread_start_time)}",
        thread,
    )


def main():
    if not h.verify_db_connections(MIGRATION_CONFIG):
        quit()
    global migration_queue
    global project_id

    migration_jobs = []
    logging.debug("Validating migraition tables queue file ...")
    total_jobs = None
    migration_jobs, all_tables = h.build_migration_jobs(args.queue_file)
    if not migration_jobs:
        logging.debug("No valid migration job is pending. Exit.")
        if not args.replication: 
            quit()
    
    for job in migration_jobs:
        migration_queue.put(job)
    total_jobs = len(migration_jobs)
    logging.debug(f"{total_jobs} migration job(s) pending in queue.")

    project_id = str(uuid.uuid4())
    start_time = time.time()
    logging.info(f"============== Start migration project: {project_id} ========== ")
    f = open("migration_snapshot.tsv", "r")
    row = list(csv.DictReader(f, delimiter="\t"))[0]
    snapshot_name = row["snapshot_name"]
    logging.info(f"Migrating with snapshot: {snapshot_name}")
    procs = [
        Process(target=execute_tasks, args=(i + 1,snapshot_name,))
        for i in range(min(args.number_thread, total_jobs))
    ]
    if args.replication == "True":
        procs.append(Process(target=h.start_replication, args=(MIGRATION_CONFIG, all_tables)))
        procs.append(Process(target=h.process_replication, args=(MIGRATION_CONFIG, all_tables, args.queue_file)))
    
    for p in procs:
        time.sleep(0.5)
        p.start()
    for p in procs:
        time.sleep(0.5)
        p.join()

    total_duration = h.get_duration(start_time)
    logging.info(
        f"============== End of migration project: {project_id}. Total time took: {total_duration}. ============== "
    )

if __name__ == "__main__":
    main()
