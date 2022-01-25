import time
from multiprocessing import Process, Queue
import argparse
from typing import final
import flexy_helper
import argparse
import logging
import uuid
from datetime import datetime

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--config-file",
    help="File Path of Source and Target Database Configuration",
    required=True,
)
parser.add_argument(
    "-n",
    "--number-thread",
    help="number of parallel threads, default: 20",
    type=int,
    default=10,
)
parser.add_argument(
    "-q", "--queue-file", help="File Path of tables for migration", required=True
)
args = parser.parse_args()


flexy_helper.setup_logging()
tasks_queue = Queue()
project_id = ""
MIGRATION_CONFIG = flexy_helper.build_config(args.config_file)

def execute_tasks(thread):
    global tasks_queue
    global project_id

    num_thread_jobs = 0
    thread_start_time = time.time()
    while not tasks_queue.empty():
        message = tasks_queue.get()
        num_thread_jobs += 1
        flexy_helper.logging_thread(
            f"Pick up migration job {num_thread_jobs}: {message}",
            thread,
        )
        flexy_helper.execute_migration_job(thread, message, MIGRATION_CONFIG)

    flexy_helper.logging_thread(
        f"No more job in queue, this thread finished. Total jobs executed: {num_thread_jobs}, total time worked: {flexy_helper.get_duration(thread_start_time)}",
        thread,
    )

def main():
    if not flexy_helper.verify_db_connections(MIGRATION_CONFIG):
        quit()

    global tasks_queue
    global project_id

    migration_jobs = []
    logging.debug("Validating migraition tables queue file ...")
    total_jobs = None
    migration_jobs = flexy_helper.build_migration_jobs(args.queue_file)
    if not migration_jobs:
        logging.debug("No valid migration job is pending. Exit.")
        quit()

    for job in migration_jobs:
        tasks_queue.put(job)
    total_jobs = len(migration_jobs)
    logging.debug(f"{total_jobs} migration job(s) pending in queue.")

    project_id = str(uuid.uuid4())
    start_time = time.time()
    logging.info(f"============== Start migration project: {project_id} ========== ")
    procs = [
        Process(target=execute_tasks, args=(i + 1,))
        for i in range(min(args.number_thread, total_jobs))
    ]
    for p in procs:
        time.sleep(0.5)
        p.start()
    for p in procs:
        time.sleep(0.5)
        p.join()

    total_duration = flexy_helper.get_duration(start_time)
    logging.info(
        f"============== End of migration project: {project_id}. Total time took: {total_duration}. ============== "
    )

if __name__ == "__main__":
    main()
