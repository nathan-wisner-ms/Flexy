import time
from multiprocessing import Process, Queue
import argparse
import src.flexy_helper as h
import logging
from datetime import datetime
import sys
sys.path.append('../src')


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
    help="Function you want to run for replication",
    choices=("start", "consume")
)

parser.add_argument(
    "-b", "--batch-size",
    help="Batch size for committing query changes together",
    type=int,
    default=500,
)

parser.add_argument(
    "-l", "--log-level",
    help="Lowest Logging Level",
    choices=("DEBUG", "INFO"),
    default="INFO",
)


args = parser.parse_args()

MIGRATION_CONFIG = h.build_config(args.config_file)
migration_copy_queue = Queue()

PROJECT_ID = h.generate_id()
logs_dir = MIGRATION_CONFIG["local"]["logs_dir"]
log_file = f'{logs_dir}/logs_migrate_{datetime.now().strftime("%Y_%m_%d_%H_%M")}_{PROJECT_ID}'
h.setup_logging(log_file, args.log_level, PROJECT_ID)

def execute_tasks(thread, snapshot_name):
    global migration_copy_queue

    num_thread_jobs = 0
    thread_start_time = time.time()
    while not migration_copy_queue.empty():
        message = migration_copy_queue.get()
        num_thread_jobs += 1
        h.logging_thread(
            f"Pick up migration copy job {num_thread_jobs}: {message}",
            thread,
        )
        h.execute_migration_job(
            thread, message, MIGRATION_CONFIG, snapshot_name)

    h.logging_thread(
        f"No more job in queue, this thread finished. Total jobs executed: {num_thread_jobs}, total time worked: {h.get_duration(thread_start_time)}",
        thread,
    )


def main():
    logging.info(f'============== Start migration project: {PROJECT_ID} ==========')
    logging.info(f'source:  host={MIGRATION_CONFIG["source"]["host"]}   database={MIGRATION_CONFIG["source"]["database"]}')
    logging.info(f'target:  host={MIGRATION_CONFIG["target"]["host"]}   database={MIGRATION_CONFIG["target"]["database"]}')
    logging.info(f'config-file: {args.config_file}  queue-file: {args.queue_file}   number-thread: {args.number_thread}')
    if args.replication:
        logging.info(f'replication: {args.replication}  batch-size: {args.batch_size}')
    logging.info(f'log-level: {args.log_level}  log file: {log_file} ')
    logging.info('======================================')
    if not h.verify_db_connections(MIGRATION_CONFIG):
        quit()

    if "start" == args.replication:
        while True:
            try:
                h.start_replication(MIGRATION_CONFIG, args.queue_file)
            except Exception as err:
                logging.warning(err)
                time.sleep(30)

    global migration_copy_queue

    logging.info("Validating migration tables queue file ...")
    copy_jobs = h.build_migration_jobs(args.queue_file)
    n_copy_jobs = len(copy_jobs)
    logging.info(f"{n_copy_jobs} migration copy job(s) pending in queue.")
    copy_procs = []
    if not copy_jobs:
        if "consume" != args.replication:
            logging.warn("No migration job or replication. Exit.")
            quit()
    else:
        snapshot_name = h.get_snapshot_name(MIGRATION_CONFIG["source"])
        if not snapshot_name:
            quit()
        logging.info(f"Migrating with snapshot: {snapshot_name}")

        #TODO: wrap in one function
        for job in copy_jobs:
            migration_copy_queue.put(job)
        copy_procs = [
            Process(target=execute_tasks, args=(i + 1, snapshot_name,))
            for i in range(min(args.number_thread, n_copy_jobs))
        ]
        start_time = time.time()
        h.start_process(copy_procs)
        logging.info(f"=========== Data migration by snapshot {snapshot_name} is completed. Total time took: {h.get_duration(start_time)}. ============")
    
    # ========== second migration stage =============
    if h.check_migration_jobs_all_success(args.queue_file):
        procs_dict = {}
        # TODO: add process: CREATE INDEXES
        procs_dict["create_indexes"] = Process(target=h.create_indexes, args=(MIGRATION_CONFIG,args.queue_file,))

        if "consume" == args.replication:
            procs_dict["consume_replication"] = Process(target=h.consume_replication, args=(
                MIGRATION_CONFIG, args.queue_file, args.batch_size,))
        
        if procs_dict:
            logging.info(f"Start second stage migration process: {procs_dict.keys()} ...")
            h.start_process(list(procs_dict.values()))

if __name__ == "__main__":
    main()
