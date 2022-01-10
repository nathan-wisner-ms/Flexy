import argparse
import flexy_helper as helper
import argparse
import logging
import time

helper.setup_logging()

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--config-file",
    help="File Path of Source and Target Database Configuration",
    required=True,
)
parser.add_argument("-t", "--table", help="Single migration job", required=True)
args = parser.parse_args()


def main():
    # TODO: validate input table value
    migration_config = helper.build_config(args.config_file)
    logging.info(f"============== Start migration job: {args.table} ========== ")
    start_time = time.time()
    helper.execute_migration_job(1, args.table, migration_config)
    logging.info(
        f"============== End of migration job: {args.table}. Total time took: {helper.get_duration(start_time)}. ============== "
    )


if __name__ == "__main__":
    main()
