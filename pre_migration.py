import argparse
import flexy_helper as helper
import argparse
import subprocess
import sys

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--config-file",
    help="File Path of Source and Target Database Configuration",
    default="config.ini",
)
parser.add_argument(
    "-f",
    "--function",
    help="Function you want to run for pre-migration",
    choices=("migrate_schema", "migrate_roles", "create_list", "create_parts"),
    required=True
)
parser.add_argument(
    "-i",
    "--indexes",
    help="True or False: Sync Schema with indexes for target db",
    choices=("True", "False"),
    default=False,
)

parser.add_argument(
    "-t",
    "--tables-file",
    help="File Path of tables to create partitions",
)

args = parser.parse_args()

MIGRATION_CONFIG = helper.build_config(args.config_file)

def main():
    if args.function == "create_parts"and not args.tables_file:
        print("Error: The following arguments are required: -t/--tables-file")
        quit()

    if not helper.verify_db_connections(MIGRATION_CONFIG):
        quit()
    
    exit_code = None
    try:
        if args.function == "migrate_schema":
            exit_code = helper.migrate_schema(args.config_file, args.indexes)
        if args.function == "migrate_roles":
            exit_code = helper.migrate_roles(args.config_file)
        if args.function == "create_list":
            exit_code = helper.create_list_of_tables(args.config_file, MIGRATION_CONFIG["source"])
        if args.function == "create_parts":
            exit_code = helper.create_table_parts(MIGRATION_CONFIG, args.tables_file)
    except subprocess.CalledProcessError as e:
        print(helper.mask_credentail(e))
        raise Exception("Faile to complete {args.function}")

    if 0 != exit_code:
        print(f"Failed to complete {args.function}")
    else:
        print(f"=============== Pre-Migration: {args.function} Done! ===========")

if __name__ == "__main__":
    main()




