import argparse
import helper
import argparse

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--config-file",
    help="File Path of Source and Target Database Configuration",
    required=True,
)
parser.add_argument(
    "-o",
    "--option",
    help="Function you want to run for pre-migration",
    choices=("schema", "roles"),
    required=True
)
parser.add_argument(
    "-i",
    "--indexes",
    help="True or False: Sync Schema with indexes for target db",
    choices=("True", "False"),
    default=False,
)
args = parser.parse_args()

def main():
    if args.option == "schema":
        helper.migrate_schema(args.config_file, args.indexes)
    if args.option == "roles":
        helper.migrate_roles(args.config_file)
    print("======= Pre-Migration Steps Done! =========")

if __name__ == "__main__":
    main()
