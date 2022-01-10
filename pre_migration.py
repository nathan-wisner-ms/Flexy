import argparse
import helper
import argparse
import subprocess

parser = argparse.ArgumentParser()
parser.add_argument(
    "-c",
    "--config-file",
    help="File Path of Source and Target Database Configuration",
    required=True,
)
parser.add_argument(
    "-o",
    "--function",
    help="Function you want to run for pre-migration",
    choices=("migrate_schema", "migrate_roles"),
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
    try:
        if args.function == "migrate_schema":
            helper.migrate_schema(args.config_file, args.indexes)
        if args.function == "migrate_roles":
            helper.migrate_roles(args.config_file)
        print(f"=============== Pre-Migration for {args.function} Done! ===========")
    except subprocess.CalledProcessError as e:
        print(e)

if __name__ == "__main__":
    main()
