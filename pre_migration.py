import argparse
import flexy_helper as helper
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
    "-f",
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
    exit_code = None
    try:
        if args.function == "migrate_schema":
            exit_code = helper.migrate_schema(args.config_file, args.indexes)
        if args.function == "migrate_roles":
            exit_code = helper.migrate_roles(args.config_file)
    except subprocess.CalledProcessError as e:
        print(helper.mask_credentail(e))
        raise Exception("Faile to complete {args.function}")
    if 0 != exit_code:
        print(f"Failed to complete {args.function}")
    else:
        print(f"=============== Pre-Migration: {args.function} Done! ===========")

if __name__ == "__main__":
    main()




