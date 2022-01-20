from configparser import ConfigParser
from datetime import datetime
from os import strerror
import time
import urllib.parse
import subprocess
import csv
import logging
import sys
import re


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s] [%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(
                f'migration_logs_{datetime.now().strftime("%Y_%m_%d_%H_%M")}'
            ),
            logging.StreamHandler(sys.stdout),
        ],
    )


setup_logging()


def build_config(config_file):
    migration_config = {}
    parser = ConfigParser()
    parser.read(config_file)
    for section in ["source", "target"]:
        if not parser.has_section(section):
            raise Exception(f"Section {section} not found in the file: {config_file}")
        migration_config[section] = {}
        params = parser.items(section)
        for param in params:
            migration_config[section][param[0]] = param[1]
    return migration_config


def build_connection_string(db_config):
    conn_string = f'postgres://{urllib.parse.quote(db_config["user"])}:{urllib.parse.quote(db_config["password"])}'
    conn_string += f'@{urllib.parse.quote(db_config["host"])}:{urllib.parse.quote(db_config["port"])}'
    conn_string += f'/{urllib.parse.quote(db_config["database"])}?sslmode={urllib.parse.quote(db_config["sslmode"])}'
    return conn_string


def spit_out_schema_files(schema_file_name, config_file_name):
    f = open(schema_file_name)
    lines = [line for line in f.readlines() if line.strip()]
    f.close()

    queries = []
    query = []
    for i in range(len(lines) - 1):
        line = lines[i]
        query.append(line)
        if not line.startswith("--"):
            if lines[i + 1].startswith("--"):
                queries.append(query)
                query = []

    queries_index = []
    queries_tables = []
    for query in queries:
        is_index_query = False
        for line in query:
            if "CREATE INDEX" in line:
                is_index_query = True
                queries_index.append(query)
                break
        if not is_index_query:
            queries_tables.append(query)

    schema_no_indexes = f"schema_no_indexes_{config_file_name}.sql"
    out1 = open(schema_no_indexes, "w+")
    for query in queries_tables:
        for line in query:
            out1.writelines(line)
    out1.close()

    out2 = open(f"schema_indexes_only_{config_file_name}.sql", "w+")
    for query in queries_index:
        for line in query:
            out2.writelines(line)
    out2.close()


def migrate_schema(config_file_name, create_indexes):
    migration_config = build_config(config_file_name)
    source_conn_url = build_connection_string(migration_config["source"])
    target_conn_url = build_connection_string(migration_config["target"])
    schema_file_name = f"schema_{config_file_name}.sql"
    print(
        f'Geting latest schema from source db: {mask_credentail(source_conn_url)}'
    )
    exit_code = subprocess.call(
        f'rm -v {schema_file_name};pg_dump --schema-only --no-privileges --no-owner "{source_conn_url}" > {schema_file_name}',
        stdout=True,
        stderr=True,
        shell=True,
    )
    if 0 != exit_code:
        return exit_code
    spit_out_schema_files(schema_file_name, config_file_name)
    schema_no_indexes_file_name = f"schema_no_indexes_{config_file_name}.sql"
    sync_schema_file_name = (
        schema_file_name if create_indexes == "True" else schema_no_indexes_file_name
    )
    print(f"Sync schema with no indexes in target db...")
    return subprocess.call(
        f'psql "{target_conn_url}" < {sync_schema_file_name}', 
        stdout=True,
        stderr=True,
        shell=True,
    )


def migrate_roles(config_file_name):
    # TODO
    print("This feature is to be implemented. ")


def logging_thread(statement, thread):
    if thread:
        thread_msg = f"[thread={thread}]"
    else:
        thread_msg = ""
    logging.debug(f"{thread_msg} {statement}")


def get_duration(start_time):
    duration = time.time() - start_time
    return time.strftime(
        f'%H:%M:%S.{str(duration).split(".")[1][:3]}', time.gmtime(duration)
    )


def build_migration_jobs(table_file):
    jobs_request = []
    jobs_tables_request_set = set()
    # validate file
    with open(table_file, "r") as f:
        lines = list(f.read().splitlines())
        for line in lines:
            line = line.strip()
            if not line:
                continue
            elif "|" in line:
                # TODO: validate line format:
                # schema.tablename
                # schema.tablename|columnname|I|,range1
                # schema.tablename|columnname|I|range1,range2
                # schema.tablename|columnname|I|range1,
                # schema.tablename|columnname|V|value
                if line in jobs_tables_request_set:
                    logging.warning(f"Duplicated table parition: {line}")
                    return []
                jobs_tables_request_set.add(line.split("|")[0])
            else:
                if line in jobs_tables_request_set:
                    logging.warning(
                        f"Duplicated table: {line}, conflict with partitioned migration jobs",
                        "ERROR",
                    )
                    return []
            jobs_tables_request_set.add(line)
            jobs_request.append(line)
    f.close()

    # filter out already succeed
    jobs_already_success = set()
    try:
        f = open("migration_jobs_status.tsv", "r")
        rows = list(csv.DictReader(f, delimiter="\t"))
        for row in rows:
            if row["status"] == "success":
                jobs_already_success.add(row["migration_job"])
        f.close()
    except Exception as error:
        logging.warning(error)
        logging.info("Generating new file: migration_jobs_status.tsv")
        f = open("migration_jobs_status.tsv", "a+")
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["migration_job", "status", "logged_at", "duration"])
        f.close()

    migration_jobs_pending = []
    for job in jobs_request:
        if job in jobs_already_success:
            logging.debug(f"Skip migration job: {job} - already succeed.")
        else:
            migration_jobs_pending.append(job)
            logging.debug(f"Queue up migration job: {job}")
    return migration_jobs_pending


def execute_migration_job(thread, message, migration_config):
    for i in range(1, 4):
        start_time = time.time()
        try:
            log_migration_jobs_status(message, "started", None)
            exit_code = migrate_copy_table(thread, message, migration_config)
            if 0 == exit_code:
                duration = get_duration(start_time)
                log_migration_jobs_status(message, "success", duration)
                logging_thread(
                    f"Sent data for table: {message}, time took: {duration}.",
                    thread,
                )
                break
            else:
                log_migration_jobs_status(message, "failure", None)
                logging_thread(f"Sleep 30 seconds for attempt {i} for {message} ...", thread)
                time.sleep(30)
        except Exception as err:
            if "does not exist" in str(err):
                logging.warning(f"Skip retry: {mask_credentail(err)}.")
                break

def build_query_condition(column, type, value):
    if "V" == type:
        if "NULL" == value:
            condition_string = f"WHERE {column} IS NULL"
        else:
            condition_string = f"WHERE {column} = '{value}'"
    elif "I" == type:
        interval_0, interval_1 = [interval.strip() for interval in value.split(",")]
        if interval_0 == "":
            condition_string = f"WHERE {column} < '{interval_0}'"
        elif interval_1 == "":
            condition_string = f"WHERE {column} >= '{interval_0}'"
        else:
            condition_string = (
                f"WHERE {column} >= '{interval_0}' AND {column} < '{interval_1}'"
            )
    return condition_string

def migrate_copy_table(thread, message, migration_config):
    source_conn_url = build_connection_string(migration_config["source"])
    target_conn_url = build_connection_string(migration_config["target"])
    condition_string = ""
    table = message
    if "|"  in message:
        table, column, type, value = message.split("|")
        condition_string = build_query_condition(column, type, value)
    cleanup_method = "DELETE FROM" if condition_string else "TRUNCATE"
    cleanup_command = f'psql "{target_conn_url}" -c "{cleanup_method} {table} {condition_string};"'

    logging_thread(
        f'{cleanup_method} target table: {table} {condition_string} ...',
        thread,
    )
    p0 = subprocess.Popen(cleanup_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    p0.wait()
    stdout0, stderr0 = p0.communicate()
    if 0 != p0.returncode:
        error_message  = stderr0.decode("utf-8")
        logging.error(f"Failed to {cleanup_method} for table: {table}, error: {mask_credentail(error_message)}, output: {stdout0}")
        if "does not exist" in error_message:
            raise Exception(f"table {table} does not exist")
        return p0.returncode

    logging_thread(f"Copying table: {table} {condition_string} ... ", thread)
    select_query = f"\COPY (SELECT * from {table} {condition_string}) TO STDOUT;"
    read_query = f'psql "{source_conn_url}" -c "{select_query}"'
    write_query = f'psql "{target_conn_url}" -c "\COPY {table} FROM STDIN;"'
    copy_command = f"{read_query} | {write_query}"
    p1 = subprocess.Popen(copy_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    p1.wait()
    stdout1, stderr1 = p1.communicate()
    if 0 != p1.returncode:
        logging.error(f"Failed to copy for table: {table}, error: {mask_credentail(stderr1)}, output: {stdout1}")
    return p1.returncode 



def log_migration_jobs_status(message, status, duration):
    f = open(f"migration_jobs_status.tsv", "a+")
    writer = csv.writer(f, delimiter="\t")
    writer.writerow([message, status, datetime.now(), duration])
    f.close()

def mask_credentail(message):
    messages = str(message).split()
    return " ".join([re.sub(r'postgres://.*@', 'postgres://<USERNAME>:<PASSWORD>@', m) for m in messages])
