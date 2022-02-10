from configparser import ConfigParser
from multiprocessing import Process, Queue
from datetime import datetime, timedelta
import time
import urllib.parse
import subprocess
import csv
import logging
import sys
import re
import psycopg2
import math
from psycopg2 import extras
import os
import re
import json
from os.path import exists
import random
import string

REPLICATION_SLOT_NAME = 'flexy_migration_slot'
MIGRATION_JOB_STATUS_FILE_NAME = "migration_jobs_status.tsv"
MIGRATION_SNAPSHOT_FILE_NAME = "migration_snapshot.tsv"


def setup_logging(log_file=None, log_level=logging.INFO, log_id=None):
    log_handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        log_handlers.append(
            logging.FileHandler(log_file)
        )
    if log_id:
        log_format = f"[%(asctime)s][%(levelname)s][{log_id}] %(message)s"
    else:
        log_format = "[%(asctime)s][%(levelname)s] %(message)s"

    logging.basicConfig(
        level=log_level,
        format=log_format,
        handlers=log_handlers,
    )


def generate_id():
    def rand_str(n): return ''.join(
        [random.choice(string.ascii_uppercase) for i in range(n)])
    return rand_str(8)


def build_config(config_file):
    migration_config = {}
    parser = ConfigParser()
    parser.read(config_file)
    for section in parser.sections():
        migration_config[section] = {}
        params = parser.items(section)
        for param in params:
            migration_config[section][param[0]] = param[1]
    return migration_config


def build_connection_string(db_config):
    conn_string = f'postgres://{urllib.parse.quote(db_config["user"])}:{urllib.parse.quote(db_config["password"])}'
    conn_string += f'@{urllib.parse.quote(db_config["host"])}:{urllib.parse.quote(db_config["port"])}'
    conn_string += f'/{urllib.parse.quote(db_config["database"])}'
    if db_config["sslmode"].strip():
        conn_string += f'?sslmode={urllib.parse.quote(db_config["sslmode"])}'
    return conn_string


def build_db_connection(db_config, replication=False):
    conn_string = build_connection_string(db_config)
    db_conn = psycopg2.connect(conn_string)
    if replication:
        db_conn = psycopg2.connect(
            conn_string, connection_factory=extras.LogicalReplicationConnection)
    else:
        db_conn = psycopg2.connect(conn_string)
    return db_conn


def verify_db_connections(migration_config):
    for section in ["source", "target"]:
        logging.info(f"Verifying connection to {section} db ...")
        db_conn = build_db_connection(migration_config[section])
        if not db_conn:
            logging.info(f"Failed to connect to {section} db.")
            return False
    return True


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
            if "CREATE INDEX" in line or "CREATE UNIQUE INDEX" in line:
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
    logging.info(
        f'Geting latest schema from source db: {mask_credentail(source_conn_url)}'
    )
    exit_code = subprocess.call(
        f'rm -fv {schema_file_name};pg_dump --schema-only --no-privileges --no-owner "{source_conn_url}" > {schema_file_name}',
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
    logging.info(f"Sync schema with no indexes in target db...")
    return subprocess.call(
        f'psql "{target_conn_url}" < {sync_schema_file_name}',
        stdout=True,
        stderr=True,
        shell=True,
    )


def migrate_roles(config_file_name):
    # TODO
    print("This feature is to be implemented. ")


def create_list_of_tables(config_file, source_db_config):
    source_db_conn = build_db_connection(source_db_config)
    query_tables_order_by_size = """
     SELECT schemaname||'.'||relname as schema_table, pg_size_pretty(pg_relation_size(relid)) AS data_size
 FROM pg_catalog.pg_statio_user_tables
 ORDER BY pg_relation_size(relid) DESC;
    """
    logging.info(
        "Fetching list of tables from source db order by data size ...")
    source_cursor = source_db_conn.cursor()
    source_cursor.execute(query_tables_order_by_size)
    rows = source_cursor.fetchall()
    filename1, filename2 = f"tables_{config_file}", f"tables_size_{config_file}.tsv"
    logging.info(f"Writing data to files: {filename1}, {filename2}")
    f1 = open(filename1, "w+")
    f2 = open(filename2, "w+")
    writer2 = csv.writer(f2, delimiter="\t")
    writer2.writerow(["schema_table", "data_size"])
    for row in rows:
        f1.write(f"{row[0]}\n")
        writer2.writerow(row)
    f1.close()
    f2.close()
    return 0


def create_table_parts(migration_config, tables_file):
    #TODO: add indexes on the column
    infile = open(tables_file)
    lines = infile.read().splitlines()
    infile.close()
    partitions = []
    source_db_config = migration_config["source"]
    source_db_conn = build_db_connection(source_db_config)
    for line in lines:
        if not line.strip() or not "|" in line:
            raise Exception(f"Unable to parse the line: {line}")
        values = line.split("|")
        schema_table, columnname = values[0], values[1]
        source_cursor = source_db_conn.cursor()
        logging.info(
            f"Fetching information for {schema_table}:{columnname} ...")
        source_cursor.execute(f"""
        SELECT data_type, is_nullable from information_schema.columns 
        WHERE table_schema||'.'||table_name = '{schema_table}' AND column_name = '{columnname}';
        """)
        result = source_cursor.fetchone()
        if not result:
            raise Exception(
                "Failed to find information: {schema_name} OR {column_name} does not exit.")
        data_type, is_nullable = result[0], result[1]
        type_code = None
        if data_type in ["smallint", "integer", "bigint", "smallserial", "serial", "bigserial"]:
            type_code = 1
        elif "timestamp" in data_type:
            type_code = 2
        else:
            raise Exception(
                f"Unable to create parts for {schema_table} based on the column {columnname}, datatype: {data_type}")

        logging.info(f"Fetching data size for {schema_table} ...")
        source_cursor.execute(f"""
        SELECT pg_size_pretty(pg_relation_size('{schema_table}'));
        """)
        table_size_string = source_cursor.fetchone()[0]
        table_size_gb = int(table_size_string.split()[0])
        chunk_size_gb = int(migration_config["local"]["chunk_size_gb"])
        n_parts = math.ceil(table_size_gb/chunk_size_gb)
        logging.info(f"Result: {table_size_string}")
        if "GB" not in table_size_string or ("GB" in table_size_string and table_size_gb < 20) or n_parts <= 1:
            logging.info(f"No need to create parts for table {schema_table}.")
            partitions.append(schema_table)
            continue
        logging.info(
            f"Chunking {schema_table}({table_size_string}) into {len(partitions)} parts for migration ...")

        query_min_max = f"min({columnname}{'::date' if '2' == type else ''}), max({columnname}{'::date' if '2' == type else ''})"
        logging.info(f"Fetching {query_min_max} for {schema_table} ...")
        source_cursor.execute(f"SELECT {query_min_max} FROM {schema_table};")
        min_max_values = source_cursor.fetchone()
        logging.info(f"Result: {min_max_values}")
        min_value, max_value = min_max_values[0], min_max_values[1]
        if 1 == type_code:
            total_size = max_value - min_value + 1
            num_step = int(total_size/n_parts)
            last_num = None
            for num in range(min_value, max_value, num_step):
                last_num = num + num_step
                partitions.append(
                    f"{schema_table}|{columnname}|I|{num},{last_num}")
            partitions.append(f"{schema_table}|{columnname}|I|{last_num},")
        elif 2 == type_code:
            date_format = "%Y-%m-%d"
            days_diff = (max_value - min_value).days
            day_step = int(days_diff/n_parts)
            curr_date = min_value
            while (max_value - curr_date).days > day_step:
                next_date = curr_date + timedelta(days=day_step)
                partitions.append(
                    f"{schema_table}|{columnname}|I|{curr_date.strftime(date_format)},{next_date.strftime(date_format)}")
                curr_date = next_date
            partitions.append(
                f"{schema_table}|{columnname}|I|{curr_date.strftime(date_format)},")
        if is_nullable == "YES":
            partitions.append(f"{schema_table}|{columnname}|V|NULL")

    if not partitions:
        return 1
    out_filename = f"parts_{tables_file}"
    print(f"Writing to {out_filename} ...\n")
    outfile = open(out_filename, "w+")
    for partition in partitions:
        print(partition)
        outfile.write(f'{partition}\n')
    outfile.close()
    source_db_conn.close()
    return 0


def logging_thread(statement, thread, level=logging.INFO):
    if thread:
        thread_msg = f"(thread={thread})"
    else:
        thread_msg = ""
    if level == logging.DEBUG:
        logging.debug(f"{thread_msg} {statement}")
    elif level == logging.WARNING:
        logging.warning(f"{thread_msg} {statement}")
    elif level == logging.ERROR:
        logging.error(f"{thread_msg} {statement}")
    else:
        logging.info(f"{thread_msg} {statement}")


def get_duration(start_time):
    duration = time.time() - start_time
    return time.strftime(
        f'%H:%M:%S.{str(duration).split(".")[1][:3]}', time.gmtime(duration)
    )


def check_migration_jobs_all_success(table_file):
    if not exists(MIGRATION_JOB_STATUS_FILE_NAME) or not exists(table_file):
        return False
    f1 = open(MIGRATION_JOB_STATUS_FILE_NAME, "r")
    rows = list(csv.DictReader(f1, delimiter="\t"))
    jobs_already_success = set()
    for row in rows:
        if row["status"] == "success":
            jobs_already_success.add(row["migration_job"])
    f1.close()
    f2 = open(table_file, "r")
    lines = list(f2.read().splitlines())
    f2.close()
    for line in lines:
        if line not in jobs_already_success:
            return False
    return True


def build_all_tables(table_file):
    all_tables = set()
    f = open(table_file, "r")
    lines = list(f.read().splitlines())
    for line in lines:
        line = line.strip()
        if not line:
            continue
        table = line if "|" not in line else line.split("|")[0]
        all_tables.add(table)
    return all_tables


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
                    raise Exception(f"Duplicated table parition: {line}")
                jobs_tables_request_set.add(line.split("|")[0])
            else:
                if line in jobs_tables_request_set:
                    raise Exception(
                        f"Duplicated table: {line}, conflict with partitioned migration jobs")
            jobs_tables_request_set.add(line)
            jobs_request.append(line)
    f.close()

    # filter out already succeed
    jobs_already_success = set()
    if exists(MIGRATION_JOB_STATUS_FILE_NAME):
        f = open(MIGRATION_JOB_STATUS_FILE_NAME, "r")
        rows = list(csv.DictReader(f, delimiter="\t"))
        for row in rows:
            if row["status"] == "success":
                jobs_already_success.add(row["migration_job"])
        f.close()
    else:
        logging.info(f"Generating new file: {MIGRATION_JOB_STATUS_FILE_NAME}")
        f = open(MIGRATION_JOB_STATUS_FILE_NAME, "a+")
        writer = csv.writer(f, delimiter="\t")
        writer.writerow(["migration_job", "status", "logged_at",
                        "duration", "thread_number", "count"])
        f.close()

    migration_jobs_pending = []
    for job in jobs_request:
        if job in jobs_already_success:
            logging.debug(f"Skip migration job: {job} - already succeed.")
        else:
            migration_jobs_pending.append(job)
            logging.debug(f"Queue up migration job: {job}")
    return migration_jobs_pending


def execute_migration_job(thread, message, migration_config, snapshot_name):
    for i in range(1, 4):
        start_time = time.time()
        try:
            log_migration_jobs_status(thread, message, "started", None, None)
            exit_code, count = migrate_copy_table(
                thread, message, migration_config, snapshot_name)
            if 0 == exit_code:
                duration = get_duration(start_time)
                log_migration_jobs_status(
                    thread, message, "success", duration, count)
                logging_thread(
                    f"Migrated table: {message}, time took: {duration}.",
                    thread,
                )
                break
            else:
                log_migration_jobs_status(
                    thread, message, "failure", None, None)
                logging_thread(
                    f"Sleep 30 seconds for attempt {i} for {message} ...", thread)
                time.sleep(30)
        except Exception as err:
            if "does not exist" in str(err):
                logging.warning(f"Skip retry: {mask_credentail(err)}.")
                break

def start_process(procs):
    for p in procs:
        time.sleep(0.5)
        p.start()
    for p in procs:
        time.sleep(0.5)
        p.join()

def build_query_condition(column, type, value):
    if "V" == type:
        if "NULL" == value:
            condition_string = f" WHERE {column} IS NULL"
        else:
            condition_string = f" WHERE {column} = '{value}'"
    elif "I" == type:
        interval_0, interval_1 = [interval.strip()
                                  for interval in value.split(",")]
        if interval_0 == "":
            condition_string = f" WHERE {column} < '{interval_0}'"
        elif interval_1 == "":
            condition_string = f" WHERE {column} >= '{interval_0}'"
        else:
            condition_string = (
                f" WHERE {column} >= '{interval_0}' AND {column} < '{interval_1}'"
            )
    return condition_string

#TODO: query all counts for the migration job and write to file
def get_migration_counts(count_jobs):
    queue = Queue()
    for job in count_jobs:
        queue.put(job)
    def get_count():
        while not queue.empty():
            #TODO: get count and write to file
            pass
    procs = [Process(target=get_count) for i in range(min(20, len(count_jobs)))]
    start_process(procs)

def migrate_tables(copy_jobs):
    pass

def migrate_copy_table(thread, message, migration_config, snapshot_name):
    source_conn_url = build_connection_string(migration_config["source"])
    target_conn_url = build_connection_string(migration_config["target"])
    condition_string = None
    table = message
    if "|" in message:
        table, column, type, value = message.split("|")
        condition_string = build_query_condition(column, type, value)
    delete_query = f"DELETE FROM {table}"
    delete_query += condition_string if condition_string else " WHERE 1 = 1"
    cleanup_command = f'psql "{target_conn_url}" -c "{delete_query};"'

    logging_thread(f"Cleaning up table in target: {delete_query} ...", thread)
    p0 = subprocess.Popen(
        cleanup_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    p0.wait()
    stdout0, stderr0 = p0.communicate()
    logging_thread(f"System output of {delete_query}: {stdout0}", thread, logging.DEBUG)
    if 0 != p0.returncode:
        error_message = stderr0.decode("utf-8")
        logging.error(
            f"Failed to DELETE FROM for table: {message}, error: {mask_credentail(error_message)}")
        if "does not exist" in error_message:
            raise Exception(f"table {table} does not exist")
        return p0.returncode, None
    select_query = f'SELECT * FROM {table} '
    if condition_string:
        select_query += condition_string
    logging_thread(f"Copying table from source: {select_query} ...", thread)
    copy_query = f"""\COPY ({select_query}) TO PROGRAM 'psql \\"{target_conn_url}\\" -c \\"\COPY {table} FROM STDIN;\\"'"""
    returncode1, stdout1, stderr1 = query_snapshot(
        source_conn_url, snapshot_name, copy_query)
    logging_thread(
        f"System output of \COPY table: {select_query}: {stdout1}", thread, logging.DEBUG)
    if 0 != returncode1:
        logging.error(
            f"Failed to copy for table: {table}, error: {mask_credentail(stderr1.decode('utf-8'))}")
        return returncode1, None
    output_rows = stdout1.decode('utf-8').strip().split("\n")
    logging.debug(output_rows)
    if len(output_rows) != 4:
        # error
        return 2, None
    else:
        copied_count = int(output_rows[3].replace("COPY ", ""))
        origin_count = int(output_rows[2].replace("COPY ", ""))
        # TODO: COMPARE ROW COUNT by query count in source table
        if copied_count == origin_count:
            logging_thread(f"Row count matched for copied: {table} {condition_string}", thread)
            return returncode1, copied_count
        else:
            logging_thread(f"Row count didn't match for copied: {table} {condition_string}. Retry.", thread)
            return 2, copied_count


def log_migration_jobs_status(thread, message, status, duration, count):
    f = open(f"migration_jobs_status.tsv", "a+")
    writer = csv.writer(f, delimiter="\t")
    writer.writerow([message, status, datetime.now(), duration, thread, count])
    f.close()

def get_indexes(db_config, tables):
    #TODO: handling partitioned tables
    #TODO: get indexes sort by tables
    schemas = set()
    for table in tables:
        if "|" in table:
            schema = table.split("|")[0].split(".")[0]
        else:
            schema = table.split(".")[0]
        schemas.add(schema)
    query_list_indexes = """
      SELECT pg_get_indexdef(indexrelid)
    FROM pg_index x
         JOIN pg_class i ON i.oid = x.indexrelid
         JOIN pg_class r ON r.oid = x.indrelid
         JOIN pg_namespace n ON n.oid = i.relnamespace
         JOIN pg_namespace rn ON rn.oid = r.relnamespace
         LEFT JOIN pg_depend d 
                on d.classid = 'pg_class'::regclass
               and d.objid = i.oid
               and d.refclassid = 'pg_constraint'::regclass
               and d.deptype = 'i'
         LEFT JOIN pg_constraint c ON c.oid = d.refobjid
   where n.nspname !~ '^pg_' and n.nspname <> 'information_schema' AND 
r.relkind = 'r' and r.relpersistence = 'p' AND pg_get_constraintdef(c.oid) IS NULL
AND n.nspname IN (%s)
    """
    db_conn = build_db_connection(db_config)
    db_cur = db_conn.cursor()
    db_cur.execute(query_list_indexes, list(schemas))
    rows = db_cur.fetchall()
    return [row[0] for row in rows]   

#create indexes concurrently in paralllel
def create_indexes(migration_config, table_file):
    logging.info("============== Creating INDEXES =================")
    tables = build_all_tables(table_file)
    indexes = get_indexes(migration_config["source"], tables)
    logging.debug(f"Fetched list of indexes : {indexes}")
    if len(indexes) == 0:
        logging.debug("No indexes to create.")
        return
    tasks = Queue()
    for index_row in indexes:
        query_create_index = index_row.replace(" INDEX ", " INDEX CONCURRENTLY IF NOT EXISTS ", 1)
        tasks.put(query_create_index)

    def execute():
        target_conn = build_db_connection(migration_config["target"])
        target_conn.autocommit = True
        target_cursor = target_conn.cursor()
        while not tasks.empty(): 
            query = tasks.get()
            logging.debug(f"Creating index: {query}")
            try:
                target_cursor.execute(query)
            except Exception as error:
                logging.warning(f"Error encounterd while {query}, error: {error}")
        target_conn.close()

    procs = [Process(target=execute) for i in range(3)]
    start_time = time.time()
    start_process(procs)
    logging.info(f"============= Indexes creation is completed. Time took: {get_duration(start_time)}. ===========")


def mask_credentail(message):
    messages = str(message).split()
    return " ".join([re.sub(r'postgres://.*@', 'postgres://<USERNAME>:<PASSWORD>@', m) for m in messages])


def create_snapshot(db_config):
    # TODO: create snapshot for offline migration
    db_conn = build_db_connection(db_config)
    db_cur = db_conn.cursor()
    db_conn.autocommit = True
    db_cur.execute(f"")


def drop_replication_slot(db_config):
    db_conn_string = build_connection_string(db_config)
    logging.debug(f"Dropping replication slot: {REPLICATION_SLOT_NAME}")
    exit_code = subprocess.call(
        f'psql "{db_conn_string}" -c "SELECT pg_drop_replication_slot(\'{REPLICATION_SLOT_NAME}\');"',
        stdout=True,
        stderr=True,
        shell=True,
    )
    return exit_code


def create_replication_slot(migration_config):
    target_db_config = migration_config["target"]
    target_db_conn_string = build_connection_string(target_db_config)
    logging.info("Creating table in target: migration_cdc_logs")
    subprocess.call(
        f'psql "{target_db_conn_string}" < ./src/create_migration_cdc_logs.sql',
        stdout=True,
        stderr=True,
        shell=True,
    )
    source_db_config = migration_config["source"]
    logging.info(
        f"Creating replication slot: {REPLICATION_SLOT_NAME} and exporting snapshot ...")
    source_db_conn = build_db_connection(source_db_config, True)
    source_db_cur = source_db_conn.cursor(cursor_factory=extras.DictCursor)
    created_at = datetime.now()
    source_db_cur.execute(
        f'CREATE_REPLICATION_SLOT {REPLICATION_SLOT_NAME} LOGICAL wal2json EXPORT_SNAPSHOT;')
    row = dict(source_db_cur.fetchone())
    logging.info(f"replication and snapshot info: {row}, time: {created_at}")
    f = open(MIGRATION_SNAPSHOT_FILE_NAME, "w+")
    writer = csv.writer(f, delimiter="\t")
    header = ["slot_name", "consistent_point",
              "snapshot_name", "output_plugin"]
    writer.writerow(header + ["created_at"])
    writer.writerow([row[h] for h in header] + [created_at])
    f.close()
    while True:
        logging.info(f'keep transaction snapshot {row["snapshot_name"]} live ...')
        time.sleep(60)


def query_snapshot(psql_conn_string, snapshot_name, query):
    command = f'bash ./src/query_snapshot.sh "{psql_conn_string}" "{snapshot_name}" "{query}"'
    logging.debug(mask_credentail(command))
    p = subprocess.Popen(command, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, shell=True)
    p.wait()
    stdout, stderr = p.communicate()
    error = stderr.decode('utf-8')
    if "ERROR:  invalid snapshot identifier:" in error:
        raise Exception(stderr)
    return p.returncode, stdout, stderr


def get_snapshot_name(db_config):
    logging.info("Fetching snapshot ...")
    conn_string = build_connection_string(db_config)
    try:
        f = open(MIGRATION_SNAPSHOT_FILE_NAME, "r")
        row = list(csv.DictReader(f, delimiter="\t"))[0]
        f.close()
        snapshot_name = row["snapshot_name"]
        logging.debug(query_snapshot(conn_string, snapshot_name, ""))
        return snapshot_name
    except Exception as error:
        logging.error(f"Snapshot not found or no longer exists: {error}")
        return None


def start_replication(migration_config, table_file):
    tables = build_all_tables(table_file)
    cdc_dir = migration_config["local"]["cdc_dir"]
    target_conn = build_db_connection(migration_config["target"])
    target_cursor = target_conn.cursor(cursor_factory=extras.DictCursor)
    target_cursor.execute("""
    SELECT * FROM migration_cdc_logs
    ORDER BY change_id DESC LIMIT 1;
    """)
    last_lsn_row = target_cursor.fetchone()
    last_lsn = last_lsn_row["lsn_0"] if last_lsn_row else None
    logging.info(
        f'Start receiving replication messages from last received lsn: {last_lsn} and write to local dir: {cdc_dir} ...')
    source_conn = build_db_connection(migration_config["source"], True)
    source_cursor = source_conn.cursor()
    options = {
        'include-timestamp': 1,
        'include-lsn': 1,
        'add-tables': ",".join(tables)
    }
    if last_lsn:
        source_cursor.start_replication(
            slot_name=f'{REPLICATION_SLOT_NAME}',
            start_lsn=last_lsn,
            options=options,
            decode=True,
            status_interval=30
        )
    else:
        source_cursor.start_replication(
            slot_name=f'{REPLICATION_SLOT_NAME}',
            options=options,
            decode=True,
            status_interval=30
        )

    def write_to_file(msg):
        msg_obj_string = str(msg)
        m = re.match(
            r".* data_start: (?P<data_start>.*); wal_end:.*", msg_obj_string)
        lsn = m.group("data_start").upper()
        convert_time_string = str(msg.send_time).replace(
            '-', '').replace(' ', '').replace(':', '')
        file_path = f"{cdc_dir}/{convert_time_string}_{msg.data_start}"
        target_cursor.execute(f"""
        INSERT INTO migration_cdc_logs(lsn_0, lsn_1, data_size, received_at)
        VALUES ('{lsn}', '{msg.data_start}', {msg.data_size}, '{msg.send_time}')
        ON CONFLICT(lsn_0) DO NOTHING;
        """)
        insert_count = target_cursor.rowcount
        if not insert_count:
            logging.info(
                f"Skip duplicate message lsn: {lsn}, {msg.data_start}")
            return
        logging.debug(
            f"<<<<<< Received: {msg_obj_string}, lsn:{msg.data_start}, send_time: {msg.send_time}")
        f = open(file_path, "w+")
        f.write(msg.payload)
        f.close()
        logging.debug(f"Write change message to {file_path}")
        target_conn.commit()
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
    source_cursor.consume_stream(write_to_file)


def handle_record(file_path, db_cursor, tables):
    n_insert, n_update, n_delete = 0, 0, 0
    try:
        message = json.load(open(file_path, 'r'))
        logging.debug(f'****** Handling message next lsn: {message["nextlsn"]},'
                      f'received_at: {message["timestamp"]}, file_path: {file_path}')
        if "change" not in message or not message["change"]:
            logging.debug(f"no change in message: {file_path}")
        else:
            for each in message["change"]:
                if each["kind"] == "insert":
                    table = f'{each["schema"]}.{each["table"]}'
                    if table not in tables:
                        continue
                    columnnames = each["columnnames"]
                    columnvalues = each["columnvalues"]
                    values_format = "%s, ".join(
                        ["" for i in range(len(columnnames)+1)])[:-2]
                    insert_query = f'INSERT INTO {table} ({", ".join(columnnames)}) VALUES ({values_format});'
                    db_cursor.execute(insert_query, columnvalues)
                    n_insert += 1
                elif each["kind"] == "update":
                    table = f'{each["schema"]}.{each["table"]}'
                    if table not in tables:
                        continue
                    columnnames = each["columnnames"]
                    columnvalues = each["columnvalues"]
                    keynames = each["oldkeys"]["keynames"]
                    keyvalues = each["oldkeys"]["keyvalues"]
                    set_names = ", ".join(
                        [f'{name} = %s' for name in columnnames])
                    where_names = " AND ".join(
                        [f'{name} = %s' for name in keynames])
                    update_query = f'UPDATE {table} SET {set_names} WHERE {where_names} ;'
                    db_cursor.execute(update_query, tuple(
                        columnvalues + keyvalues))
                    n_update += 1
                elif each["kind"] == "delete":
                    table = f'{each["schema"]}.{each["table"]}'
                    if table not in tables:
                        continue
                    keynames = each["oldkeys"]["keynames"]
                    keyvalues = each["oldkeys"]["keyvalues"]
                    where_names = " AND ".join(
                        [f'{name} = %s' for name in keynames])
                    delete_query = f'DELETE FROM {table} WHERE {where_names} ;'
                    db_cursor.execute(delete_query, tuple(keyvalues))
                    n_delete += 1
                else:
                    logging.debug(
                        f'Skip handling change: {each["kind"]} in {file_path}')
    except Exception as error:
        logging.error(f"Encountered error while handling message file: {file_path}, message: \n{open(file_path, 'r').read()} \nerror: {error}")
        raise Exception(error)
    return n_insert, n_update, n_delete


def consume_replication(migration_config, tables_file, batch_size):
    if not check_migration_jobs_all_success(tables_file):
        logging.info(
            f"Migration jobs in {tables_file} are not completed yet, do not start consuming replication message.")
        quit()

    logging.info(
        "Initial loading is completed. Start consuming replication messages ...")
    tables = build_all_tables(tables_file)

    cdc_dir = migration_config["local"]["cdc_dir"]
    target_conn = build_db_connection(migration_config["target"])
    target_cursor = target_conn.cursor()
    ls_command = f"ls -ltr {cdc_dir}" + \
        " | awk '{ print $9 }' " + f" | head -n {batch_size + 1}"
    while True:
        total_count = len(os.listdir(cdc_dir))
        if not total_count:
            logging.info("No unproceeded message. Sleep 30 seconds ...")
            time.sleep(30)
            continue
        else:
            logging.info(f"Number of total unproceeded message(s): {total_count}")
        logging.info(f"Fetching next {batch_size} unproceeded messages ...")
        #TODO: write to file
        p = subprocess.Popen(ls_command, stdout=subprocess.PIPE,
                             stderr=subprocess.PIPE, shell=True)
        p.wait()
        stdout, stderr = p.communicate()
        if 0 != p.returncode:
            logging.warning(
                f"Failed to get list of files, error: {stderr}. Try again.")
            continue
        filenames = stdout.decode('utf-8').strip().split("\n")
        if len(filenames) < 1:
            continue
        logging.info(
            f"Processing {len(filenames)} message(s) from {filenames[0]} to {filenames[-1]} ...")
        n_processed = 0
        start_time = time.time()
        total_insert, total_update, total_delete = 0, 0, 0
        for filename in filenames:
            if not filename.strip():
                continue
            n_insert, n_update, n_delete = handle_record(
                f'{cdc_dir}/{filename}', target_cursor, tables)
            total_insert += n_insert
            total_update += n_update
            total_delete += n_delete
            n_processed += 1
        try:
            target_conn.commit()
            # all success, can remove
            end_time = time.time()
            total_seconds = end_time - start_time
            logging.info(f"Processed {n_processed} transaction messages, time took: {get_duration(start_time)}, average: {round(n_processed/total_seconds, 2)} message/s,"
                         f" insert: {total_insert} ({round(total_insert/total_seconds, 2)}/s),"
                         f" update: {total_update} ({round(total_update/total_seconds, 2)}/s),"
                         f" delete: {total_delete} ({round(total_delete/total_seconds, 2)}/s),"
                         f" remove local files."
            )
            for filename in filenames:
                os.remove(f'{cdc_dir}/{filename}')
        except Exception as err:
            logging.error(f"Failed to commit the changes, error: {err}")