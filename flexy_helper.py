from configparser import ConfigParser
from datetime import datetime, timedelta
import time
import urllib.parse
import subprocess
import csv
import logging
import sys
import re
from itsdangerous import exc
import psycopg2
import math
from psycopg2 import extras
import os
import re
import json

REPLICATION_SLOT_NAME = 'flexy_migration_slot'


def setup_logging(log_file=None):
    handlers = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(
            logging.FileHandler(log_file)
        )
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s][%(levelname)s] %(message)s",
        handlers=handlers,
    )


def build_config(config_file):
    migration_config = {}
    parser = ConfigParser()
    parser.read(config_file)
    for section in ["source", "target", "local"]:
        if not parser.has_section(section):
            raise Exception(
                f"Section {section} not found in the file: {config_file}")
        migration_config[section] = {}
        params = parser.items(section)
        for param in params:
            migration_config[section][param[0]] = param[1]
    return migration_config


def build_connection_string(db_config, replication=False):
    conn_string = f'host={db_config["host"]} port={db_config["port"]} dbname={db_config["database"]} user={db_config["user"]} password={db_config["password"]}'
    if db_config["sslmode"].strip():
        conn_string += f' sslmode={db_config["sslmode"]}'
    if replication:
        conn_string += f' replication=database'
    # conn_string = f'postgres://{urllib.parse.quote(db_config["user"])}:{urllib.parse.quote(db_config["password"])}'
    # conn_string += f'@{urllib.parse.quote(db_config["host"])}:{urllib.parse.quote(db_config["port"])}'
    # conn_string += f'/{urllib.parse.quote(db_config["database"])}'
    # if db_config["sslmode"].strip():
    #     conn_string += f'?sslmode={urllib.parse.quote(db_config["sslmode"])}'
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
        print(f"Verifying connection to {section} db ...")
        db_conn = build_db_connection(migration_config[section])
        if not db_conn:
            print(f"Failed to connect to {section} db.")
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
    source_conn_url = build_connection_string(migration_config["source"], True)
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


def create_list_of_tables(config_file, source_db_config):
    source_db_conn = build_db_connection(source_db_config)
    query_tables_order_by_size = """
     SELECT schemaname||'.'||relname as schema_table, pg_size_pretty(pg_relation_size(relid)) AS data_size
 FROM pg_catalog.pg_statio_user_tables
 ORDER BY pg_relation_size(relid) DESC;
    """
    print("Fetching list of tables from source db order by data size ...")
    source_cursor = source_db_conn.cursor()
    source_cursor.execute(query_tables_order_by_size)
    rows = source_cursor.fetchall()
    filename1, filename2 = f"tables_{config_file}", f"tables_size_{config_file}.tsv"
    print(f"Writing data to files: {filename1}, {filename2}")
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
    infile = open(tables_file)
    lines = infile.read().splitlines()
    infile.close()
    partitions = []
    source_db_config = migration_config["source"]
    source_db_conn = build_db_connection(source_db_config)
    for line in lines:
        if not line.strip() or not "|" in line:
            continue
        values = line.split("|")
        schema_table, columnname = values[0], values[1]
        source_cursor = source_db_conn.cursor()
        print(f"Fetching information for {schema_table}:{columnname} ...")
        source_cursor.execute(f"""
        SELECT data_type, is_nullable from information_schema.columns 
        WHERE table_schema||'.'||table_name = '{schema_table}' AND column_name = '{columnname}';
        """)
        result = source_cursor.fetchone()
        if not result:
            print(
                "Failed to find information: {schema_name} OR {column_name} does not exit.")
            continue
        data_type, is_nullable = result[0], result[1]
        type_code = None
        if data_type in ["smallint", "integer", "bigint", "smallserial", "serial", "bigserial"]:
            type_code = 1
        elif "timestamp" in data_type:
            type_code = 2
        else:
            print(
                f"Unable to create parts for {schema_table} based on the column {columnname}, datatype: {data_type}")
            continue

        print(f"Fetching data size for {schema_table} ...")
        source_cursor.execute(f"""
        SELECT pg_size_pretty(pg_relation_size('{schema_table}'));
        """)
        table_size_string = source_cursor.fetchone()[0]
        table_size_gb = int(table_size_string.split()[0])
        chunk_size_gb = int(migration_config["local"]["chunk_size_gb"])
        n_parts = math.ceil(table_size_gb/chunk_size_gb)
        print(f"Result: {table_size_string}")
        if "GB" not in table_size_string or ("GB" in table_size_string and table_size_gb < 20) or n_parts <= 1:
            print(f"No need to create parts for table {schema_table}.")
            partitions.append(schema_table)
            continue
        print(
            f"Chunking {schema_table}({table_size_string}) into {len(partitions)} parts for migration ...")

        query_min_max = f"min({columnname}{'::date' if '2' == type else ''}), max({columnname}{'::date' if '2' == type else ''})"
        print(f"Fetching {query_min_max} for {schema_table} ...")
        source_cursor.execute(f"SELECT {query_min_max} FROM {schema_table};")
        min_max_values = source_cursor.fetchone()
        print(f"Result: {min_max_values}")
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


def logging_thread(statement, thread):
    if thread:
        thread_msg = f"(thread={thread})"
    else:
        thread_msg = ""
    logging.debug(f"{thread_msg} {statement}")


def get_duration(start_time):
    duration = time.time() - start_time
    return time.strftime(
        f'%H:%M:%S.{str(duration).split(".")[1][:3]}', time.gmtime(duration)
    )

def check_migration_jobs_all_success(table_file):
    try:
        f1 = open("migration_jobs_status.tsv", "r")
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
    except Exception as error:
        return False


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
                    raise Exception(f"Duplicated table: {line}, conflict with partitioned migration jobs")
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
    return migration_jobs_pending, jobs_tables_request_set


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


def build_query_condition(column, type, value):
    if "V" == type:
        if "NULL" == value:
            condition_string = f"WHERE {column} IS NULL"
        else:
            condition_string = f"WHERE {column} = '{value}'"
    elif "I" == type:
        interval_0, interval_1 = [interval.strip()
                                  for interval in value.split(",")]
        if interval_0 == "":
            condition_string = f"WHERE {column} < '{interval_0}'"
        elif interval_1 == "":
            condition_string = f"WHERE {column} >= '{interval_0}'"
        else:
            condition_string = (
                f"WHERE {column} >= '{interval_0}' AND {column} < '{interval_1}'"
            )
    return condition_string


def migrate_copy_table(thread, message, migration_config, snapshot_name):
    source_conn_url = build_connection_string(migration_config["source"], True)
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
    logging_thread(f"System output of {delete_query}: {stdout0}", thread)
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
    command = f'./query_snapshot.sh "{source_conn_url}" "{snapshot_name}" "{copy_query}"'
    p1 = subprocess.Popen(command, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, shell=True)
    p1.wait()
    stdout1, stderr1 = p1.communicate()
    logging_thread(
        f"System output of \COPY table: {select_query}: {stdout1}", thread)
    if 0 != p1.returncode:
        logging.error(
            f"Failed to copy for table: {table}, error: {mask_credentail(stderr1.decode('utf-8'))}")
        return p1.returncode, None
    output_rows = stdout1.decode('utf-8').split("\n")
    print(output_rows)
    if len(output_rows) == 5:
        copied_count = int(output_rows[3].replace("COPY ", ""))
    logging_thread(
        f"Comparing row count from source table: {select_query} ...", thread)
    return p1.returncode, copied_count
    # TODO: COMPARE ROW COUNT
    # source_conn = build_db_connection(migration_config["source"], True)
    # source_cursor = source_conn.cursor()
    # source_cursor.execute(f"""SELECT COUNT(1) FROM {table} {condition_string};""")
    # source_count = source_cursor.fetchone()[0]
    # source_conn.close()
    # if source_count == copied_count:
    #     logging_thread(f"Row count matched for copied: {table} {condition_string}", thread)
    #     return p1.returncode, copied_count
    # else:
    #     logging_thread(f"Row count didn't match for copied: {table} {condition_string}. Retry.", thread)
    #     return 2, copied_count


def log_migration_jobs_status(thread, message, status, duration, count):
    f = open(f"migration_jobs_status.tsv", "a+")
    writer = csv.writer(f, delimiter="\t")
    writer.writerow([message, status, datetime.now(), duration, thread, count])
    f.close()


def mask_credentail(message):
    messages = str(message).split()
    return " ".join([re.sub(r'postgres://.*@', 'postgres://<USERNAME>:<PASSWORD>@', m) for m in messages])


def create_snapshot(db_config):
    #TODO: create snapshot for offline migration
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
    logging.debug("Creating table in target: migration_cdc_logs")
    subprocess.call(
        f'psql "{target_db_conn_string}" < create_migration_cdc_logs.sql',
        stdout=True,
        stderr=True,
        shell=True,
    )
    source_db_config = migration_config["source"]
    logging.debug(
        f"Creating replication slot:{REPLICATION_SLOT_NAME} and exporting snapshot ...")
    source_db_conn = build_db_connection(source_db_config, True)
    source_db_cur = source_db_conn.cursor(cursor_factory=extras.DictCursor)
    source_db_cur.execute(
        f'CREATE_REPLICATION_SLOT {REPLICATION_SLOT_NAME} LOGICAL wal2json EXPORT_SNAPSHOT;')
    row = dict(source_db_cur.fetchone())
    logging.info(row)
    f = open(f"migration_snapshot.tsv", "w+")
    writer = csv.writer(f, delimiter="\t")
    header = ["slot_name", "consistent_point",
              "snapshot_name", "output_plugin"]
    writer.writerow(header)
    writer.writerow([row[h] for h in header])
    f.close()
    while True:
        logging.debug("keep transaction snapshot live ...")
        time.sleep(60)


def start_replication(migration_config, tables):
    cdc_dir = migration_config["local"]["cdc_dir"]
    logging.info(
        f'Start receiving replication messages and write to local dir: {cdc_dir} ...')
    source_conn = build_db_connection(migration_config["source"], True)
    source_cursor = source_conn.cursor()
    source_cursor.start_replication(slot_name=f'{REPLICATION_SLOT_NAME}',
                                    options={'include-timestamp': 1,
                                             'include-lsn': 1,
                                             'add-tables': ",".join(tables)},
                                    decode=True, status_interval=30)
    target_conn = build_db_connection(migration_config["target"])
    target_cursor = target_conn.cursor()

    def write_to_file(msg):
        msg_obj_string = str(msg)
        m = re.match(
            r".* data_start: (?P<data_start>.*); wal_end:.*", msg_obj_string)
        lsn = m.group("data_start")
        convert_time_string = str(msg.send_time).replace(
            '-', '').replace(' ', '').replace(':', '')
        file_path = f"{cdc_dir}/{convert_time_string}_{msg.data_start}"
        target_cursor.execute(f"""
        INSERT INTO migration_cdc_logs(lsn_0, lsn_1, data_size, received_at, file_path)
        VALUES ('{lsn}', '{msg.data_start}', {msg.data_size}, '{msg.send_time}', '{file_path}')
        ON CONFLICT(lsn_0) DO NOTHING;
        """)
        insert_count = target_cursor.rowcount
        if not insert_count:
            logging.warn(
                f"Skip duplicate message lsn: {lsn}, {msg.data_start}")
            return
        target_conn.commit()
        logging.debug(
            f"Received: {msg_obj_string}, lsn:{msg.data_start}, send_time: {msg.send_time}")
        f = open(file_path, "w+")
        f.write(msg.payload)
        f.close()
        logging.info(f"Write change message to {file_path}")
        msg.cursor.send_feedback(flush_lsn=msg.data_start)
    source_cursor.consume_stream(write_to_file)


def handle_record(record, conn, tables):
    db_cursor = conn.cursor()
    file_path = record["file_path"]
    logging.debug(f'Handling message lsn: {record["lsn_0"]}, received_at: {record["received_at"]},'
                  f' data_size: {record["data_size"]}, file_path: {file_path}')
    message = json.load(open(file_path, 'r'))
    if "change" in message and message["change"]:
        for each in message["change"]:
            if each["kind"] == "insert":
                table = f'{each["schema"]}.{each["table"]}'
                if table not in tables:
                    continue
                columnnames = each["columnnames"]
                columnvalues = each["columnvalues"]
                insert_query = f'INSERT INTO {table} ({", ".join(columnnames)}) VALUES {tuple(columnvalues)} ;'
                db_cursor.execute(insert_query)
            if each["kind"] == "update":
                table = f'{each["schema"]}.{each["table"]}'
                if table not in tables:
                    continue
                columnnames = each["columnnames"]
                columnvalues = each["columnvalues"]
                keynames = each["oldkeys"]["keynames"]
                keyvalues = each["oldkeys"]["keyvalues"]
                set_names = ", ".join([f'{name} = %s' for name in columnnames])
                where_names = " AND ".join(
                    [f'{name} = %s' for name in keynames])
                update_query = f'UPDATE {table} SET {set_names} WHERE {where_names} ;'
                db_cursor.execute(update_query, tuple(
                    columnvalues + keyvalues))
            if each["kind"] == "delete":
                table = f'{each["schema"]}.{each["table"]}'
                if table not in tables:
                    continue
                keynames = each["oldkeys"]["keynames"]
                keyvalues = each["oldkeys"]["keyvalues"]
                where_names = " AND ".join(
                    [f'{name} = %s' for name in keynames])
                delete_query = f'DELETE FROM {table} WHERE {where_names} ;'
                db_cursor.execute(delete_query, tuple(keyvalues))
    log_update_query = f'UPDATE migration_cdc_logs SET proceeded_at = NOW() WHERE change_id = {record["change_id"]} ;'
    db_cursor.execute(log_update_query)
    conn.commit()
    os.remove(file_path)


def process_replication(migration_config, tables, tables_file):
    while not check_migration_jobs_all_success(tables_file):
        logging.debug("Migration jobs not fully complete, do not start process replication message. Sleep for 1 mintue.")
        time.sleep(60)
    logging.info("Initial loading is completed. Start processing replication messages ...")
    target_conn = build_db_connection(migration_config["target"])
    target_cursor = target_conn.cursor(cursor_factory=extras.DictCursor)
    # Fetch 1000 rows
    query = """
    SELECT * FROM migration_cdc_logs 
    WHERE proceeded_at IS NULL
    ORDER BY change_id
    LIMIT 1000;
    """
    while True:
        logging.debug("Fetching next 1000 unproceeded messages ...")
        target_cursor.execute(query)
        row_count = target_cursor.rowcount
        if not row_count:
            logging.debug("No unproceeded message. Sleep 30 seconds ...")
            time.sleep(30)
        logging.debug(f"Retrived unproceeded message(s): {row_count}.")
        row = target_cursor.fetchone()
        while row:
            record = dict(row)
            handle_record(record, target_conn, tables)
            row = target_cursor.fetchone()
