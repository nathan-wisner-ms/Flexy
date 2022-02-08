from cmath import log
from configparser import ConfigParser
from datetime import datetime, timedelta
from os import strerror
import time
import urllib.parse
import subprocess
import csv
import logging
import sys
import re
import psycopg2
import math


def setup_logging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="[%(asctime)s][%(levelname)s] %(message)s",
        handlers=[
            logging.FileHandler(
                f'migration_logs_{datetime.now().strftime("%Y_%m_%d_%H_%M")}'
            ),
            logging.StreamHandler(sys.stdout),
        ],
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


def build_connection_string(db_config):
    conn_string = f'postgres://{urllib.parse.quote(db_config["user"])}:{urllib.parse.quote(db_config["password"])}'
    conn_string += f'@{urllib.parse.quote(db_config["host"])}:{urllib.parse.quote(db_config["port"])}'
    conn_string += f'/{urllib.parse.quote(db_config["database"])}'
    if db_config["sslmode"].strip():
        conn_string += f'?sslmode={urllib.parse.quote(db_config["sslmode"])}'
    return conn_string


def build_db_connection(db_config):
    db_conn = psycopg2.connect(
        database=db_config["database"], user=db_config["user"], password=db_config[
            "password"], host=db_config["host"], port=db_config["port"]
    )
    return db_conn


def verify_db_connections(migration_config):
    for section in ["source", "target"]:
        db_config = migration_config[section]
        db_conn_string = build_connection_string(db_config)
        print(
            f"Verifying connection to {section} db: {mask_credentail(db_conn_string)}")
        process = subprocess.Popen(
            f'psql "{db_conn_string}" -c "SELECT version() AS {section}_db_version;"', stdout=True, stderr=True, shell=True)
        process.communicate()
        if 0 != process.returncode:
            print(f"Failed to connect to {section} db: {db_conn_string}")
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
            if "CREATE INDEX" in line or "CREATE UNIQUE INDEX" in line or "ALTER INDEX" in line:
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


def create_list_of_tables(config_file, source_db_config):
    source_db_conn = build_db_connection(source_db_config)
    query_tables_order_by_size = """
     SELECT schemaname||'.'||tablename as schema_table, pg_size_pretty(pg_relation_size(schemaname||'.'||tablename::varchar)) AS data_size
FROM pg_catalog.pg_tables   
WHERE schemaname != 'information_schema' AND schemaname !=  'pg_catalog'  
ORDER BY pg_relation_size(schemaname||'.'||tablename::varchar) DESC;
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


def execute_migration_job(thread, message, migration_config):
    for i in range(1, 4):
        start_time = time.time()
        try:
            log_migration_jobs_status(thread, message, "started", None, None)
            exit_code, count = migrate_copy_table(
                thread, message, migration_config)
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


def migrate_copy_table(thread, message, migration_config):
    source_conn_url = build_connection_string(migration_config["source"])
    target_conn_url = build_connection_string(migration_config["target"])
    condition_string = ""
    table = message
    if "|" in message:
        table, column, type, value = message.split("|")
        condition_string = build_query_condition(column, type, value)
    cleanup_method = "DELETE FROM" if condition_string else "TRUNCATE"
    cleanup_command = f'psql "{target_conn_url}" -c "{cleanup_method} {table} {condition_string};"'

    logging_thread(
        f'{cleanup_method} target table: {table} {condition_string} ...',
        thread,
    )
    p0 = subprocess.Popen(
        cleanup_command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    p0.wait()
    stdout0, stderr0 = p0.communicate()
    logging_thread(
        f"System output of {cleanup_method} table {table} {condition_string}: {stdout0}", thread)
    if 0 != p0.returncode:
        error_message = stderr0.decode("utf-8")
        logging.error(
            f"Failed to {cleanup_method} for table: {message}, error: {mask_credentail(error_message)}")
        if "does not exist" in error_message:
            raise Exception(f"table {table} does not exist")
        return p0.returncode, None

    logging_thread(f"Copying table: {table} {condition_string} ... ", thread)
    select_query = f"\COPY (SELECT * from {table} {condition_string}) TO STDOUT;"
    read_query = f'psql "{source_conn_url}" -c "{select_query}"'
    write_query = f'psql "{target_conn_url}" -c "\COPY {table} FROM STDIN;"'
    copy_command = f"{read_query} | {write_query}"
    p1 = subprocess.Popen(copy_command, stdout=subprocess.PIPE,
                          stderr=subprocess.PIPE, shell=True)
    p1.wait()
    stdout1, stderr1 = p1.communicate()
    logging_thread(
        f"System output of \COPY table: {table} {condition_string}: {stdout1}", thread)
    if 0 != p1.returncode:
        logging.error(
            f"Failed to copy for table: {table}, error: {mask_credentail(stderr1.decode('utf-8'))}")
        return p1.returncode, None

    copied_count = int(stdout1.decode(
        'utf-8').replace("COPY", "").replace("\n", ""))
    logging_thread(f"Comparing row count from source table: {table} {condition_string} ...", thread)
    source_conn = build_db_connection(migration_config["source"])
    source_cursor = source_conn.cursor()
    source_cursor.execute("""SET max_parallel_workers_per_gather = 30;""")
    source_conn.commit()
    source_cursor.execute(f"""SELECT COUNT(1) FROM {table} {condition_string};""")
    source_count = source_cursor.fetchone()[0]
    source_conn.close()
    if source_count == copied_count:
        logging_thread(f"Row count matched for copied: {table} {condition_string}", thread)
        return p1.returncode, copied_count
    else:
        logging_thread(f"Row count didn't match for copied: {table} {condition_string}. Retry.", thread)
        return 2, copied_count


def log_migration_jobs_status(thread, message, status, duration, count):
    f = open(f"migration_jobs_status.tsv", "a+")
    writer = csv.writer(f, delimiter="\t")
    writer.writerow([message, status, datetime.now(), duration, thread, count])
    f.close()


def mask_credentail(message):
    messages = str(message).split()
    return " ".join([re.sub(r'postgres://.*@', 'postgres://<USERNAME>:<PASSWORD>@', m) for m in messages])
