# Flexy
## Introduction
A command-line tool for faster PG to PG offline and online parallel migration. Run from any VM that can use `psql` to connect to migrating db servers.
## Set up
* In your vm workspace, 
```
    git clone https://github.com/ljiang001/Flexy.git\
```
* Install the following if not yet installed.
    * python3
    * postgresql-client
    * pip3 or pip 
* `cd` to the folder
* Install python packages:


    * ```pip3 install -r requirements.txt ``` **or** ```pip install -r requirements.txt``` 
* Also install using **sudo apt** to update python to the latest version
    * ```sudo apt update```
    * ```sudo apt install python3.9 ```
    
### Pre-Migration
1. Config: Fill source/target db configuration in the `config.ini`. Can copy to any new config file for different migration project but must follow the template.
 * The config details can be found in the **Connection String** settings for each database under **PSQL**. The details for **source** should be the Single Server and the details for **destination** should be the Flexible Server

**Flexible Server Connection String Example**
![Flexible Server Connection String Example](/media/FSPG%20-%20Migration%20connection%20string.png)

**Single Server Connection String Example**
![Single Server Connection String Example](/media/SSPG%20-%20Connection%20String.png)

2. Create `logs` dir or other dirctory to write logs to, update `log_dir` in `config.ini` file
3. Migrate Schema (skip if tables already migrated)

    * Create target database in target db server
    * Migrate schema:<br>
     ```
     python3 pre_migration.py --config-file=yourconfigfile --function=migrate_schema
     ```
    * Example:
     ```
     python3 pre_migration.py --config-file=config.ini --function=migrate_schema
     ```
    
    * *Possible Error Message*
        * An error detailing ``` no pg_hba.conf exists for {your ip address} ``` means you need to allow your IP inside your database 
            * This can be done by going into Networking for Flexible Server and Connection Secuirty for Single Server and **Allow public access to Azure Services** along with **Add Current Client IP**

               This is what it looks like for **Single Server**
               ![Single Server](/media/SSPG%20-%20Network%20Settings.png)
               
               This is what it looks like for **Flexible Server**
               ![Flexible Server](/media/FSPG%20-%20Network%20Settings.png)
   * *Flags:*
        * required:
            *  `--function` (`-f`)(choices: `migrate_schema`, `create_list`, `create_parts`, `create_slot`, `drop_slot`)
            * `--config-file`(`-c`)

        * optional:
            * `--indexes=True`(`-i`) (default: `False`)
    * This command will generate 3 files:
        * `schema_{config-file-name}.sql`
        * `schema_no_indexes_{config-file-name}.sql`
        * `schema_indexes_only_{config-file-name}.sql`
    
        if `--indexes=True`: migrate with indexes
        <br>else: migrate with no indexes
3. Create a list of all tables from the source database order by descending data size
    * Use function to create list:
    ```
     python3 pre_migration.py --config-file=yourconfigfile --function=create_list
    ``` 
    * This command will generate 2 files:
        * `tables_{config-file-name}`
        * `tables_size_{config-file-name}.tsv`
    * Query used in the function: 
    ```
    SELECT schemaname||'.'||tablename as schema_table, pg_size_pretty(pg_relation_size(schemaname||'.'||tablename::varchar)) AS data_size
    FROM pg_catalog.pg_tables   
    WHERE schemaname != 'information_schema' AND schemaname !=  'pg_catalog'  
    ORDER BY pg_relation_size(schemaname||'.'||tablename::varchar) DESC;
    ```
    * You can also manually create a file containing tables. see format in step 5.  
4. (optional) Create partitions for large tables based on an indexed monotonically increasing column (e.g., id column) (OR) a timestamp column (e.g., created_at, updated_at, etc). 
    * Create a file containing tables and columns, each line formatted as following:
    ```
    schemaname.tablename|columnname
    ```
    * Create parts:
    ```
    python3 pre_migration.py --config-file=yourconfigfile --function=create_parts --tables-file=yourtablefile
    ```
    * This command will generate a file containing a list of migration job string that can be used in the `migrate.py` function, which divide a large table into multiple migration jobs based on the watermark column and table size.  
    * Create the index for the column if not yet created in source and target tables 
    * **Note**: 
        * strongly recommand for table size large than 100 GB
        * Recommand for table size large than 50 GB
        * The function assumes the data are evenly distributed by the id/timestamp. The average size of each chunk will be 10 GB. 
5. Combine the file from step 3 and 4, create a file for selected tables to migrate:
    * Four different string formats are accepted in each line <br>
        | line in file | translation in program |
        | -----------  |----------- |
        |`schemaname.tablename` |`SELECT * FROM schemaname.tablename`|
        |`schemaname.tablename\|columnname\|I\|,interval_0`|`SELECT * FROM schemaname.tablename WHERE columnname < 'interval_1'` <br>(no lower bound, exclusive upper bound)|
        |`schemaname.tablename\|columnname\|I\|interval_0,interval_1`|`SELECT * FROM schemaname.tablename WHERE columnname >= 'interval_0' AND columnname < 'interval_1'` <br>[inclusive lower bound, exclusive upper bound)|
        |`schemaname.tablename\|columnname\|I\|interval_0,`| `SELECT * FROM schemaname.tablename WHERE columnname >= 'interval_0'` <br> [inclusive lower bound, no upper bound)|
        |`schemaname.tablename\|columnname\|V\|value`| `SELECT * FROM schemaname.tablename WHERE columnname = 'value'`|
        |`schemaname.tablename\|columnname\|V\|NULL`| `SELECT * FROM schemaname.tablename WHERE columnname IS NULL`|
    * Example file:
         ```
         public.orders
         public.items|id|I|1,50000000
         public.items|id|I|50000000,
         public.customers|country|V|US
         ```  
    * **Tips**: <br>
        * Put the large partitioned tables together on the top. The rest should sort by descending data size. 
4. Disable triggers and foreign keys validation
    * In target server: Change server parameter `session_replication_role` TO `REPLICA` in global server level
5. Optional performance tuning:
    * In target server: Increase `max_wal_size` to largest value
    * In target server: Increase `maintenance_work_mem` to largest value
    * In target & source servers: Increase `max_parallel_workers_per_gather`
    * In target & source servers: Scale up vCores
7. Create consistent snapshot before migration:
    * Online migration:
        * Create a directory to save incoming WAL message in local, and update the `cdc_dir` in `config.ini`
        * start a new screen session:
        ```
        screen -S create_slot
        ```
        * Create replication slot and export consistent snapshot
        ```
        python3 pre_migration.py --config-file=yourconfigfile --function=create_slot
        ```
        * Save and exit the session: keyboard `ctrl+A+D`
        * **Note**:
            * This screen session need to be keep live in the initial data loading process
            * to restart the whole migration process and re-create slot, drop slot first
            ```
            python3 pre_migration.py --config-file=yourconfigfile --function=drop_slot
            ```
            * and `rm migration_jobs_status.tsv`
    * Offline migration
        * TODO

| :warning: WARNING          |
|:---------------------------|
| Schemas and Tables must be created in target database before starting migration   |


## Migration
* Migrate tables in Parallel
    * Offline migration:
        ```
        python3 migrate.py --config-file=yourconfigfile --queue-file=yourtablesfile
        ````
    * Online migration:
        1. Create a new screen session to start receiving replication message
        ```
        screen -S start_replication
        ```
        2. In the screen:
        ```
        python3 migrate.py --config-file=yourconfigfile --queue-file=yourtablesfile --replication=start
        ```
        3. Save and exit the screen: hit `ctrl+A+D`
        4. Start a new screen session:
        ```
        screen -S migrate
        ```
        5. In the screen session, start initial data loading and start consuming message after all complete. 
        ```
        python3 migrate.py --config-file=yourconfigfile --queue-file=yourtablesfile --replication=consume
        ```
        6. Save and exit the screen.
        * **Note**:
          * If error in initial migration stage, re-run the command to resume migration, the function skip migration for already succeeded tables, and will start consuming replication after all successfully migrated
          * If seeing error in consuming replication stage, check the file where it fails. and you can remove the file if the change message in the file can be ignore, then re-run the command to continue.
          * Warnning: can only run one consume command at a time. Make sure no same process is running at the same time, check by `ps aux | grep consume`

    * *Flags* in migrate.py:
        * use `python3 migrate.py --help` to see details
        * required:
            * `--config-file` (`-c`)
            *  `--queue-file`(`-q`)
        * optional:
            * `--number-thread` (`-n`) (default: `20`)
            * `--log-level` (`-l`) (default:`INFO`, choice: `INFO`, `DEBUG`)
            for online migration:
            * `--replication` (`-r`) (choice: consume, start)
            * `--batch-size` (`-b`) (default: `500`)
    * In each migration, each table will be truncated or deleted for the partitoned part
    * Tracking job status
        * The status of each migration job will be appended to local file `migration_jobs_status.tsv`.
    * Re-Run
        * Any migration jobs logged as `success` in `migration_jobs_status.tsv` will not be re-migrated
        * To re-run the whole migration process, remove `migration_jobs_status.tsv`
* Logging
    * You can check logs saved in `{logs_dir}/logs_{function}_{date}_{PROJECT_ID}` during/after migration.
* Monitor
    * Monitor CPUs in your source/target db servers
* Add indexes during migration
    * To reduce entire migration spanning time, you can add indexes back for tables that already completely migrate while still running migration for other tables
## Post Migration
* Add all indexes back
* Alter sequences restart ids
* Revert changed server parameter values
* Scale down resources that are not needed
## Feedback
* Send email to lingjiang@microsoft.com if you encounter any issues/question or have any feature request. Thank you!