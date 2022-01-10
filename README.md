# Flexy
## Introduction
A command-line tool for faster PG to PG offline parallel migration. Run from any VM that can use `psql` to connect to migrating db servers.

---
## Set up
* Download and upload the folder to your VM.<br>
* Install the following if not yet installed.
    *  python3
    * postgresql-client   
* `cd` to the folder  
---
### Pre-Migration
1. Config: Fill source/target db configuration in the `config.ini`. Can copy to any new config file for different migration project but must follow the template.
2. Migrate Schema (skip if tables already migrated)
    * Create target database in target db server
    * Run command:<br>
     ```python3 pre_migration.py --config-file=yourconfigfile --function=migrate_schema```
       * **NOTE:**
            * required flag: `--config-file`, `--function`
            * optional flag: ``--indexes=True`` (default: `False`)
            * This command will generate 3 files:
                * `schema_{config-file-name}.sql`
                * `schema_no_indexes_{config-file-name}.sql`
                * `schema_indexes_only_{config-file-name}.sql`
        <br> if `--indexes=True`: migrate with indexes
        <br> else: migrate with no indexes
3. Create a file for selected tables to migrate:
    * Four different string formats are accepted in each line <br>
        | line in file | translation in program |
        | -----------  |----------- |
        | `schemaname.tablename` |`SELECT * FROM schemaname.tablename`
        |`schema.tablename\|columnname\|I\|interval_0,interval_1`|`SELECT * FROM schemaname.tablename WHERE columnname >= 'interval_0' AND columnname <= 'interval_1'`|
        | `schema.tablename\|columnname\|I\|interval_0,`| `SELECT * FROM schemaname.tablename WHERE columnname >= 'interval_0'`|
        |`schema.tablename\|columnname\|V\|value`| `SELECT * FROM schemaname.tablename WHERE columnname = 'value'`|
    * Example file:
         ```
         public.orders
         public.items|id|I|1,50000000
         public.items|id|I|50000000,
         public.customers|country|V|US
         ```  
    * **NOTE**: <br>
         To speed up the migration process, you should generate the file sort by data size in descending 
          orders. You can query it by:
         ````
        SELECT schemaname||'.'||relname as schema_table_name, 
        pg_relation_size(relid) as data_size, pg_size_pretty(pg_relation_size(relid)) as pretty_data_size
        FROM pg_catalog.pg_statio_user_tables
        ORDER BY data_size DESC;
         ````
         It's strongly recommend that you will chunk a large table into multiple migration jobs based on an indexed watermark column (e.g., id, timestemp) or some value that can segment the tables. The recommend size is under 20 GB for each partition. To check the partioned data size, you query it by:
        ````
        CREATE TEMPORARY TABLE subset1 AS (
            SELECT * FROM table1 WHERE ...
        );
        SELECT pg_size_pretty(pg_relation_size('subset1'));
        ```` 
        
        Currently, you will be responsible for the validation of partion values, make sure the sets not overlap.
4. Change server parameters in target database server:
    * Disable triggers and foreign keys validation by change server parameter `session_replication_role` TO `REPLICA` in target database server in global server level
    * Increase `max_wal_size` to largest value
    * Increase `maintenance_work_mem` to largest value
6. (optional) Scale up vCores in source and target db server
---
## Migration
* Run command:
    ```
    python3 migrate.py --config-file=config.ini --queue-file=yourtablesfile
    ```
    * required flag: `--config-file`, `--function`
    * optional flag: `--parallel-number` (default: `20`)
    * **Note:**
        * For large data migration, you should run from `screen` session
        * When the program completes sending data over network, it still requires sometime for target db server to finish writing to disk. Since each (partition) table is committed in one transaction, if query `SELECT * FROM tablename {WHERE .. } LIMIT 1;` can return any result, it means this migration job is finally completed.  
* Tracking job status
    * The status of each migration job will be appended to file `migration_jobs_status.tsv`.
* Logging
    * You can check logs saved in `migration_logs_{date}` during/after migration.
* Monitor
    * Monitor CPUs in your source/target db servers
* Add indexes during migration
    * To reduce whole migration time, you can add indexes back for tables already finally complete while still migrating for other tables
* Re-Run
    * Any migration jobs logged as `success` in `migration_jobs_status.tsv` will not be re-migrated
    * To re-run the whole migration process, remove `migration_jobs_status.tsv` and start with a fresh database
---
## Post Migration
* Add all indexes back
* Alter sequences restart ids
* Revert changed server parameter values
---
## Feedback
* Send email to lingjiang@microsoft.com if you encounter any issues/question or have any feature request. Thank you!