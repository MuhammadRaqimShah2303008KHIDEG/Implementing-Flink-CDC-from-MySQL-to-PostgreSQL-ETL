import psycopg2
from psycopg2.extras import execute_batch, execute_values
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()

# Environment variables
pg_host = os.getenv("HOST")
pg_database = os.getenv("DATABASE")
pg_user = os.getenv("USER")
pg_password = os.getenv("PASSWORD")
pg_port = os.getenv("PORT")
pg_schema = os.getenv("SCHEMA")


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# PostgreSQL connection config
PG_CONFIG = {
    "host": pg_host,
    "database": pg_database,
    "user": pg_user,
    "password": pg_password,
    "port": pg_port
}

def write_to_postgres_batch(rows, table_name, columns, conflict_key):
    if not rows:
        return

    conn = None
    try:
        conn = psycopg2.connect(**PG_CONFIG)
        cur = conn.cursor()

        col_list = ', '.join([f'"{c}"' for c in columns])
        update_list = ', '.join([f'"{c}" = EXCLUDED."{c}"'
                                for c in columns 
                                if c != conflict_key])

        sql = f'''
            INSERT INTO "{pg_schema}"."{table_name}" ({col_list}, "dbId")
            VALUES %s
            ON CONFLICT ("{conflict_key}", "dbId") DO UPDATE SET
            {update_list}
        '''

        # Convert rows(dict) → rows(list) in correct order
        values = [[row[c] for c in columns] + [2] for row in rows]
        print(f"Postgres CDC is Started {sql}: {values}")
        logger.info(f"🚀 Postgres CDC is Started {sql}: {values}")
        execute_values(cur, sql, values, page_size=100)
        conn.commit()

        logger.info(f"✔ {len(rows)} row(s) synced → {table_name}")

    except Exception as e:
        logger.error(f"❌ Postgres write error for {table_name}: {e}")
        if conn:
            conn.rollback()
        raise

    finally:
        if conn:
            cur.close()
            conn.close()


def processing(table_result, table_name, columns, conflict_key, batch_size=1):
    """
    Process Flink CDC stream and write to PostgreSQL.
    """
    logger.info(f"📥 Starting to process stream for {table_name}")
    batch = []
    
    try:
        with table_result.collect() as results:
            for row in results:
                # Convert Flink Row to dictionary
                row_dict = {columns[i]: row[i] for i in range(len(columns))}
                batch.append(row_dict)

                # Write batch when size is reached
                if len(batch) >= batch_size:
                    write_to_postgres_batch(batch, table_name, columns, conflict_key)
                    batch = []

            # Write leftover rows
            if batch:
                write_to_postgres_batch(batch, table_name, columns, conflict_key)
                
    except KeyboardInterrupt:
        logger.info(f"\n⏹️  Pipeline stopped for {table_name}")
        if batch:
            write_to_postgres_batch(batch, table_name, columns, conflict_key)
    except Exception as e:
        logger.error(f"❌ Pipeline error for {table_name}: {e}")
        raise
