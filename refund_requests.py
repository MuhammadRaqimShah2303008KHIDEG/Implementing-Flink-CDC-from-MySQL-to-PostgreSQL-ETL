from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import DataTypes
from pyflink.common import Row
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
from pg_process import *
load_dotenv()

mysql_host = os.getenv("MYSQL_HOST")
mysql_port = os.getenv("MYSQL_PORT") 
mysql_user = os.getenv("MYSQL_USER") 
mysql_password = os.getenv("MYSQL_PASSWORD") 
mysql_database = os.getenv("MYSQL_DATABASE") 

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define column lists for each table
refund_requests = {
        'columns': ["id", "mobileNo", "transactionId", "referenceNumber", "status", 
                   "createdDate", "cnic", "amount", "payment_ids", "isPartial", "updatedDate", 
                   "merchantId", "operatorId", "payableAmount", "chargedAmount", "payableRefund", 
                   "feeAmount", "feeAppliedState", "mode", "refundUserKey", "chargebackId", 
                   "isManualRefund", "userKey", "currency"],
        'pk': 'id'
    }
def process_table_thread(table_name, table_config):
    """
    Process a single table in a separate thread with its own TableEnvironment.
    """
    try:
        logger.info(f"🚀 Starting CDC pipeline for {table_name}")
        
        # Create a NEW TableEnvironment for this thread
        env_settings = EnvironmentSettings.in_streaming_mode()
        t_env = TableEnvironment.create(environment_settings=env_settings)
        
        # Setup all CDC tables in this environment
        # setup_cdc_tables(t_env)
        t_env.execute_sql(f"""
    CREATE TABLE `refund_requests` (
        `id` BIGINT NOT NULL,
        `mobileNo` STRING,
        `transactionId` BIGINT,
        `referenceNumber` STRING,
        `status` INT,
        `createdDate` TIMESTAMP(0),
        `cnic` STRING,
        `amount` DECIMAL(11,2),
        `payment_ids` STRING,
        `isPartial` INT,
        `updatedDate` TIMESTAMP(0),
        `merchantId` BIGINT,
        `operatorId` BIGINT,
        `payableAmount` DECIMAL(11,2),
        `chargedAmount` DECIMAL(11,2),
        `payableRefund` DECIMAL(11,2),
        `feeAmount` DECIMAL(11,2),
        `feeAppliedState` INT,
        `mode` STRING,
        `refundUserKey` STRING,
        `chargebackId` BIGINT,
        `isManualRefund` INT,
        `userKey` STRING,
        `currency` STRING,
        PRIMARY KEY (`id`) NOT ENFORCED
    ) WITH (
            'connector' = 'mysql-cdc',
            'hostname' = '{mysql_host}',
            'port' = '{mysql_port}',
            'username' = '{mysql_user}',
            'password' = '{mysql_password}',
            'database-name' = '{mysql_database}',
            'table-name' = 'refund_requests',
            'server-time-zone' = 'Asia/Karachi',
            'scan.startup.mode' = 'latest-offset'
    )
    """)

        
        # Execute query for this specific table
        table_result = t_env.execute_sql(f"SELECT * FROM {table_name}")
        # print(f"Postgres CDC is Started: {sql}")
        # print(f"FLINK CDC is Started ON {table_name}: {table_result}")
        # Process the stream
        processing(
            table_result=table_result,
            table_name=table_name,
            columns=table_config['columns'],
            conflict_key=table_config['pk'],
            batch_size=1
        )
        
    except Exception as e:
        logger.error(f"❌ Thread error for {table_name}: {e}")
        raise



if __name__ == "__main__":
    print("\n✅ Starting Multi-Table CDC Pipeline...")
    print("📊 Monitoring MySQL changes across refund_requests table...\n")
    process_table_thread('refund_requests',refund_requests)