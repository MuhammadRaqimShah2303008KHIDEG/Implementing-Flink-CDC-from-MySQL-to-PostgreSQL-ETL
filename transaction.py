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
transaction= {
        'columns': ["transactionID", "ACR", "amount", "codeOTP", "createdDate", "mobileNo", 
                   "status", "transactionType", "updatedDate", "userKey", "merchantID", 
                   "operatorID", "productID", "transactionPeriod", "processed", "action", 
                   "accountId", "integrationId", "productReference", "acknowledge", "refund", 
                   "currency", "merchantUrl", "paymentToken", "transactionStatusID", "cnic", 
                   "sessionId", "isDirectPay", "auto_recon_flag", "payableAmount", "feeAmount", 
                   "feeAppliedState", "orderPrefix", "credentials"],
        'pk': 'transactionID'
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
        CREATE TABLE `transaction` (
            `transactionID` BIGINT NOT NULL,
            `ACR` STRING,
            `amount` DECIMAL(11,2),
            `codeOTP` STRING,
            `createdDate` TIMESTAMP(0),
            `mobileNo` STRING,
            `status` INT,
            `transactionType` INT,
            `updatedDate` TIMESTAMP(0),
            `userKey` STRING,
            `merchantID` BIGINT,
            `operatorID` BIGINT,
            `productID` BIGINT,
            `transactionPeriod` STRING,
            `processed` INT,
            `action` INT,
            `accountId` STRING,
            `integrationId` STRING,
            `productReference` STRING,
            `acknowledge` INT,
            `refund` INT,
            `currency` STRING,
            `merchantUrl` STRING,
            `paymentToken` STRING,
            `transactionStatusID` STRING,
            `cnic` STRING,
            `sessionId` STRING,
            `isDirectPay` INT,
            `auto_recon_flag` INT,
            `payableAmount` DECIMAL(11,2),
            `feeAmount` DECIMAL(11,2),
            `feeAppliedState` INT,
            `orderPrefix` STRING,
            `credentials` BIGINT,
            PRIMARY KEY (`transactionID`) NOT ENFORCED)
            WITH (
                'connector' = 'mysql-cdc',
                'hostname' = '{mysql_host}',
                'port' = '{mysql_port}',
                'username' = '{mysql_user}',
                'password' = '{mysql_password}',
                'database-name' = '{mysql_database}',
                'table-name' = 'transaction',
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
    print("📊 Monitoring MySQL changes across transaction table...\n")
    process_table_thread('transaction',transaction)