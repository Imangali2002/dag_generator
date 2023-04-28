import datetime
from typing import Union, List, Optional

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.connection import Connection
from airflow.models.baseoperator import chain
from airflow.models import Variable

import hashlib
import psycopg2
from clickhouse_driver import connect as clickhouse_connect




# -- Connections --

def get_conn_secrets(conn_id: str) -> any:
    """
    Returning database connection secrets
    """
    try:
        return Connection.get_connection_from_secrets(conn_id=conn_id)
    except Exception as ex:
        raise '[-] Connection secrets not found for conn_id={conn_id}'


def get_source_conn(conn_id: str) -> any:
    """
    Setting up connection with source database
    """
    source_conn_secrets = get_conn_secrets(conn_id)
    try:
        return psycopg2.connect(dbname=source_conn_secrets.schema, user=source_conn_secrets.login, password=source_conn_secrets.password, host=source_conn_secrets.host, port=source_conn_secrets.port)
    except Exception as ex:
        print('[-] Connection failed with source')
        return None


def get_target_conn(conn_id: str) -> any:
    """
    Setting up connection with target database
    """
    target_conn_secrets = get_conn_secrets(conn_id)
    try:
        return clickhouse_connect(f'clickhouse://{target_conn_secrets.login}:{target_conn_secrets.password}@{target_conn_secrets.host}:{target_conn_secrets.port}')
    except Exception as ex:
        print('[-] Connection failed with target')
        return None


# -- Operations --

def hash(val, hash_keyword):
    """
    Hashing data with SHA256 algorithm in Uppercase format
    """
    if val is None:
        hash_data = f"{hash_keyword}".encode("utf-8")
    else:
        hash_data = f"{str(val).lower()}{hash_keyword}".encode("utf-8")
    result = hashlib.sha256(hash_data).hexdigest()
    return result.upper()


def hash_columns(vals, indexes, hash_keyword):
    """
    Looping columns by index to Hash
    """
    tmp = list(vals)
    for index in indexes:
        tmp[index] = hash(vals[index], hash_keyword)
    return tuple(tmp)


def insert_values(cursor: any, data: List[Union[list, tuple]], schema: str, table: str) -> None:
    """
    Inserting data into target database
    """
    sql = f"""INSERT INTO "{schema}"."{table}" VALUES"""
    cursor.execute(sql, data)


def truncate_table(cursor, schema: str, table: str) -> None:
    """
    Truncating table in target database
    """
    try:
        sql = f"""TRUNCATE TABLE "{schema}"."{table}";"""
        cursor.execute(sql)
        print(f'[+] Table `{schema}.{table}` truncated!')
    except Exception as ex:
        print('[-] Failed on trunacte')
        raise ex



def extract_and_load(source_schema: str, source_table: str,
                     target_schema: str, target_table: str,
                     source_conn_id: str, target_conn_id: str,
                     sql: Optional[str],
                     columns_index_to_hash: list,
                     batch_size: int = 50000, truncate: bool = False) -> None:
    HASH_KEYWORD = Variable.get("hash_password")    # password to hash data
    SDU_LOAD_IN_DT = datetime.datetime.now()# + datetime.timedelta(hours = 6)

    conn_source = get_source_conn(source_conn_id)
    conn_target = get_target_conn(target_conn_id)

    if conn_source is not None and \
       conn_target is not None:

        try:
            with conn_source.cursor() as cursor_source, \
                 conn_target.cursor() as cursor_target:

                sql = f"""SELECT * FROM "{source_schema}"."{source_table}";""" if sql is None else sql
                cursor_source.execute(sql)
                total_len = 0

                while True:
                    data = cursor_source.fetchmany(batch_size)

                    if len(data) == 0:
                        break

                    if truncate:
                        truncate_table(cursor_target, target_schema, target_table)
                        truncate = False

                    # adding SDU_LOAD_IN_DT column
                    data = [[*list(i), SDU_LOAD_IN_DT] for i in data]
                    # hashing date
                    data = [*map(lambda row: hash_columns(row, indexes=list(columns_index_to_hash), hash_keyword=HASH_KEYWORD), data)]

                    total_len += len(data)
                    insert_values(cursor_target, data, target_schema, target_table)

            conn_source.commit()
            conn_target.commit()

            print(f'[+] Compleated migration from `{source_schema}.{source_table}` to `{target_schema}.{target_table}` total length={total_len}')

        except Exception as ex:
            raise '[-] Problems with processing the ETL processor'
        finally:
            conn_source.close()
            conn_target.close()
    else:
        raise '[-] Problems with connections'



def main(**kwargs) -> None:
    extract_and_load(
        source_schema=kwargs['source_schema'],
        source_table=kwargs['source_table'],
        target_schema=kwargs['target_schema'],
        target_table=kwargs['target_table'],
        batch_size=kwargs['batch_size'],
        truncate=kwargs['truncate'],
        sql=kwargs['sql'],
        columns_index_to_hash=kwargs['columns_index_to_hash'],
        target_conn_id=kwargs['target_conn_id'],
        source_conn_id=kwargs['source_conn_id'],
    )



with DAG(
    dag_id='test_dag',
    start_date=datetime.datetime.strptime('28/04/2023', '%d/%m/%Y'),
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=['testing', 'generator'],
    default_args={
        'owner':'Ima',
        'depends_on_past': False
    }
) as dag:

    TABLES = {
        'VIEW_SDU': {
            'n_columns': 20,
            'sql': 'SELECT * FROM "SOURCE_SCHEMA_NAME"."VIEW_SDU" GROUP BY column_b LIMIT 10',
            'columns_index_to_hash': [2],
            'include_load_time': True,
        }
    }
    SOURCE_SCHEMA = 'SOURCE_SCHEMA_NAME'
    TARGET_SCHEMA = 'TARGET_SCHEMA_NAME'

    SOURCE_CONN_ID = 'postgresql-prod'
    TARGET_CONN_ID = 'clickhouse-prod'


    tasks = []

    for key, value in TABLES.items():

        params = {
            'source_schema': SOURCE_SCHEMA,
            'source_table': key,
            'target_schema': TARGET_SCHEMA,
            'target_table': key.upper(),
            'batch_size': 50000,
            'truncate': True,
            'source_conn_id': SOURCE_CONN_ID,
            'target_conn_id': TARGET_CONN_ID,
            'sql': value['sql'],
            'columns_index_to_hash': value['columns_index_to_hash']
        }

        task_ = PythonOperator(
            task_id=f"{TARGET_SCHEMA}.{key}",
            trigger_rule='all_done',
            python_callable=main,
            op_kwargs=params
        )

        tasks.append(task_)


    chain(*tasks)