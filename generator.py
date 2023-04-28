import os
import datetime

from jinja2 import Environment, FileSystemLoader
from jinja2 import Template



def get_jinja_template(template_filename: str) -> Template:
    """
    Returning jinja2 template to render

    :return: jinja template
    """
    file_dir = os.path.abspath(os.getcwd()+'/')
    env = Environment(loader=FileSystemLoader(file_dir))
    return env.get_template(f'templates/{template_filename}')


def generate_dag(dag_id: str, owner: str,
                 tags: list, start_datetime: datetime.datetime,
                 schedule_interval: str,
                 source: dict, target: dict,
                 tables: dict) -> None:
    """
    Function to generate DAG
    
    :dag_id: id of DAG
    :owner: owner of DAG
    :tags: tags of DAG
    :start_datetime: start datetime of DAG
    :schedule_interval: cron schedule expression of DAG for update regularity (generate cron schedule expression with `https://crontab.guru/`)
    :source: DBMS setting of source Database, value:
        {
            :conn_id: [str], database secrets id in Airflow connections 
            :dbms: [str], list of available source dbms ['ClickHouse', 'PostgreSQL', 'MySQL', 'MSSQL'] 
            :schema: [str], schema in source database
        }
    :target: DBMS setting of target Database, value:
        {
            :conn_id: [str], database secrets id in Airflow connections 
            :dbms: [str], list of available target dbms ['ClickHouse', 'PostgreSQL']    
            :schema: [str], schema in source database
        }
    :tables: tables in target|source database, TABLE NAME AUTOMATICALLY CHANGE TO UPPER REGISTER IN TARGET DATABASE, value:
        {
            :table_name: [str], value:
            {
                :n_columns: [int], amount of columns in target table database(to generate `%s` in insert sql script), not required for Clickhouse
                            `%s` - value placeholder string formating in python
                :sql: [str], custom sql script needs if table name in source difference or selects from sql script
                :columns_index_to_hash: [List[int]], indexes of columns in table
                :include_sdu_load_in_dt: [bool], include SDU_LOAD_IN_DT column into data
            }
        }
    """
    inputs = {
        'dag_id': dag_id,
        'owner': owner,
        'tags': tags,
        'start_datetime': start_datetime,
        'schedule_interval': schedule_interval,
        'source': source,
        'target': target,
        'tables': tables,
        'contain_load_time': any([val['include_load_time'] for val in tables.values()]),
        'contain_hash_columns': any([len(val['columns_index_to_hash']) for val in tables.values()]),
    }

    template = get_jinja_template(template_filename='dag_template.jinja2')

    with open(f"dags/{inputs['dag_id']}.py", 'w', encoding="utf-8") as f:
        f.write(template.render(inputs))
        print(f'[+] File saved as `./dags/{dag_id}.py`')
