import datetime

import generator


example_input1 = {
    'dag_id': 'test_dag',
    'owner': 'Ima',
    'start_datetime': datetime.datetime.now().strftime("%d/%m/%Y"),
    'schedule_interval': '0 0 * * *',
    'tags': ['testing', 'generator'],
    'source': {
        'conn_id': 'postgresql-prod',
        'dbms': 'PostgreSQL',
        'schema': 'SOURCE_SCHEMA_NAME'
    },
    'target': {
        'conn_id': 'clickhouse-prod',
        'dbms': 'ClickHouse',
        'schema': 'TARGET_SCHEMA_NAME',
    },
    'tables': {
        'VIEW_SDU': {
            'n_columns': 20,
            'sql': '''SELECT * FROM "SOURCE_SCHEMA_NAME"."VIEW_SDU" GROUP BY column_b LIMIT 10''',
            'columns_index_to_hash': [2],
            'include_load_time': True,
        }
    },
}


generator.generate_dag(**example_input1)
