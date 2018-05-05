import numpy as np
from typing import List, Dict, Any
import psycopg2
from scipy.stats import entropy

def create_psql_db(data_file_path: str, db_name: str) -> None:
    raise NotImplementedError

def connect_to_db(db_name: str) -> Dict[str, Any]:
    '''
    Connects to the given database
    Returns a dict:
    conn       : db connection object
    measures   : (list) the measures of the db
    dimensions : (list) the dimensions of the db
    table name : (str)
    count      : (int)
    '''
    conn = psycopg2.connect(database = db_name, user = "surya")
    table_name = get_table_name(db_name)
    return {'conn': conn,
            'table_name': table_name,
            'measures': get_measures(db_name),
            'dimensions': get_dimensions(db_name),
            'count': int(select_query(conn, ('select count(*) from ' + table_name))[0][0]),
            'columns': get_columns(db_name)}

def get_table_name(db_name: str) -> str:
    if db_name == 'seedb':
        return 'census'
    else:
    	raise NotImplementedError

def get_measures(db_name: str) -> List[str]:
    if db_name == 'seedb':
        return ['age', 'fnlwgt', 'capital_gain', 'capital_loss',
                'hours_per_week']
    else:
    	raise NotImplementedError

def get_dimensions(db_name: str) -> List[str]:
    if db_name == 'seedb':
        return ['workclass', 'education', 'occupation', 'relationship',
                'race', 'sex', 'native_country', 'salary', 'marital_status']
    else:
    	raise NotImplementedError

def get_columns(db_name: str) -> List[str]:
    if db_name == 'seedb':
        return ['id', 'age', 'workclass', 'fnlwgt', 'education',
                'education_num', 'marital_status', 'occupation',
                'relationship', 'race', 'sex', 'capital_gain',
                'capital_loss', 'hours_per_week', 'native_country',
                'salary']
    else:
    	raise NotImplementedError

def select_query(conn, query: str, return_float=False) -> List[Any]:
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    if return_float==True:
        rows = np.array(rows)
        rows[...,-1] = rows[...,-1].astype(float)
    return rows

def get_distinct_values(conn, table_name: str, attribute_name: str) -> List:
    query = 'select distinct(' + attribute_name + ') from ' + table_name
    rows = select_query(conn, query)

def kl_divergence(p1, p2):
    return entropy(p1, p2)

