from typing import List, Dict, Any
import psycopg2

def exists(db_name: str) -> bool:
    raise NotImplementedError

def create_psql_db(data_file_path: str, db_name: str) -> None:
    raise NotImplementedError

def connect(db_name: str) -> Dict[str, Any]:
    '''
    Connects to the given database
    Returns a dict:
    conn       : db connection object
    measures   : the measures of the db
    dimensions : the dimensions of the db
    table name : ???
    '''
    conn = psycopg2.connect(database = db_name, user = "mili")
    return {'conn': conn,
			'measures': get_measures(db_name),
			'dimensions': get_dimensions(db_name)}

def get_measures(db_name: str) -> List:
    if db_name == 'census':
        return ['age', 'fnlwgt', 'education-num', 'capital-gain',
                'capital-loss', 'hours-per-week']
    else:
    	raise NotImplementedError

def get_dimensions(db_name: str) -> List:
    if db_name == 'census':
        return ['workclass', 'education', 'occupation', 'relationship',
                'race', 'sex', 'native-country']
    else:
    	raise NotImplementedError

def select_query(conn, query: str) -> List:
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    return rows

