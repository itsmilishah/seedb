from typing import List
from db_utils import *

class SeeDB(object):
    '''
    Assumptions:
    1. the table has an index column with unique, continuous
    integer sequence
    2. the table has no missing values
    '''

    def __init__(self, db_name: str) -> None:
        '''
        Connects to the database of the given name
        '''
        conn_data = connect_to_db(db_name)
        self.db, self.measures, self.attributes, self.table_name,\
                self.n_tuples, self.columns = (conn_data['conn'],
                        conn_data['measures'], conn_data['dimensions'],
                        conn_data['table_name'], conn_data['count'],
                        conn_data['columns'])
        self.func = ['average', 'min', 'max', 'sum', 'count']

    def recommend_views(self, query: str, reference: str) -> List:
        # get a batch
        # get Dq and Dr
        # now view loops start (a=8, m=6, f=5)
        # boolean lookup table
        data_batch = batch_generator(i, 1000)
        raise NotImplementedError


    def visualize(self, measure_name: str, function_name: str,
            attribute_name: str, attribute_values: List) -> None:
        raise NotImplementedError

    def batch_generator(self, start, size):
        '''
        Divides the data into mini-batches (indexes ??)
        and returns the mini-batches (indexes ??) for query.
        With feauture-first grouping ???
        '''
        return (start, start + size)
        # count = self.database(get tuple count)

    def utility(self, view):
        raise NotImplementedError

    def prune(self):
        raise NotImplementedError
