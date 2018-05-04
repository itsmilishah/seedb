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
        Connects to the database of the given name and
        takes lists of measures and dimensions
        '''
        conn_data = connect_to_db(db_name)
        self.database, self.measures, self.dimensions, self.table_name = (
                conn_data['conn'], conn_data['measures'],
                conn_data['dimensions'], conn_data['table_name'])
        # self.n_tuples =

    def recommend_views(self, query, reference):
        raise NotImplementedError

    def visualize(self, ):

    def batch_generator(self):
        '''
        Divides the data into mini-batches (indexes ??)
        and returns the mini-batches (indexes ??) for query.
        With feauture-first grouping ???
        '''
        # count = self.database(get tuple count)
        raise NotImplementedError

    def utility(self, view):
        raise NotImplementedError

    def prune(self):
        raise NotImplementedError
