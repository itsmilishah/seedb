from typing import List
from db_utils import *

class SeeDB(object):

    def __init__(self, db_name: str) -> None:
        '''
        Connects to the database of the given name and
        takes lists of measures and dimensions
        '''
        conn_data = connect_to_db(db_name)
        self.database, self.measures, self.dimensions = (conn_data['conn'],
                conn_data['measures'], conn_data['dimensions']
        # self.n_tuples =

    def batch_generator(self):
        '''
        Divides the data into mini-batches (indexes ??)
        and returns the mini-batches (indexes ??) for query.
        With feauture-first grouping ???
        '''
        # count = self.database(get tuple count)
        pass

    def utility(self, view):
        raise NotImplementedError

    def prune(self):
        raise NotImplementedError
