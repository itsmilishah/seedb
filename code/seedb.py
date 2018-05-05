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

    def phase_idx_generator(self, n_phases=10):
        '''
        Divides the data into mini-batches (indexes ??)
        and returns the mini-batches (indexes ??) for query.
        With feauture-first grouping ???
        '''
        batch_size = np.floor(self.n_tuples/n_phases).astype(int)
        return [(batch_size*i, batch_size*(i+1)) for i in range(n_phases)]

    def utility(self, view):
        raise NotImplementedError

    def prune(self, kl_divg, iter_phase, N_phase=10, delta=0.05, k=5):
        '''
        input: a list of KL divergences of the candidate views, batch number
        output: a list of indices of the views that should be discarded
        '''
        N = len(kl_divg)
        # Calculate the confidence interval
        delta = 0.05
        a = 1.-(iter_phase/N_phase)
        b = 2*np.log(np.log(iter_phase+1))
        c = np.log(np.pi**2/(3*delta))
        d = 0.5*1/(iter_phase+1)
        conf_error = np.sqrt(a*(b+c)*d)

        # sort the kl divergences
        kl_sorted = np.sort(kl_divg)[::-1]
        index = np.argsort(kl_divg)[::-1]
        min_kl_divg = kl_sorted[k-1]-conf_error
        for i in range(k, N):
            if kl_sorted[i]+conf_error < min_kl_divg:
                return index[i:]
        return 0
        

        
