from typing import List
from db_utils import *

class SeeDB(object):
    '''
    Assumptions:
    1. the table has an index column with unique, continuous
    integer sequence
    2. the table has no missing values
    3. Pruning distance measure has KL-divergence
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
        self.functions = ['avg', 'min', 'max', 'sum', 'count']

    def recommend_views(self, query_dataset_cond: str,
            reference_dataset_cond: str, n_phases: int = 10) -> List:
        '''
        Inputs:
        query_dataset_cond     : what goes in the WHERE sql clause
                                 to get query dataset
        reference_dataset_cond : what goes in the WHERE sql clause
                                 to get reference dataset
        '''
        # TODO : allow for multiple distance functions
        dist = kl_divergence   # distance function for pruning

        ## Initialize candidate_views to all views ##
        candidate_views = dict()
        for attribute in self.attributes:
            for measure in self.measures:
                for func in self.functions:
                    # print(attribute, measure, func)
                    # print(candidate_views)
                    if attribute not in candidate_views:
                        candidate_views[attribute] = dict()
                    if measure not in candidate_views[attribute]:
                        candidate_views[attribute][measure] = set()
                    # print(candidate_views)
                    candidate_views[attribute][measure].add(func)

        ## views selection loop ##
        itr_phase = 0
        for start, end in self.phase_idx_generator(n_phases):
            itr_phase += 1

            itr_view = -1
            dist_views = []
            mappings_distidx_view = dict()

            for attribute in candidate_views:

                ## Sharing optimization : combine multiple aggregates
                selections = ''
                for measure in candidate_views[attribute]:
                    for func in candidate_views[attribute][measure]:
                        selections += func + '(' + measure + '), '
                selections = selections[: -2] # removes ', ' in the end
                query_dataset_query = self._make_view_query(selections,
                        self.table_name, query_dataset_cond, attribute,
                        start, end)
                reference_dataset_query = self._make_view_query(selections,
                        self.table_name, reference_dataset_cond, attribute,
                        start, end)
                q = select_query(self.db, query_dataset_query,
                        return_float = True)
                r = select_query(self.db, reference_dataset_query,
                        return_float = True)

                ## for pruning
                itr_col = -1
                for measure in candidate_views[attribute]:
                    for func in candidate_views[attribute][measure]:
                        itr_view += 1
                        itr_col += 1
                        print(q.shape, r.shape)
                        d = dist(q[:, itr_col], r[:, itr_col])
                        dist_views.append(d)
                        mappings_distidx_view[iter_view] = (attribute, measure, function)

            ## prune
            pruned_view_indexes = self.prune(dist_views, itr_phase)

            ## delete pruned views
            pruned_views = [mappings_distidx_view[idx]
                            for idx in pruned_view_indexes]
            for attribute, measure, func in pruned_views:
                candidate_views[attribute][measure].remove(func)
                if len(candidate_views[attribute][measure]) == 0:
                    del candidate_views[attribute][measure]
                    if len(candidate_views[attribute]) == 0:
                        del candidate_views[attribute]

        ## make final recommended views ##
        recommend_views = []
        for attribute in candidate_views:
            for measure in candidate_views[attribute]:
                for func in candidate_views[attribute][measure]:
                    recommend_views.append((attribute, measure, func))

        return recommended_views

    def _make_view_query(self, selections, table_name, cond, attribute,
            start, end):
        return ' '.join(['select', selections,
                        'from', table_name,
                        'where', cond,
                        'and id>=' + str(start), 'and', 'id<' + str(end),
                        'group by', attribute,
                        'order by', attribute])


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

    def prune(self, kl_divg, iter_phase, N_phase=10, delta=0.05, k=5):
        '''
        input: a list of KL divergences of the candidate views, batch number
        output: a list of indices of the views that should be discarded
        '''
        kl_divg = np.array(kl_divg)
        if iter_phase == 1:
            return []
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
        return []

