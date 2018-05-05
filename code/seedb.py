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
        self.functions = ['average', 'min', 'max', 'sum', 'count']

    def recommend_views(self, query_dataset_cond: str,
            reference_dataset_cond: str, phase_size: int = 10) -> List:
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
                    if attribute not in candidate_views:
                        candidate_views[attribute] = dict()
                        if measure not in candidate_views[attribute]:
                            candidate_views[attribute][measure] = set()
                    candidate_views[attribute][measure].add(func)

        ## view selection loop ##
        itr_phase = -1
        for start, end in self.phase_idx_generator():
            itr_phase += 1
            mappings_view_dist = dict()
            dist_views = []
            for attribute in candidate_views:

                ## Sharing optimization : combine multiple aggregates
                selections = ''
                for measure in candidate_views[attributes]:
                    for view in candidate_views[attributes][measure]
                        selections += func + '(' + measure + '), '
                selections = selections[: -3] # removes ', ' in the end
                query_dataset_query = self._make_view_query(selections,
                        table_name, query_dataset_cond, attribute)
                reference_dataset_query = self._make_view_query(selections,
                        table_name, reference_dataset_cond, attribute)
                q = select_query(self.conn, query_dataset_query,
                        return_float = True)
                r = select_query(self.conn, reference_dataset_query,
                        return_float = True)

                ## begin_pruning
                idx = -1
                for measure in candidate_views[attribute]:
                    for func in candidate_views[attribute][measure]:
                        idx += 1
                        assert ((attribute, measure, function)) not in mappings_view_dist
                        mappings_view_dist[(attribute, measure, function)] = idx
                        dist_views.append(dist(q[: idx], r[:, idx]))
            dist_views = np.array(dist_views)
            not_pruned_view_indexes = self.prune(dist_views)
                # TODO : while pruning, make sure to remove the
                #        empty attibutes from candidate_views
                # TODO :
                # for each phase:
                # mapping : (a, m, f) => list of KL divergences
                    # for each attr:
                        # for each view, measure:
                    # q = ...
                    # r = ...
                        # for each view:
                            # for each measure:

        raise NotImplementedError

    def _make_view_query(selections, table_name, cond, attribute,
            start, end):
        return ' '.join(['select', query,
                        'from', table_name,
                        'where', query_dataset_cond,
                        'and id >= ', start, 'id < ', end,
                        'group by', attribute,
                        'order by', attribute])


    def visualize(self, measure_name: str, function_name: str,
            attribute_name: str, attribute_values: List) -> None:
        raise NotImplementedError

    def utility(self, view):
        raise NotImplementedError

    def prune(self):
        raise NotImplementedError
