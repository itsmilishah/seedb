from typing import List
from db_utils import *
import pdb

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
                    for view in candidate_views[attributes][measure]:
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

    def _make_view_query(self, selections, table_name, cond, attribute,
            start, end):
        return ' '.join(['select', selections,
                        'from', table_name,
                        'where', cond,
                        'and id >= ', start, 'id < ', end,
                        'group by', attribute,
                        'order by', attribute])


    def visualize(self, views, labels=None) -> None:
        '''

        '''
        query_dataset_cond = "marital_status in ('Married-civ-spouse', 'Married-spouse-absent', 'Married-AF-spouse')" 
        reference_dataset_cond = "marital_status in ('Divorced', 'Never-married', 'Separated', 'Widowed')"
        if labels==None:
            labels = ['Query','Reference']
        for view in views:
            attribute, measure, function = view
            # get the query table result
            selection = function + '(' + measure + '), '
            query_dataset_query = self._make_view_query(selection,
                    self.table_name, query_dataset_cond, attribute, 0, self.n_tuples)
            reference_dataset_query = self._make_view_query(selection,
                    self.table_name, reference_dataset_cond, attribute, 0, self.n_tuples)
            table_query = np.array(select_query(self.db, query_dataset_query))
            table_reference = np.array(select_query(self.db, reference_dataset_query))
            
            # create plot
            plt.figure()
            n_groups = table.shape[0]
            index = np.arange(n_groups)
            bar_width = 0.35
            opacity = 0.8
             
            rects1 = plt.bar(index, table_query[:,1], bar_width,
                             alpha=opacity,
                             color='b',
                             label=labels[0])
             
            rects2 = plt.bar(index + bar_width, table_reference[:,1], bar_width,
                             alpha=opacity,
                             color='g',
                             label=labels[1])
             
            plt.xlabel(attribute)
            plt.ylabel(function+'('+measure+')')
            plt.title('View = ',view)
            plt.xticks(index + bar_width, tuple(table_query[:,0]))
            plt.legend()
            plt.tight_layout()
            plt.savefig(attribute+'_'+measure+'_'+function+'.png', dpi=300)
            plt.close()
        pdb.set_trace()


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



