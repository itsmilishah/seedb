from seedb import SeeDB
import numpy as np
from db_utils import *

def main(db_name):
    seedb = SeeDB(db_name)
    # # _make_view_query test
    # query = seedb._make_view_query('avg(fnlwgt)', 'census',
            # 'workclass=\' Private\'', 'marital_status', 0, 10)
    # print(query)
    # print(select_query(seedb.db, query))
    
    # # recommend_views test
    #  print(seedb.recommend_views('sex=\' Female\'', 'sex=\' Male\''))
    # # convert query to float test
    # query = 'select avg(capital_gain) \
            # from census group by workclass \
            # order by workclass limit 10;'
    # a = select_query(seedb.db, query, return_float=True)
    # print('query float outputs = ',a)

    # # kl divergence test
    # p1 = np.array(([1,2,3,4,5]))
    # p2 = np.array(([4,1,2,6,5]))
    # print('kl divg = ',kl_divergence(p1,p2))

    # # pruning test
    # kl_divg = np.array(([7,3,4,2.5,1,5,6,3.5,8,2.1]))
    # iter_phase = 1
    # N_phase = 10
    # a = seedb.prune(kl_divg, iter_phase, N_phase)
    # print('views to be removed = ',a)
    # test visualization
    views = [('workclass','age','avg'), ('education','capital_gain','avg')]
    labels = ['married','unmarried']
    seedb.visualize(views, labels)

if __name__ == '__main__':

    db_name = 'seedb'
    main(db_name)

