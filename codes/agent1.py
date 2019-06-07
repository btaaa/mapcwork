# coding: utf-8

import numpy
import pandas
from scipy.cluster.hierarchy import fclusterdata
from geopy.distance import vincenty
import warnings
warnings.filterwarnings('ignore')
import math
import multiprocessing
import collections
import sys
import subprocess
import sqlalchemy



import sqlalchemy.pool as pool
import psycopg2

def getconn():
    c = psycopg2.connect(f"postgresql://postgres:geospatial@10.10.10.249:5434/cuebiq")
    return c

mypool = pool.QueuePool(getconn, max_overflow=100, pool_size=50)





def multi_agent(uid=""):
    # FYI, multiprocessing.Pool can't guarantee that it keeps inputs ordered correctly
    # dict format is {input: output}...
    return {
        "userid": f"{uid}",
        "home_loc": findHome(uid)
    }




def findHome(id_):
    

    # __engine = sqlalchemy.create_engine(f"postgresql://postgres:geospatial@10.10.10.249:5434/cuebiq",pool_size=10, max_overflow=20)
    # __engine.dispose()
    conn = mypool.connect()
    conn.detach()
    print(conn.is_valid)
    q = f"select * from ma_cub where user_id like '{id_}'"
    X = pandas.read_sql_query(con=conn,sql=q)
    print(len(X))
    X['lat'] = X['lat'].astype(float)
    X['lon'] = X['lon'].astype(float)
    X['date_time'] = X["datetime"].apply(lambda x:  pandas.to_datetime(x,unit='s',utc=True))
    X['date_time_utc'] = X["datetime"].apply(lambda x:  pandas.to_datetime(x,unit='s',utc=True))
    X['date_time_zone'] = X["date_time"].apply(lambda x:  x.tz_convert (tz='US/Eastern'))
    X['date_time_utc_zone'] = X["date_time_utc"].apply(lambda x:  x.tz_convert (tz='US/Eastern'))
    X['hour'] = X["date_time_utc"].apply(lambda x:  x.tz_convert (tz='US/Eastern').hour)

    X['date_time_utc_zone_time'] = X["date_time_utc"].apply(lambda x:  x.tz_convert (tz='US/Eastern').time)
    X = X[["lat","lon","hour"]].dropna()
    print(len(X))
    # print(X)
    home_IDs = []
    home_lons = []
    home_lats = []
    outdf = pandas.DataFrame(dict(home_id=[id_]))
    X=X.loc[(X["hour"]>=20)&(X["hour"]<=23),]
    if len(X.index)>5:

        # fclust1 = fclusterdata(X[['lat','lon']], t= 200, criterion = 'distance', metric=lambda u, v: vincenty(u, v).meters)
        fclust1 = fclusterdata(X[['lat','lon']], t= 200, criterion = 'distance', metric=lambda u, v: vincenty(u, v).meters, method='complete')
        most_common,num_most_common = collections.Counter(fclust1).most_common(1)[0]
        X['cluster'] = fclust1
        X1 = X

        X1 = X1.loc[X1['cluster']==most_common]
        if len(X1.index)>=5:
            # print X1
            home_IDs.append(id_),home_lons.append(numpy.mean(X1['lon'])),home_lats.append(numpy.mean(X1['lat']))
            print(numpy.mean(X1['lon']),numpy.mean(X1['lat']),"latlongs")
            return [numpy.mean(X1['lon']),numpy.mean(X1['lat'])]


# __engine = sqlalchemy.create_engine(f"postgresql://postgres:geospatial@10.10.10.249:5434/cuebiq",pool_size=10, max_overflow=20)

if __name__ == "__main__":
    BATCHSIZE=10
    BATCHID=0
    batch_offset = BATCHID * BATCHSIZE
    csvshell = "ma_1_users.csv"

    # q = f"select * from ma_cub_users order by u_count limit {BATCHSIZE} offset {batch_offset}"

    chunkcounter = 0
    for udf in pandas.read_csv(csvshell, chunksize=BATCHSIZE):
        useridlist=udf['user_id'].tolist()
        # useridlist=['f334b82c1de730c61cc78a10ff920f73b7d911d3f0abd52f0063c8c441ff697f','c1f42368f411321e7161cd7e99fcd9f78d9d6283b8d47bbdf38935e9d2b3191f','d0af03749ba833781ac65ecd1e8e52c34d91d8b9567faecc03c078a32918e43f', '1658f43f13281ba89aa8bed27c0e0042d821a1c879abd089577e58a9d9c2249c']
        # conn = mypool.connect()
        # print(conn.is_valid)
        # udf = pandas.read_sql_query(q, con=conn)
        # print()
        # conn.close()
        # useridlist = udf["user_id"].tolist()
        print(useridlist)
        pool = multiprocessing.Pool(5)  # Use 5 multiprocessing processes to handle jobs...
        results = pool.map(multi_agent, useridlist) # map xrange(0, 12) into pool_job()


        print(results)
        df = pandas.DataFrame(results)
        print(df)
        df.to_csv(f"homes_{csvshell.replace('.csv', '')}_{chunkcounter}.csv")
        chunkcounter=chunkcounter+1