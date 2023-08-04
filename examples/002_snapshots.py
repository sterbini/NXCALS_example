# %%
import logging
import nx2pd as nx

from nxcals.spark_session_builder import get_or_create, Flavor
logging.info('Creating the spark instance')
logging.info('Creating the spark instance')
spark = get_or_create('snapshots')


sk  = nx.SparkIt(spark)
logging.info('Spark instance created.')

fill_number = 7358
snapshot_list = ['Q trim F8470']

# The 'Q trim F8470' snapshot is a snapshot you can find in timber.cern.ch
variables_list = [] 
for ii in snapshot_list:
    variables_names, _ = sk.get_snapshot(ii)
    variables_list.append(*variables_names)

# %%
# one can get use only partially the snapshot and apply it to anoter time interval
fill_time_windows = sk.get_fill_time(fill_number)
# Simplest approach
t0 = fill_time_windows['start']
t1 = fill_time_windows['end']
df = sk.get(t0, t1, variables_list)

# %%
