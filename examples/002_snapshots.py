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
snapshot_list = ['Q trim F8470',
                 'FILL Main Machine and Beams conditions',
                 ]

# The 'Q trim F8470' snapshot is a snapshot you can find in timber.cern.ch
variables_list = [] 
for ii in snapshot_list:
    variables_names, _ = sk.get_snapshot(ii)
    variables_list += variables_names


# %%
# one can get use only partially the snapshot and apply it to anoter time interval
fill_time_windows = sk.get_fill_time(fill_number)

# %%
t0 = fill_time_windows['start']
t1 = fill_time_windows['end']
df = sk.get(t0, t1, variables_list)
# %%
modes = [ii['mode'] for ii in  fill_time_windows['modes']]

# from RAMP to RAMPDOWN
#  (note the inversion of the lists in t1 to get the last list RAMPDOWN)
t0 = fill_time_windows['modes'][modes.index('RAMP')]['start']
t1 = fill_time_windows['modes'][::-1][
        modes[::-1].index('RAMPDOWN')]['end']
df = sk.get(t0, t1, variables_list)


# %%
