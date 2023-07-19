# %%
# python 002.py 2> std.err

import os
import getpass
import logging
import sys

# logging.basicConfig(stream=sys.stdout, level=logging.INFO)

# os.environ['PYSPARK_PYTHON'] = "./environment/bin/python"
# username = getpass.getuser()
# print(f'Assuming that your kerberos keytab is in the home folder, '
#       f'its name is "{getpass.getuser()}.keytab" '
#       f'and that your kerberos login is "{username}".')

# logging.info('Executing the kinit')
# os.system(f'kinit -f -r 5d -kt {os.path.expanduser("~")}/'+
#           f'{getpass.getuser()}.keytab {getpass.getuser()}');

# %%
import json
import nx2pd as nx
import pandas as pd

from nxcals.spark_session_builder import get_or_create, Flavor

logging.info('Creating the spark instance')

# Here I am using YARN (to do compution on the cluster)
#spark = get_or_create(flavor=Flavor.YARN_SMALL, master='yarn')

# Here I am using the LOCAL (assuming that my data are small data,
# so the overhead of the YARN is not justified)
# WARNING: the very first time you need to use YARN
# spark = get_or_create(flavor=Flavor.LOCAL)

logging.info('Creating the spark instance')
spark = get_or_create('My_APP')
sk  = nx.SparkIt(spark)
logging.info('Spark instance created.')

# %%
# Simplest approach
data_list = ["HX:FILLN"]
t0 = 1634707139157238525
t1 = 1634751674621363525
df = sk.nxcals_df(data_list, t0, t1)
pd_df = df.toPandas()
assert len(pd_df) == 2  # a sanity check
assert pd_df.nxcals_value.values[0] == 7505
assert pd_df.nxcals_value.values[1] == 7504
df.show()
# %%
aux = sk.get_variables(['LHC.BCTDC%:BEAM_INTENSITY','%HX:BMO%',])
assert len(aux)==26
print(json.dumps(aux,
                 sort_keys=False, indent=4, default=str))
# %%

my_fill = 6666
aux = sk.get_fill_time(my_fill)
print(json.dumps(sk.get_fill_time(my_fill),
                 sort_keys=False, indent=4,default=str))
#assert ['duration'] == pd.Timedelta('0 days 14:53:58.321000')

# %% or
my_fill = 'last'
print(json.dumps(sk.get_fill_time(my_fill),
                 sort_keys=False, indent=4,default=str))



# %% Latency test
# data_list = ["LHC.BCTDC%:BEAM_INTENSITY"]
data_list = ["LHC.BCTDC%:BEAM_INTENSITY", "LHC.BCTFR.A6R4.B%:BUNCH_INTENSITY"]
# You can use directly pd.Timestamp
# and mix the different formats
# Note that all format (pd.Timistamp, int, str) maintains the ns precision
t1 = pd.Timestamp.now(tz="UTC") - pd.Timedelta('11d')*0
t0 = t1-pd.Timedelta('1h')

df = sk.get(t0, t1, data_list)
print(t1)
df.tail()

# %%
t0 = pd.Timestamp('2018-05-11 10:29:36.957000+0000', tz='UTC')
t1 = pd.Timestamp('2018-05-11 10:29:36.958000+0000', tz='UTC')

df = sk.nxcals_df(["HX:FILLN"],
                  t0,
                  t1,
                  pandas_processing=[
                     nx.pandas_get,
                     nx.pandas_pivot,
                     ]
                 )
nx.pandas_index_localize(df)

# %%

aux = sk.get_fill_raw_data(7770,['%HX:BMO%','LHC.BCTDC%:BEAM_INTENSITY'])

# %%
# you need a YARN master
if False:
    spark.stop()
    spark = get_or_create(flavor=Flavor.YARN_SMALL, master='yarn')
    sk  = nx.SparkIt(spark)
    my_df = sk.get_fill_data(7920,sampling_frequency= '5s')
    my_df.to_parquet('test.parquet')
#else:
#    my_df = pd.read_parquet('test.parquet')

#print(my_df)

# %%
# On line computation
#print((
#    nx.pandas_interpolate(my_df[['LHC.BSRT.5R4.B1:BUNCH_EMITTANCE_H']])
#      .apply(lambda x:x[0], axis=1)
#    /
#    nx.pandas_interpolate(my_df[['LHC.BSRT.5R4.B1:BUNCH_EMITTANCE_V']])
#    .apply(lambda x:x[0], axis=1)
# .apply(lambda x:x[0]))
# .dropna())

# %%
# Wires scan  (just for fun)

t0 = pd.Timestamp('2022-05-22 12',tz="UTC")
t1 = pd.Timestamp('2022-05-22 20',tz="UTC")

df = sk.get(t0, t1, ['LHC.BWS.5L4.B2H1:Acquisition:projBunchPositionSet1'])
# %%
# Some change between R2 and R3...
t0 = pd.Timestamp('2022-05-22 12',tz="UTC")
t1 = pd.Timestamp('2022-05-22 20',tz="UTC")
df = sk.get(t0, t1, ['LHC.BWS.%NB_BUNCHES%'])

print('All done! Test passed.')
# %%
