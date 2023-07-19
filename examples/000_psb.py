# %%
import logging
import nx2pd as nx
import pandas as pd

from nxcals.spark_session_builder import get_or_create, Flavor

logging.info('Creating the spark instance')

spark = get_or_create('My_APP')
sk  = nx.SparkIt(spark)
logging.info('Spark instance created.')

# %%
t0 = pd.Timestamp('2023-07-12 21:20', tz='UTC')
t1 = pd.Timestamp('2023-07-12 21:40', tz='UTC')

df = sk.nxcals_df(['PSB.LSA:CYCLE',
                   'PSB.TGM:SCNUM'],
                  t0,
                  t1,
                  pandas_processing=[
                     nx.pandas_get,
                     nx.pandas_pivot,
                     ]
                 )
nx.pandas_index_localize(df)
df.head()
# %%
import matplotlib.pyplot as plt
plt.plot(df.index, df['PSB.TGM:SCNUM'])

# %%
df['PSB.LSA:CYCLE'].unique()
# %%
