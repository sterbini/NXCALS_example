# %%
from nxcals.spark_session_builder import get_or_create
from nxcals.api.extraction.data.builders import DataQuery
spark = get_or_create("My_APP")

df = DataQuery.builder(spark).entities().system('CMW') \
    .keyValuesEq({'device': 'LHC.LUMISERVER', 'property': 'CrossingAngleIP1'}) \
    .timeWindow('2022-04-22 00:00:00.000', '2022-04-23 00:00:00.000') \
    .build()
# %%
