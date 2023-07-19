from nxcals.api.extraction.data.builders import *
import pyspark.sql.functions as func
from pyspark.sql.functions import col
from pyspark.sql.types import *
import numpy as np
import pandas as pd
import gc
from scipy.interpolate import interp1d
from typing import Union
from pyspark.sql import DataFrame
import getpass
import os
import sys
from pathlib import Path
import json
import time
import dask.dataframe as dd
import shutil


__version__ = 'of 24.10.2022 @ 03:17PM'

print ('\n>>> Loading nx2pd.py version {}\n'.format(__version__))

def connect_to_spark(
    master = None,
):
    """
    This method takes a pyspark_conda_env and returns a SparkSession.

    Args:
        Takes the path of the pyspark_conda_env. Default value is
        '~/pyspark_conda_env.tar.gz'.
    Returns:
        An handle to the spark sessions.

    Raises:
        None.
    """
    from nxcals import spark_session_builder

    return spark_session_builder.get_or_create(
        master=master,
    )


def get_kerberos_token(
    username: str = getpass.getuser(),
    keytab_file: Union[
        str, Path
    ] = f"{os.path.expanduser('~')}/{getpass.getuser()}.keytab",
):
    """
    This method takes a keytab_file and a username to get a kerberos token.

    Args:
        username (str): the username. The default value is the terminal username
        keytab_file: Union[str, Path]: the path of the keytab file 
    Returns:
        None.

    Raises:
        None.
    """
    os.system(f"kinit -k -t {keytab_file} {username}@CERN.CH")


class SparkIt:
    '''
    The SparkIt class contain the main method to access the NXCALS data (e.g., nxcals_df).
    To be instantiated it requires the SparkSession handle.
    '''
    def __init__(self, spark):
        self.spark = spark
        self.api = self.spark._jvm.cern.nxcals.api
        self.fillService = (self.api
                    .custom
                    .service
                    .Services
                    .newInstance()
                    .fillService())
        self.ServiceClientFactory = self.api.extraction.metadata.ServiceClientFactory
        self.vs = self.ServiceClientFactory.createVariableService()

        self.Variables = self.api.extraction.metadata.queries.Variables
 
        self.TimeUtils = self.api.utils.TimeUtils
        self.ExtractionProperties = self.api.custom.service.extraction.ExtractionProperties
        self.LookupStrategy = self.api.custom.service.extraction.LookupStrategy


        self.extractionService = (self.api
                            .custom
                            .service
                            .Services
                            .newInstance()
                            .extractionService())

        self.group_service = self.spark._jvm.cern.nxcals.api.extraction.metadata.ServiceClientFactory.createGroupService()
        self.Groups = self.spark._jvm.cern.nxcals.api.extraction.metadata.queries.Groups

    def get_snapshot(self, snapshot_name):
        group = self.group_service.findOne(self.Groups.suchThat().name().eq(snapshot_name)).get()
        var_names = [var.getVariableName() for var in self.group_service.getVariables(group.getId())['getSelectedVariables']]
        my_dict = dict(group.getProperties())
        return var_names, my_dict

    def get_fill_time(self, number):
        if number == 'last':
            my_fill = self.fillService.getLastCompleted().get().getNumber()
        elif isinstance(number,int):
            my_fill = number
        else:
            raise NotImplementedError

        my_dict={}
        aux = self.fillService.findFill(my_fill).get()

        my_dict['fill'] = aux.getNumber()
        my_dict['start'] = pd.Timestamp(aux.getValidity()
                                            .getStartTime()
                                            .toString(), tz = 'UTC')
        my_dict['end'] = pd.Timestamp(aux.getValidity()
                                        .getEndTime()
                                        .toString(), tz = 'UTC')
        my_dict['duration'] = my_dict['end'] - my_dict['start']

        bm_list = []
        for modes in aux.getBeamModes():
            bm = {}
            bm['mode'] = modes.getBeamModeValue()
            bm['start'] = pd.Timestamp(modes.getValidity()
                                            .getStartTime()
                                            .toString(), tz = 'UTC')
            bm['end'] = pd.Timestamp(modes.getValidity()
                                        .getEndTime()
                                        .toString(), tz = 'UTC')
            bm['duration'] = bm['end'] - bm['start']
            bm_list.append(bm)
        my_dict['modes'] = bm_list
        return my_dict

    def _get_variable(self, variable_str):
        #System = spark._jvm.java.lang.System
        #System.setProperty("service.url", "https://cs-ccr-nxcals5.cern.ch:19093,https://cs-ccr-nxcals5.cern.ch:19094,https://cs-ccr-nxcals6.cern.ch:19093,https://cs-ccr-nxcals6.cern.ch:19094,https://cs-ccr-nxcals7.cern.ch:19093,https://cs-ccr-nxcals7.cern.ch:19094,https://cs-ccr-nxcals8.cern.ch:19093,https://cs-ccr-nxcals8.cern.ch:19094");
        
        var = self.vs.findAll(self.Variables
                        .suchThat()
                        .variableName()
                        .like(variable_str))
        my_list = []
        for ii in var:
            my_dict = {}
            my_dict['name'] = ii.getVariableName()
            my_dict['unit'] = ii.getUnit()
            my_dict['description'] = ii.getDescription()
            my_dict['system'] = ii.getSystemSpec().getName()
            my_list.append(my_dict)
        return my_list

    def get_variables(self, variable_list):
        if isinstance(variable_list, str):
            return self._get_variable(variable_list)
        if isinstance(variable_list, list):
            my_list = [] 
            for vv in variable_list:
                aux = self._get_variable(vv)
                my_list = my_list+ aux
            # https://stackoverflow.com/questions/9427163/remove-duplicate-dict-in-list-in-python
            my_list = [dict(t) for t in {tuple(d.items()) for d in my_list}]
            my_list = (pd.DataFrame(my_list)
                    .sort_values('name')
                    .to_dict(orient='records'))
            return my_list
        raise NotImplementedError

    def nxcals_df(self, data: list, t0: Union[pd.Timestamp, str, int], t1: Union[pd.Timestamp, str, int], system_name: str = 'CMW', spark_processing: list = [], pandas_processing: list = [], **kwarg) -> Union[pd.DataFrame, DataFrame]:
        '''
        It takes the a list of NXCLAS variable of the same type and extract it between t0 and t1. 
        - t0,t1 can be integer, pd.Timestamp or strings. 
            If not tz_aware will be casted to UTC.
        - system_name is the NXCALS system ('CMW' is the predifined one)
        - spark_processing is the sorted list of the functions to apply on the spark df 
            (to be executed in the NXCALS cluster) 
        - pandas_processing is the sorted list of the functions to apply after the conversion in pandas df. 
            (if pandas_processing is empty then no pandas conversion is executed).
        '''

        if not isinstance(data,list):
            raise ValueError("The argument 'data' should be a list.")
        elif len(data)==0:
            raise ValueError("The list 'data' should not be empty.")
            
        if not isinstance(t0, pd.Timestamp):
            t0=pd.Timestamp(t0, tz='UTC'); 
        if not isinstance(t1, pd.Timestamp):
            t1=pd.Timestamp(t1, tz='UTC'); 
        
        if t0.tzinfo == None:
            t0 = t0.tz_localize('UTC')
        if t1.tzinfo == None:
            t1 = t1.tz_localize('UTC')
            
        assert t0<t1, "The 't1' date should be after the 't0' date"


        if isinstance(data[0], str):
            df = DataQuery.builder(self.spark).byVariables() \
                    .system(system_name) \
                    .startTime(t0.value) \
                    .endTime(t1.value)
            for ee in data:
                df = df.variableLike(ee)
        elif isinstance(data[0], dict):
            df = DataQuery.builder(self.spark).byEntities() \
                    .system(system_name) \
                    .startTime(t0.value) \
                    .endTime(t1.value)
            for ee in data:
                df = df.entity().keyValuesLike(ee)
        else:
            raise TypeError
        df = df.buildDataset()


        for trasformation in spark_processing:
            df = trasformation(df)
        for trasformation in pandas_processing:
            df = trasformation(df)

        return df

    def get(self, t0, t1, data, pandas_index_localize=True):
        my_var = self.get_variables(data)
        my_df = []

        for ii in my_var:
            df = self.nxcals_df([ii['name']], 
                        t0,
                        t1,
                        system_name=ii['system'],
                        pandas_processing=[
                            pandas_get, 
                            pandas_pivot])
            my_df.append(df.sort_index())
        my_df = pd.concat(my_df, axis=1).sort_index()
        if pandas_index_localize:
            nx.pandas_index_localize(my_df)
        return my_df
    
    def get_last_event(self, data_list, t1 = pd.Timestamp.now(tz='UTC'), timedelta = pd.Timedelta(days=1)):
        if not isinstance(t1, pd.Timestamp):
            t1=pd.Timestamp(t1, tz='UTC'); 
        t0 = t1 - timedelta
        df = self.nxcals_df(data_list, t0, t1, pandas_processing=[pandas_get, pandas_pivot])
        assert len(df.columns)<2
        if len(df)<2:
            return self.get_last_event(data_list, t1=t1, timedelta= 3*timedelta)
        else:
            return df.sort_index().iloc[-1].values[0]

    def get_last_fills(self, timedelta=pd.Timedelta(days=1)):
        t0 = pd.Timestamp.now(tz='UTC') - timedelta
        t1 = pd.Timestamp.now(tz='UTC')
        data_list = ['HX:FILLN']
        df = self.nxcals_df(data_list, t0, t1, pandas_processing=[pandas_get, pandas_pivot, pandas_index_localize])
        df['start'] = df.index.to_series()
        df['duration'] = (-df.index.to_series().diff(periods=-1))
        df['duration [ms]'] = df['duration'].astype('timedelta64[ms]')
        df['duration [h]'] = df['duration [ms]']/1000/3600
        df['end'] = df['start']+df['duration']
        if len(df)<2:
            return self.get_last_fills(3*timedelta)
        else:
            df = df.set_index('HX:FILLN')
            df.index.name = None
            return df[['start', 'end', 'duration [h]']]

    def get_last_completed_fill(self, timedelta=pd.Timedelta(days=1)):
        return self.get_last_fills(timedelta).dropna().iloc[[-1]]

    def get_fill_raw_data(self, fill, data):
        my_fill = self.get_fill_time(fill)
        t0 = my_fill['start']
        t1 = my_fill['end']-pd.Timedelta('1ns')
        return self.get(t0, t1, data)

    def get_fill_data(self, fill,
                      events_list = [['HX:FILLN', 'HX:AMODE', 'HX:BETASTAR_IP%', 'LHC.RUNCONFIG:IP%-XING-%-MURAD'], ['HX:BMODE']],
                      data_list = [['RPMBB.%_RO%_%I_MEAS'],['%ALICE:I_MEAS', '%ATLAS:I_MEAS', ' %CMS:I_MEAS'],['LHC.BQM.B%:BUNCH_LENGTH_MEAN'], ['LHC.BCTDC%:BEAM_INTENSITY'],
                                                   ['LHC.BCTFR.%.B%:BUNCH_INTENSITY'],['AT%:BUNCH_LUMI_INST', 'CMS%:BUNCH_LUMI_INST'],['LHC.BSRT.%:BUNCH_EMITTANCE_%']],
                     sampling_frequency= None, verbose=False, split_h=100):
        my_fill = self.get_fill_time(fill)
        t0 = my_fill['start']
        t1 = my_fill['end']-pd.Timedelta('1ns')
        if split_h==None:
            return self.get_LHC_data(t0, t1, events_list, data_list, sampling_frequency, verbose=verbose).sort_index()
        else:
            # print(t0)
            # print(t1)
            time_interval = list(pd.date_range(start=t0,
                  end=t1+pd.Timedelta('1ns'), freq=f'{split_h}h'))
            time_interval.append(t1+pd.Timedelta('1ns'))
            print(f' --- data will be downloaded in {len(time_interval)-1} chunks')
            # print(time_interval)
            my_list = []
            for ii,jj in zip(time_interval[0:-1],time_interval[1:]):
                  my_list.append(self.get_LHC_data(ii, jj-pd.Timedelta('1ns'), 
                                 events_list, data_list, sampling_frequency,
                                verbose=verbose).sort_index())
            df = pd.concat(my_list).sort_index()
            for ii in events_list:
                  try:
                        df[ii] = df[ii].fillna(method='ffill')
                  except:
                        print(f"{ii} not in index")
        
             # refill the event
            if 'HX:FILLN' in events_list:
                  df['HX:FILLN'] = df['HX:FILLN'].fillna(method='bfill')
                  df['HX:FILLN'] = df['HX:FILLN'].fillna(method='ffill')
                  df['HX:FILLN'] = df['HX:FILLN'].astype(int)

            df['HX:FILLN'] = fill
            return df

    def get_LHC_data(self, t0, t1,
                    events_list = [['HX:FILLN', 'HX:AMODE', 'HX:BETASTAR_IP%', 'LHC.RUNCONFIG:IP%-XING-%-MURAD'], ['HX:BMODE']],
                    data_list = [['RPMBB.%_RO%_%I_MEAS'],['%ALICE:I_MEAS', '%ATLAS:I_MEAS', ' %CMS:I_MEAS'],['LHC.BQM.B%:BUNCH_LENGTH_MEAN'], ['LHC.BCTDC%:BEAM_INTENSITY'],
                                                   ['LHC.BCTFR.%.B%:BUNCH_INTENSITY'],['AT%:BUNCH_LUMI_INST', 'CMS%:BUNCH_LUMI_INST'],['LHC.BSRT.%:BUNCH_EMITTANCE_%']],
                    sampling_frequency='5s', verbose=False):   
        gc.collect()

        if not isinstance(t0, pd.Timestamp):
            t0=pd.Timestamp(t0, tz='UTC'); 
        if not isinstance(t1, pd.Timestamp):
            t1=pd.Timestamp(t1, tz='UTC'); 
        
        if t0.tzinfo == None:
            t0 = t0.tz_localize('UTC')
        if t1.tzinfo == None:
            t1 = t1.tz_localize('UTC')

        # event
        events_df = []
        for event in events_list :
            if verbose:
                print(f'EVENT: {event}')
            events_df.append(self.nxcals_df(event, t0, t1,
                              spark_processing=[],
                              pandas_processing=[pandas_get, 
                                                 pandas_pivot]))
        # data
        data_df = []
        for data in data_list:
            # print(f'DATA: {data}')
            if sampling_frequency == None:
                data_df.append(self.nxcals_df(data, t0, t1,
                              pandas_processing=[pandas_get, 
                                                 pandas_pivot]))
            else:
                data_df.append(self.nxcals_df(data, t0, t1,
                              spark_processing=[lambda data:spark_subsampling(data, start=t0, end=t1, freq= sampling_frequency)],
                              pandas_processing=[pandas_get, 
                                                 pandas_pivot]))

        # we forward fill the events assuming on change   
        events_df = pd.concat(events_df)
        events_df['event flag'] = True

        data_df = pd.concat(data_df)
        data_df['event flag'] = False

        # we concatenate events and data 
        df = pd.concat([events_df,
                        data_df, 
                       ]).sort_index()

        for ii in events_df.columns:
            df[ii] = df[ii].fillna(method='ffill')

        df = df[~df.index.duplicated(keep='first')].sort_index()
        
        my_list_of_event = list(set(events_df.columns)-set('event flag'))

        # ensure you get the first event at the start
        for my_event in my_list_of_event:
            if pd.isna(df.iloc[0][my_event]):
                df.iloc[0, df.columns.get_loc(my_event)]=self.get_last_event([my_event], t1=df.index[0])
        
        # refill the event
        for ii in events_df.columns:
            df[ii] = df[ii].fillna(method='ffill')
        
        if 'HX:FILLN' in events_df.columns:
            df['HX:FILLN'] = df['HX:FILLN'].astype(int)
        return df

    def is_fill_in_parquet(self, fill_number, my_path):
      path_test=my_path/f"HX:FILLN={fill_number}"
      return path_test.exists()

    def fill_to_parquet(self, fill_number, my_path, event_list, data_list, split_h=100):
        if not self.is_fill_in_parquet(fill_number, my_path):
            aux = self.fill_to_df(fill_number, event_list, data_list, split_h=split_h)
            aux.to_parquet(my_path, partition_cols=['HX:FILLN', 'HX:BMODE'])
            return aux 

    def fill_to_df(self, fill_number, event_list, data_list, split_h):
        aux = self.get_fill_time(fill_number)
        #print(json.dumps(aux,
        #            sort_keys=False, indent=4,default=str))

        #for ii in datavars['2022']['data_list']:
        #     print(ii)
        if len(aux['modes'])>1:
            my_df = self.get_fill_data(aux['fill'],
            events_list=event_list,
            data_list=data_list,
            sampling_frequency = None, verbose=True, split_h=split_h) 
            my_df['creation time'] = int(time.time()*1e9)
            for my_column, my_type in zip(my_df.columns,my_df.dtypes):
                if my_type==object:
                    if (len(my_df[my_column].dropna())>0) and (type(my_df[my_column].dropna().iloc[0])==dict):
                        my_df[my_column] = my_df[my_column].apply(lambda x:x['elements'] if pd.notnull(x) else x)
            if 'HX:BMODE' in  my_df.columns:
                aux = my_df.sort_index().loc[aux['start'].value:
                                        aux['end'].value].sort_index() 
                return aux  

    def add_data_to_fill_parquet(self, parquet_file_name, data):
      if not os.path.exists(parquet_file_name):
            print('File not found!')
            return
      else:
            my_df = dd.read_parquet(parquet_file_name).compute()
            #for my_column, my_type in zip(my_df.columns,my_df.dtypes):
            #      if my_type==object:
            #            if (len(my_df[my_column].dropna())>0) and (type(my_df[my_column].dropna().iloc[0])==dict):
            #                  my_df[my_column] = my_df[my_column].apply(lambda x:x['elements'] if pd.notnull(x) else x)
            #my_df = my_df.append(data)
            #delete the file parquet_file
            my_fill = int(my_df.iloc[0]['HX:FILLN'])
            #delete the folder parquet_file_name
            new_df = self.fill_to_df(my_fill, [['HX:FILLN','HX:BMODE']], (
                              data))
            del new_df['creation time']

            if 'HX:BMODE' in  my_df.columns:
                  shutil.rmtree(parquet_file_name)
                  my_df = pd.concat([my_df, new_df], axis=0,join="outer").sort_index()
                  my_df = my_df[~my_df.index.duplicated(keep='first')].sort_index()
                  my_df['creation time'] = int(time.time()*1e9)
                  my_df.to_parquet(Path(parquet_file_name).parent, partition_cols=['HX:FILLN', 'HX:BMODE'])  
                  return my_df  

def spark_get(df):
    return df

def spark_decimate(df, factor=.1):
    return df.sample(factor)

def spark_subsampling(spark_df, start, end, freq= '5min', 
                      index='nxcals_timestamp', columns='nxcals_variable_name',
                      value='nxcals_value', rounding='1min', verbose=False):
    assert start<end, "The 'end' date should be after the 'start' date"
    
    if not isinstance(start, pd.Timestamp):
        start=pd.Timestamp(start, tz='UTC'); 
    if not isinstance(end, pd.Timestamp):
        end=pd.Timestamp(end, tz='UTC'); 
        
    if start.tzinfo == None:
        start = start.tz_localize('UTC')
    if end.tzinfo == None:
        end = end.tz_localize('UTC')
    
    try:
        aux = pd.date_range(start=start.round(rounding), end=end, freq=freq).view(np.int64)
    except:
        aux = np.array([start.value, end.value])
    def _set_window(timestamp):
        try:
            return np.int(np.where(aux>=(timestamp+1))[0][0])
        except:
            return np.int(0)
    def _set_window_time(window):
        try:
            return np.int(aux[window-1])#np.int((aux[window-1]+aux[window])/2)
        except:
            return np.int(0)
        
    try:
        delta_t_ns = pd.Timedelta(freq).to_timedelta64().astype(int)
    except:
        delta_t_ns = 1
        
    def set_window(timestamp):
        return int((timestamp - start.value)/delta_t_ns)
    def set_window_time(window):
        return int(start.value+(window + 0.5) * delta_t_ns)

    if isinstance(spark_df.schema[value].dataType, StructType):
        if verbose: print('It is a vector')
        def compute_mean(x):
            try:
                aux=0
                for i in len(x):
                    aux=aux+x[i]
                return aux/len(x)
            except:
                return x[0]
        udf_compute_mean = func.udf(compute_mean, ArrayType(DoubleType()))
        vector_data = True
        values=f'{value}.elements'
    elif isinstance(spark_df.schema[value].dataType, FloatType) or isinstance(spark_df.schema[value].dataType, DoubleType):
        if verbose: print('It is a scalar')
        vector_data = False
        values=value
    else:
        raise ValueError('The spark df does not contains nxcals_value/nxcals_value.elements.')
    
    udf_set_window = func.udf(set_window, LongType())
    udf_set_window_time = func.udf(set_window_time, LongType())
    
    spark_df=spark_df.withColumn('window',udf_set_window(col(index)))
    spark_df=spark_df.withColumn('timeWindow',udf_set_window_time(col('window')))
    if vector_data:
        print()
        return spark_df.groupby(columns, 'timeWindow').agg(udf_compute_mean(func.collect_list(values)).alias(values),\
                                        func.min("timeWindow").alias(index), func.min("window").alias('ts_id')).drop('timeWindow')
    else:
        return spark_df.groupby(columns,'timeWindow').agg(func.mean(values).alias(values),\
                                        func.min("timeWindow").alias(index), func.min("window").alias('ts_id')).drop('timeWindow')

    

def pandas_get(df):
    df = df.toPandas()
    return df

def pandas_ffill(df):
    df = pd.concat(df).sort_index().fillna(method='ffill')
    return df

def pandas_combine_name(df, columns):
    df['name'] = df[columns[0]]
    for ii in columns[1:]:
        df['name'] += ':' + df[ii]
    return df
    
def pandas_pivot(df, index='nxcals_timestamp', columns='nxcals_variable_name', values=None):
    if values == None:
        if 'nxcals_value' in df.columns:
            values='nxcals_value'
        elif 'nxcals_value.elements' in df.columns:
            values='nxcals_value.elements'
        else:
            raise ValueError("The datafame has not the column nxcals_value/nxcals_value.elements")
    df = df.pivot(index=index, columns=columns, values=values)
    df.index.name = None
    df.columns.name = None
    return df

def pandas_interpolate(df):
    interpolated_df = pd.DataFrame()
    for ii in df.columns:
        my_df = df[[ii]]
        x = np.array([x for x in my_df.dropna().index])
        x_na = np.array(list(set(my_df.index.values)-set(my_df.dropna().index.values)))#np.array([x for x in my_df[my_df.isna()].index])
        x_na_filtered = x_na[(x_na>x[0]) & (x_na<x[-1])]
        x_complement = x_na[(x_na<=x[0]) | (x_na>=x[-1])]
        y = np.array([x[0] for x in my_df.dropna().values])
        f = interp1d(x, y.transpose())
        y_na_filtered = list(map(lambda x: [f(x)], x_na_filtered))
        y = list(map(lambda x: [x[0]], my_df.dropna().values.tolist()))
        nan_array = np.empty(np.shape(y[0]))
        if len(nan_array)>1:  
            nan_array=nan_array[0]
        nan_array[:]= np.nan
        interpolated_df[ii] = pd.Series(y+y_na_filtered+[nan_array for x in x_complement.tolist()],
                                        index=x.tolist()+x_na_filtered.tolist()+x_complement.tolist()).sort_index().apply(lambda x:x[0])
    return interpolated_df

def pandas_index_localize(df):
    df.index = df.index.map(lambda x: pd.Timestamp(x).tz_localize('UTC'))
    return df


