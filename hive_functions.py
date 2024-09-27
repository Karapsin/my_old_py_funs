import sys
sys.path.append('/usr/sdp/current/spark3-client/python/')
sys.path.append('/usr/sdp/current/spark3-client/python/lib/py4j-0.10.9.3-src.zip')
from GP_functions import *
from glob import glob
from pyspark import SparkContext, SparkConf, HiveContext
from defaults import *
from hive_users import *

def get_hive_connection(login):
    
    def get_connection():
        if(
            1==0
            or not('s_'+login+'_hive_context' in globals()) 
           ):

            time_print('подключение к hive')
            os.environ['SPARK_MAJOR_VERSION'] = '3'
            os.environ['SPARK_HOME'] = '/usr/sdp/current/spark3-client/'
            os.environ['PYSPARK_DRIVER_PYTHON'] = 'python3'
            os.environ['LD_LIBRARY_PATH'] = '/opt/python/virtualenv/jupyter/lib'
            os.environ['PYSPARK_PYTHON'] = 'python3'

            time_print('получение тикета') 
            user=login+'_omega-sbrf-ru'
            password=get_hive_pass(login)
            sp.Popen('kdestroy', shell = True, encoding = 'utf-8',  stdin=sp.PIPE)
            (
                sp
                .Popen(f'kinit {user}@DF.SBRF.RU', 
                       shell = True, 
                       encoding = 'utf-8',  
                       stdin=sp.PIPE
                 )
                .communicate(f'''{password}''')

            )
            time_print('тикет получен') 

            conf = (SparkConf().setAppName('2rgrgrg')\
                        .setMaster("yarn")\
                        .set('spark.executor.cores', '1')\
                        .set('spark.executor.memory', '4g')\
                        .set('spark.executor.memoryOverhead', '4g')\
                        .set('spark.driver.memory', '10g')\
                        .set('spark.driver.maxResultSize', '20g')\
                        .set('spark.shuffle.service.enabled', 'true')\
                        .set('spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive', 'true')\
                        .set('spark.dynamicAllocation.enabled', 'true')\
                        .set('spark.dynamicAllocation.executorIdleTimeout', '120s')\
                        .set('spark.dynamicAllocation.cachedExecutorIdleTimeout', '600s')\
                        .set('spark.dynamicAllocation.initialExecutors', '3')\
                        .set('spark.dynamicAllocation.maxExecutors', '40')\
                        .set('spark.port.maxRetries', '150'))

            globals()['s_'+login+'_spark_context'] = SparkContext.getOrCreate(conf=conf)
            globals()['s_'+login+'_hive_context'] = HiveContext(eval('s_'+login+'_spark_context'))
            time_print('подключено')
            
    try:
        get_connection()
        eval('s_'+login+'_hive_context').sql('select 1').toPandas()
    except Exception as e:
        time_print(str(e))
        time_print('подключение. попытка 2')
        exec('del '+'s_'+login+'_hive_context')
        get_connection()
        eval('s_'+login+'_hive_context').sql('select 1').toPandas()

def read_from_hive(comment, 
                   hive_sql_query, 
                   debug=False, 
                   my_login=my_login
    ):
   
    if debug:
        time_print(hive_sql_query)
    
    get_hive_connection(my_login)
    
    time_print(comment+': считывание данных в df')
    df=eval('s_'+my_login+'_hive_context').sql(hive_sql_query).toPandas()
    time_print(comment+': данные считаны')
    
    return df

def transfer_from_hive_to_GP(comment, 
                             hive_sql_query, 
                             gp_connection, 
                             gp_table,
                             my_login=my_login,
                             drop=True, 
                             data_types=None, 
                             distributed_by=None, 
                             debug=False
    ):
    
    df=read_from_hive(comment=comment, 
                      hive_sql_query=hive_sql_query, 
                      debug=debug,
                      my_login=my_login
        )
    cols=df.columns.values.tolist()
    
    #грузим в GP только если df не пустой
    load_to_GP(
                [
                 comment+': загрузка в GP',
                 gp_connection,
                 gp_table,
                 df,
                 drop
                ],
                debug=debug
    )
    
    #чистим память           
    del df               
    gc.collect()
    
    if not(data_types is None):
        alter_query_list=list(
                                map(lambda col, d_type:( 
                                                        ('ALTER TABLE '+gp_table+' ALTER COLUMN '+col+
                                                         ' SET DATA TYPE '+
                                                         (
                                                          (d_type+' USING '+col+'::'+d_type) 
                                                          if d_type!='timestamp' 
                                                          else ('timestamp USING ' +col+'::timestamp without time zone')
                                                         )
                                                        )
                                                      ), 
                                    cols, 
                                    data_types
                                )
                        )
        
        if debug:
            time_print(
                        ('ALTER TABLE '+gp_table+' SET distributed RANDOMLY;')+
                       ';'.join(alter_query_list)
            )

        execute_in_GP(
                      [
                       'проставляем типы данных',
                       ['capgp', '19345802'],
                       ('ALTER TABLE '+gp_table+' SET distributed RANDOMLY;')+
                       ';'.join(alter_query_list)
                      ]
        )
        
    if not(distributed_by is None):
        
        if debug:
            time_print(('ALTER TABLE '+gp_table+' SET distributed by ('+distributed_by+');'))
        
        execute_in_GP(
                      [
                       'установка distributed by',
                       ['capgp', '19345802'],
                       ('ALTER TABLE '+gp_table+' SET distributed by ('+distributed_by+');')
                      ]
        )
    
def transfer_from_hive_to_GP_by_key(comment, 
                                    hive_sql_query, 
                                    gp_connection, 
                                    gp_table, 
                                    key_field, 
                                    key_values, 
                                    drop_old, 
                                    try_cnt=1, 
                                    cooldown=0,
                                    data_types=None, 
                                    distributed_by=None, 
                                    debug=False
    ):
    
    if not('{transfer_condition}' in hive_sql_query):
            raise ValueError('Запрос должен содержать {transfer_condition} в WHERE')
    
    for i in range(len(key_values)):
        time_print('обработка '+key_field+'='+key_values[i])
        
        def do_value_transfer():
            transfer_from_hive_to_GP(comment=comment,
                                     hive_sql_query=(hive_sql_query
                                                      .format(transfer_condition="{key_field}='{key_value}'")
                                                      .format(key_field=key_field,
                                                              key_value=key_values[i]
                                                       )
                                                     ),
                                     gp_connection=gp_connection,
                                     gp_table=gp_table,
                                     drop=(True if (i==0 and drop_old) else False),
                                     data_types=(data_types if i==0 else None),
                                     distributed_by=(distributed_by if i==0 else None),
                                     debug=debug
            )
        
        try_n_times(do_value_transfer,
                    [],
                    try_cnt,
                    cooldown   
        )
    time_print('перенос завершён')
    
del my_email
del my_login