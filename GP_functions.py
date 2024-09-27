# coding: utf-8
import subprocess as sp
import gc
import numpy as np
import psycopg2
import io
import csv
import math
import itertools
from GP_users import *
from GP_bd import *
from fun_helpers import *

import psycopg2.extras
psycopg2.extras.register_default_json(loads=lambda x: x)
psycopg2.extras.register_default_jsonb(loads=lambda x: x)

#проверяет есть ли подключение, если его нет - создаёт
#первый аргумент - псевдоним БД (без 2, пример capgp2=capgp)
#второй - юзер от которого подключаемся (должен быть файлик с паролем в wd)
def get_GP_connection(con_params):
    
    if(
        1==0
        or not(con_params[0] in globals()) 
        or eval(con_params[0]).closed>0 
        or (con_params[0] in globals() 
            and 
            (eval(con_params[0]).dsn.split()[0].replace('user=', ''))!=con_params[1]
           )
       ):

        #host, db
        db_host=get_dbname_and_host(con_params[0])
        db_name=db_host[0]
        host_name=db_host[1]
        
        #user, pass
        user_and_pass_file=get_user_and_pass_file(con_params[1])
        user=user_and_pass_file[0]
        password=user_and_pass_file[1]
         
        time_print('получение тикета')
        sp.Popen(bytes('kdestroy', 'utf-8'), shell = True,  stdin=sp.PIPE)
        (
            sp.Popen(bytes("kinit {user}@OMEGA.SBRF.RU".format(user=user), 'utf-8'), shell = True, stdin=sp.PIPE)
            .communicate(bytes("{password}".format(password=password), 'utf-8'))

        ) 
        time_print('тикет получен') 
        
        #создание
        time_print('подключение к '+con_params[0])
        globals()[con_params[0]] = psycopg2.connect(host=host_name, 
                                                    port='5432', 
                                                    database=db_name, 
                                                    user=con_params[1], 
                                                    password=password,
                                                    options='-c statement_timeout=14400s'
                                    )

#проверяет есть соединение, если нет - создаёт
def try_connection(con_params):
    try:
        get_GP_connection(con_params)
        pd.read_sql("""SELECT 1""", eval(con_params[0]))   
    except Exception as e:
        time_print(str(e))
        time_print('подключение. попытка 2')
        eval(con_params[0]).close()
        get_GP_connection(con_params)
        pd.read_sql("""SELECT 1""", eval(con_params[0]))
        
    time_print('подключение к '+con_params[0]+' активно')

def try_wrapper(fun, try_cnt, cooldown):
    if try_cnt>1:
        res=try_n_times(fun,
                        [],
                        try_cnt,
                        cooldown
            )
    else:
        res=fun()    
        
    return res

#выполняет запрос в GP
#params:
#0: stage
#1: con_params
#2: query
def execute_in_GP(params_list, debug=False, try_cnt=1, cooldown=0):
    
    def execute():
        time_print(params_list[0])

        if len(params_list)!=3:
            raise ValueError('Неверное число элементов в params_list')

        if debug:
            time_print(params_list[2])

        try_connection(params_list[1])

        time_print('старт запроса')
        eval(params_list[1][0]).cursor().execute(params_list[2])
        eval(params_list[1][0]).commit()
        time_print('запрос исполнен') 
    
    try_wrapper(execute, try_cnt, cooldown)
    
#читает данные из GP в df
#0: stage
#1: con_params     
#2: sql
#чтобы не было потери точности bigint кастим как text
def read_from_GP(params_list, debug=False, try_cnt=1, cooldown=0, order_list=[]):

    def read():
        time_print(params_list[0])
        if len(params_list)!=3:
            raise ValueError('Неверное число элементов в params_list')

        if debug:
            print(params_list[2])

        try_connection(params_list[1])

        clean_query=params_list[2].rstrip()

        if clean_query[-1]==';':
            clean_query=clean_query[:-1]    
           
        execute_in_GP(
                      [
                       'создание временной таблички для чтения',
                       params_list[1],
                       """
                       drop table if exists name_which_no_one_will_pick_used_in_py_script;
                       create temporary table name_which_no_one_will_pick_used_in_py_script as(
                          """+clean_query+"""
                        ) DISTRIBUTED RANDOMLY;
                       """     
                      ],
                      debug
        )

        time_print('генерация bigint::text, jsonb::text')
        sql_update=pd.read_sql(
                                    """
                                    select 
                                           coalesce(array_to_string(Array_Agg(string), ' '), 'select 1') as result
                                    from(
                                        SELECT format(
                                          'ALTER TABLE %I ALTER COLUMN %I SET DATA TYPE %s;',
                                          'name_which_no_one_will_pick_used_in_py_script',                                                  
                                          column_name,
                                          data_type_new
                                        ) as string
                                        from(
                                        select t.*,
                                              'text' as data_type_new
                                        FROM information_schema.columns t
                                        WHERE 1=1
                                              and data_type in ('bigint', 'jsonb')
                                              and table_schema = 'pg_temp_'||(select sess_id from pg_stat_activity where pid = pg_backend_pid())::text
                                              and table_name='name_which_no_one_will_pick_used_in_py_script'
                                        ) as s
                                    ) as sub;
                                    """, 
                                   eval(params_list[1][0])

                     )['result'].tolist()[0]
        time_print('успешно') 

        execute_in_GP(
                      [
                       'bigint::text, jsonb::text',
                       params_list[1],
                       sql_update    
                      ],
                      debug
        )
 
        if len(order_list)>0:
            for i in range(len(order_list)):
                if order_list[i] is None:
                    order_list[i]=''
                else:
                    order_list[i]=str(i+1)+' '+order_list[i]
            order_by='order by '+(', '.join([x for x in order_list if x!='']))
            
        time_print('загрузка данных в память')
        res=pd.read_sql('select * from name_which_no_one_will_pick_used_in_py_script '+(order_by if len(order_list)>0 else ''), 
                        eval(params_list[1][0])
            )
        
        time_print('данные считаны') 
        execute_in_GP(
                      [
                       'удаление временной таблички для чтения',
                       params_list[1],
                       """
                       drop table if exists name_which_no_one_will_pick_used_in_py_script;
                       """     
                      ],
                      debug
        )
    
        return res
        
    res=try_wrapper(read, try_cnt, cooldown)

    return res
    
#генерирует create table в sql на основе df или select
#con_params: если на вход select, то тут подключение по которому тянем его
#debug: print generated sql
def generate_create_table_statement(df, table_name, debug=False, con_params=None, temporary=False):
    
    temp_part= 'temporary' if temporary else ''
    
    if isinstance(df, pd.DataFrame):
        df=df.head(min(100, df.shape[0]))

        def get_data_col_types(column_name, column_type):
            if(column_type == 'int64'):
                res='"'+str(column_name)+'" '+'bigint'
            elif (column_type == 'float64'):
                res='"'+str(column_name)+'" '+'float'
            elif (column_type == 'bool'):
                res='"'+str(column_name)+'" '+'boolean'
            else: res='"'+str(column_name)+'" '+'text'
            return res
        
        create_inner_part=', '.join(
                                    list(map(get_data_col_types, 
                                             list(df.columns.values), 
                                             df.dtypes
                                        ))
                                )
        
        sql_create=(
                    'create '+temp_part+' table '+table_name+' ('+
                    create_inner_part+
                    ');'
                   )

    elif isinstance(df, str):
        
        #убрать ; если есть
        df=df.rstrip()
        if df[-1]==';':
            df=df[:-1]
            
        execute_in_GP(
                      [
                       'создание временной таблички для определения типов данных',
                       con_params,
                       """
                       drop table if exists name_which_no_one_will_pick_used_in_py_script_2;
                       create temporary table name_which_no_one_will_pick_used_in_py_script_2 as(
                            select *
                            from(
                                 """+df+"""
                            ) as sub
                            limit 0
                        );
                       """        
                      ],
                      debug
        )
        
        types_df=pd.read_sql( """ 
                                     select 
                                          ordinal_position,
                                          '"'||column_name||'"'||' '||data_type as gen_str
                                    from information_schema.columns
                                    where 1=1
                                          and table_schema = 'pg_temp_'||(select sess_id from pg_stat_activity where pid = pg_backend_pid())::text
                                          and table_name='name_which_no_one_will_pick_used_in_py_script_2'
                                    order by 1 
                                """, 
                                    eval(con_params[0])
                    )
        
         
        create_inner_part=', '.join(types_df['gen_str'])
        
        sql_create=(
                    'create '+temp_part+' table '+table_name+' ('+
                    create_inner_part+
                    ');'
                   )
        
    else:
        raise ValueError('Неизвестный тип. Создать табличку можно только на основу df или select.')

 
    if debug:
        print(sql_create)
        
    return sql_create

#загружает данные из df в GP
#params:
#0: stage
#1: con_params     
#2: table_name
#3: df
#4: drop_old (true/false)
#debug: print generated sql
#sql_to_create: опционально, генерит табличку не на основе df, а на основе sql
#create_transfer_table: сгенерить трансфертную табличку (нужно если она не создана)
#reuse_transfer_table: оставить трансфертную табличку (truncate, а не drop) для переиспользования
#distributed_by: если None, то проставляется автоматически, иначе указанная колонка
#to_temporary_table: True если загружаем во временную табличку

#идея такая:
#загружаем данные в питон (bigint )
#загружаем данные в трансфертную табличку 
#в трансфертной табличке возвращаем bigint
#insert из трансфертной в целевую
#трансфертную truncate (если надо переиспользовать) или drop (если она потом не нужна)

#суть переиспользования транфертной таблички (create_transfer_table, reuse_transfer_table):
#на её создание из-за обращения к information_schema и использования sess_id уходит много времени
#для, например, многоэтапной перекладки табличка в плане названий колонок и типов данных будет всегда одинакова
#поэтому мы её просто truncate, а не drop, чтоб не надо было генерить заново

def load_to_GP(params_list, 
               debug=False, 
               sql_to_create=None, 
               con_params_for_data_types=None, 
               create_transfer_table=True, 
               reuse_transfer_table=False,
               distributed_by=None,
               to_temporary_table=False, 
               try_cnt=1, 
               cooldown=0
    ):
    
    def load():
        def create_table():
            #зачистка
            time_print('сброс старой таблички')
            eval(params_list[1][0]).cursor().execute(""" drop table if exists """+params_list[2])
            eval(params_list[1][0]).commit()

            #создаём новую табличку
            time_print('создание новой таблички')
            sql_create=generate_create_table_statement(df=params_list[3] if sql_to_create is None else sql_to_create, 
                                                       table_name=params_list[2], 
                                                       debug=debug,
                                                       con_params=con_params_for_data_types
                      )

            eval(params_list[1][0]).cursor().execute(sql_create)
            eval(params_list[1][0]).commit() 
            del sql_create

        def create_transfer_table():
            time_print('генерим int::numeric, jsonb::text')
            sql_updates=pd.read_sql("""
                                        select 
                                               coalesce(array_to_string(Array_Agg(string), ' '), 'select 1') as result
                                        from(
                                            SELECT format(
                                              'ALTER TABLE %I.%I ALTER COLUMN %I SET DATA TYPE %s;',
                                             '"""+schema_name+"""',
                                              '"""+transfer_table_name_no_schema+"""',
                                              column_name,
                                              data_type_new
                                            ) as string
                                            from(
                                            select t.*,
                                                   case 
                                                        when data_type in ('bigint', 'smallint', 'integer')
                                                            then 'numeric'
                                                        when data_type in ('jsonb')
                                                            then 'text'
                                                   end as data_type_new
                                            FROM information_schema.columns t
                                            WHERE 1=1
                                                  and data_type in ('bigint', 'smallint', 'integer', 'jsonb')
                                                  and table_schema =  '"""+schema_name+"""'
                                                  and table_name= '"""+target_table_name_no_schema+"""'
                                            ) as s
                                        ) as sub;
                                        """, 
                                        eval(params_list[1][0])
                        )['result'].tolist()[0]
            time_print('успешно')

            execute_in_GP(
                           [
                           'создаём трансфертную табличку', 
                            params_list[1], 
                            """ drop table if exists """+transfer_table_name+""";
                            create table """+transfer_table_name+""" as (
                            select *
                            from """+params_list[2]+"""
                            limit 0
                            ) DISTRIBUTED RANDOMLY;
                            """+sql_updates
                           ],
                            debug
            )
            del sql_updates

        def clear_transfer_table():
            time_print('очищение трансфертной таблички')
            eval(params_list[1][0]).cursor().execute(('drop table ' if not(reuse_transfer_table) else 'truncate ')+transfer_table_name)
            eval(params_list[1][0]).commit() 
            time_print('успешно')

        def create_nedeed_tables():
            if params_list[4]:
                create_table()
            create_transfer_table()

        if len(params_list)!=5:
            raise ValueError('Неверное число элементов в params_list')

        #подключение
        try_connection(params_list[1])

        if not(con_params_for_data_types is None):
            try_connection(con_params_for_data_types)

        if to_temporary_table:
            temp_table_schema_name=read_from_GP(
                                                  [
                                                   'получаем название схемы для временной таблички',
                                                    params_list[1],
                                                    """
                                                    select 'pg_temp_'||(select sess_id from pg_stat_activity where pid = pg_backend_pid())::text as "name"
                                                    """
                                                  ]
                                    )['name'][0]
            params_list[2]=temp_table_schema_name+'.'+params_list[2]

        #из входных данных получаем названия схем и табличек 
        no_last_quote_table_name=params_list[2][:-1] if params_list[2][-1]=='"' else params_list[2]
        transfer_table_name=no_last_quote_table_name+'_temp_transfer'+('"' if params_list[2][-1]=='"' else '')
        schema_name=get_schema_or_table_from_string(params_list[2], 'schema')
        transfer_table_name_no_schema=get_schema_or_table_from_string(transfer_table_name, 'table')
        target_table_name_no_schema=get_schema_or_table_from_string(params_list[2], 'table')
        transfer_table_only_schema=get_schema_or_table_from_string(transfer_table_name, 'schema')

        if params_list[3].shape[0]==0:

            if create_transfer_table:
                create_nedeed_tables()

            time_print('загружать пустую табличку не требуется')
            clear_transfer_table()

        else:
            time_print(params_list[0])

            eval(params_list[1][0]).cursor().execute('SET search_path TO {schema}'.format(schema=schema_name)) 
            eval(params_list[1][0]).commit()

            create_nedeed_tables()

            #загрузкa
            time_print('загрузка данных')
            csv_io = io.StringIO()
            params_list[3].to_csv(csv_io, sep='\t', header=False, index=False, na_rep='NaN')
            csv_io.seek(0)

            try:
                eval(params_list[1][0]).cursor().copy_expert(
                                                            'COPY '+
                                                            transfer_table_name+
                                                            """ FROM STDIN WITH (NULL 'NaN', DELIMITER '\t', FORMAT CSV)""", 
                                                            csv_io
                                                 )
            except Exception as e:
                execute_in_GP(['', params_list[1], """ truncate """+transfer_table_name])
                eval(params_list[1][0]).cursor().copy_from(csv_io, transfer_table_name_no_schema, null='NaN')

            time_print('данные загружены в трансфертную табличку')

            time_print('генерация alter table для возвращения правильных типов данных (numeric::bigint и т.д.)')
            sql_updates=pd.read_sql("""
                                            select 
                                                   coalesce(array_to_string(Array_Agg(string), ' '), 'select 1') as result
                                            from(
                                                SELECT format(
                                                  'ALTER TABLE %I.%I ALTER COLUMN %I SET DATA TYPE %s using %I::%s;',
                                                  tr_table_name_schema,
                                                  tr_table_name_table,
                                                  column_name,
                                                  data_type,
                                                  column_name,
                                                  data_type
                                                ) as string
                                                from(
                                                select t.*,
                                                       '{tr_table_name_schema}' as tr_table_name_schema,
                                                       '{tr_table_name_table}' as tr_table_name_table
                                                FROM information_schema.columns t
                                                WHERE 1=1
                                                      and data_type in ('bigint', 'smallint', 'integer', 'jsonb')
                                                      and table_schema =  '{schema}'
                                                      and table_name= '{table}'
                                                ) as s
                                            ) as sub;
                                            """.format(tr_table_name_schema=transfer_table_only_schema,
                                                       tr_table_name_table=transfer_table_name_no_schema,
                                                       schema=schema_name,
                                                       table=target_table_name_no_schema
                                                ), 
                                            eval(params_list[1][0])
                            )['result'].tolist()[0]
            if debug:
                time_print(sql_updates)
            time_print('успешно')

            execute_in_GP(
                            [
                              'возвращение типов данных',
                               params_list[1],
                               sql_updates
                           ]
            )

            time_print('перенос данных из трансфертной таблички в основную')
            eval(params_list[1][0]).cursor().execute('insert into '+params_list[2]+' select * from '+transfer_table_name) 
            time_print('успешно')

            clear_transfer_table()

            if not(distributed_by is None):
                time_print('устанавливаем distributed by '+distributed_by)
                if debug:
                    time_print('ALTER TABLE '+params_list[2]+' SET distributed  BY ('+distributed_by+');')

                eval(params_list[1][0]).cursor().execute('ALTER TABLE '+params_list[2]+' SET distributed  BY ('+distributed_by+');')
                eval(params_list[1][0]).commit() 
                time_print('успешно')

            time_print('загружено успешно')
            
    try_wrapper(load, try_cnt, cooldown)

def do_vacuum(con_params, 
              table_to_vacuum, 
              full=False,
              try_cnt=1, 
              cooldown=0
    ):
    
    def vacuum():
        time_print('vacuum')
        try_connection(con_params)
        prev_isol=eval(con_params[0]).isolation_level
        eval(con_params[0]).set_isolation_level(0)
        eval(con_params[0]).cursor().execute('vacuum '+('full ' if full else '')+table_to_vacuum)
        eval(con_params[0]).commit()
        eval(con_params[0]).set_isolation_level(prev_isol)
        time_print('успешно')
        
    try_wrapper(vacuum, try_cnt, cooldown)
    
#считывает данные из GP и перекладывает их
#params:
#0: stage
#1: con_params_read
#2: read_query
#3: con_params_load
#4: table_to_transfer
#debug: print generated sql
#distributed_by: если None, то автоматически, иначе то что указано
def transfer_between_db_in_GP(params_list, 
                              debug=False, 
                              distributed_by=None, 
                              to_temporary_table=False,
                              try_cnt=1, 
                              cooldown=0
    ):

    def transfer():
        time_print(params_list[0])        

        if len(params_list)!=5:
            raise ValueError('Неверное число элементов в params_list')

        if '{transfer_condition' in params_list[2]:
            raise ValueError('Используйте {transfer_condition} только для многоэтапной перекладки')

        try_connection(params_list[1])    
        try_connection(params_list[3])

        load_to_GP(
                   [
                    params_list[0]+': загрузка',
                    params_list[3],
                    params_list[4],
                    read_from_GP(   
                                 [
                                 params_list[0]+': чтение',
                                 params_list[1],
                                 params_list[2]
                                 ],
                                 debug=debug
                    ),
                    True
                   ],
                   debug=debug,
                   sql_to_create=params_list[2],
                   con_params_for_data_types=params_list[1],
                   distributed_by=distributed_by,
                   to_temporary_table=to_temporary_table
        )

        #чистка памяти
        gc.collect()

        time_print('перенос завершён')
        
    try_wrapper(transfer, try_cnt, cooldown)
   

#считывает данные из GP и перекладывает их
#params:
#0: stage
#1: con_params_read
#2: read_query
#3: con_params_load
#4: table_to_transfer
#5: лист с названиями ключей, которые используем для многоэтапной перекладки
#6: logical: перекладывать всё, или только то, чего нет (True - всё)

#transfer_values лист с листами, каждый из которых содержит значения ключа для перекладки 
#(опционально)

#debug: print generated sql
#distributed_by: если None, то автоматически, иначе то что указано

#попытки:
#global_try_cnt: число попыток запуска функции
#global_cooldown: время (в секундах) между попытками запуска функции
#try_cnt: число попыток для прогрузки 1 значения ключа
#cooldown: время между попытками
#target_table_keys_aliases_list - 

#пример использования попыток:
#перекладываем данные с 1 по 5 марта
#1,2,3 марта - переложились
#на 4-ом отрубился connection
#тогда ждём сколько-то времени и пытаемся ещё раз 
#(пока не истратим попытки)
#в итоге уже переложенные данные за 1,2,3 марта не утрачиваются
#4 марта либо перекладывается на какой-то из попыток, либо выдаёт ошибку
def transfer_between_db_in_GP_by_key(params_list, 
                                     debug=False, 
                                     distributed_by=None, 
                                     transfer_values=None, 
                                     vacuum_every_step=False,
                                     to_temporary_table=False,
                                     global_try_cnt=1, 
                                     global_cooldown=0,
                                     try_cnt=1, 
                                     cooldown=0,
                                     target_table_keys_alias_list=None
                                    ):
    
    def transfer(transfer_values=transfer_values, target_table_keys_alias_list=target_table_keys_alias_list):
    
        if len(params_list)!=7:
            raise ValueError('Неверное число элементов в params_list')
            
        if not(isinstance(params_list[5], list)):
            params_list[5]=[params_list[5]]

        if target_table_keys_alias_list is None and not(params_list[6]):
            target_table_keys_alias_list=params_list[5]
        
        if not(transfer_values is None) and not(isinstance(transfer_values[0], list)):
            transfer_values=[transfer_values]

        time_print(params_list[0]) 

        if not('{transfer_condition' in params_list[2]):
            raise ValueError('Запрос должен содержать {transfer_condition} в WHERE')


        transfer_conditions_list=list(set([x for x in (re.findall(r"{(\w+)}", params_list[2])) if 'transfer_condition' in x]))
        appendix_list=[x.replace('transfer_condition', '') for x in transfer_conditions_list]

        if len(transfer_conditions_list)!=len(params_list[5]):
            raise ValueError('Число различных {transfer_condition} должно совпадать с числом ключей для переноса')

        if not(transfer_values is None) and (len(transfer_values)!=len(params_list[5])):
            raise ValueError('Число листов в transfer_values должно совпадать с числом ключей для переноса')

        if not(transfer_values is None) and (len(transfer_values)!=len(transfer_conditions_list)):
            raise ValueError('Число листов в transfer_values должно совпадать с числом различных {transfer_conditions}')

        #удалить ; из конца если есть
        clean_query=params_list[2].rstrip()

        if clean_query[-1]==';':
            clean_query=clean_query[:-1]


        def gen_format_inner_part(transfer_conditions_list):
            return (', '.join(
                              list(
                                    map(lambda x: x+"='1=1'", 
                                        transfer_conditions_list
                                    )
                              )
                         )
                 )

        #получить список уникальных значений
        #если они не поданы - получаем их sql запросом
        #cчитывание значений
        if transfer_values is None:
            values_list=gen_num_seq_list(0, len(params_list[5])-1)
            for i in range(len(params_list[5])):
                get_distinct_query=(
                                    'SELECT DISTINCT ('+
                                     params_list[5][i]+')::text'+
                                     ' FROM ('+
                                     clean_query+
                                     ') as sub '+
                                     'order by 1'
                                    )
                get_distinct_query=eval('get_distinct_query.format('+gen_format_inner_part(transfer_conditions_list)+')')
                if debug:
                    time_print(get_distinct_query)

                read_distinct_params=[
                                      'Получение значений '+params_list[5][i],
                                       params_list[1],
                                       get_distinct_query
                                     ]


                try_connection(params_list[1])
                values_list[i]=read_from_GP(read_distinct_params).iloc[:,0].tolist()

        else:
            values_list=list(map(lambda x: list(map(str, x)), transfer_values))

        try_connection(params_list[3])
        
        values_list=list(itertools.product(*values_list))

        #если перекладываем не всё: загружаем то что уже есть, удаляем это из value_list
        if params_list[6]:

            if debug:
                time_print(""" DROP TABLE IF EXISTS """+ params_list[4])
            execute_in_GP(
                            [
                             'удаление старой '+ params_list[4], 
                             params_list[3], 
                             """ DROP TABLE IF EXISTS """+ params_list[4]
                            ]
            )

            create_query=generate_create_table_statement(eval('params_list[2].format('+
                                                              gen_format_inner_part(transfer_conditions_list)+
                                                              ')'
                                                         ), 
                                                         params_list[4], 
                                                         debug, 
                                                         params_list[1],
                                                         to_temporary_table
                          )

            if debug:
                time_print(create_query)
            execute_in_GP(
                            [
                                'создание новой таблицы', 
                                 params_list[3], 
                                 create_query
                            ]
            )

        else:
            already_transfer_values=gen_num_seq_list(0, len(transfer_conditions_list)-1)
            cols_str=','.join(list(map(lambda x: '('+x+')::text', target_table_keys_alias_list)))
            order_by=','.join(list(map(lambda x: str(x), gen_num_seq_list(1, len(transfer_conditions_list)))))
            
            sql=("SELECT DISTINCT "+cols_str+
                 ' FROM '+params_list[4]+' as sub '+
                 f"order by {order_by}")
            
            if debug:
                time_print(sql)
            
            already_transferred=read_from_GP(
                                             [
                                                 'Получение уже переложенных значений', 
                                                 params_list[3], 
                                                 sql
                                             ]
                                ).apply(tuple, axis=1).tolist()
            time_print('перенесено: '+str(already_transferred))
            time_print('фильтруем от уже перенесённых значений')
            values_list=[x for x in values_list if x not in already_transferred]
                
            time_print('осталось перенести: '+str(values_list))

        step=0
        print('----------СТАРТ МНОГОЭТАПНОЙ ПЕРЕКЛАДКИ------------')
        for i in range(len(values_list)):

            step=step+1
            time_print('Шаг '+str(step)+' из '+str(len(values_list)))

            current_tuple=list(values_list[i])

            ##############################################################
            for j in range(len(current_tuple)):

                value=current_tuple[j]

                if value is None or pd.isnull(value):
                    current_tuple[j]=' IS NULL'
                elif isinstance(value, int) or isinstance(value, float) or isinstance(value, np.int64):
                    current_tuple[j]="="+str(value) 
                else:
                    #replace и E на случай сложных строк с множеством кавычек типа ООО "'''ЭТ-алон""
                    current_tuple[j]="=E'"+(str(value).replace("'", "\\\\'").replace('"', '\\\\"'))+"'"
            ##############################################################

            first_format=gen_num_seq_list(0, len(transfer_conditions_list)-1)
            second_format=gen_num_seq_list(0, len(transfer_conditions_list)-1)
            for j in range(len(transfer_conditions_list)):
                field_name='field'+appendix_list[j]
                field_value='field_value'+appendix_list[j]

                gen1=transfer_conditions_list[j]+"='{"+field_name+'}{'+field_value+"}'"
                gen2=field_name+'=""" '+params_list[5][j]+'""", '+field_value+'="""'+current_tuple[j]+'"""'

                first_format[j]=gen1
                second_format[j]=gen2

            str_query='clean_query.format('+(', '.join(first_format))+').format('+(', '.join(second_format))+')'
            if debug:
                time_print(str_query)
                time_print(eval(str_query))

            #добавляем условие 
            new_query=eval(str_query)

            if debug:
                time_print(new_query)

            try_connection(params_list[1])

            def do_value_transfer():
                load_to_GP(
                           [
                             'загрузка '+str(params_list[5])+' '+str(list(values_list[i])),
                             params_list[3],
                             params_list[4],
                             read_from_GP(
                                           [
                                               'чтение '+str(params_list[5])+' '+str(list(values_list[i])), 
                                               params_list[1], 
                                               new_query
                                           ]
                             ),
                            (True if (step==1 and params_list[6]) else False)
                           ],
                           debug=debug,
                           create_transfer_table=(True if step==1 else False),
                           reuse_transfer_table=(True if step<len(values_list) else False),
                           con_params_for_data_types=params_list[1],
                           sql_to_create=eval('params_list[2].format('+gen_format_inner_part(transfer_conditions_list)+')'),
                           distributed_by=(None if step<len(values_list) else distributed_by),
                           to_temporary_table=to_temporary_table
                )

            try_n_times(do_value_transfer,
                        [],
                        try_cnt,
                        cooldown
            )

            if vacuum_every_step:
                do_vacuum(params_list[3], params_list[4])

            #чистка памяти
            gc.collect()

        print('----------КОНЕЦ МНОГОЭТАПНОЙ ПЕРЕКЛАДКИ------------')

        time_print('перенос завершён')
        
    try_wrapper(transfer, try_cnt, cooldown)
    
    
def get_query_info(database, 
                   user,
                   try_cnt=1, 
                   cooldown=0
    ):
    
    res=read_from_GP(
                     [
                         'получение данных о запросах', 
                          [database, user], 
                          f"""
                          Select 
                                  pid,
                                  application_name,
                                  (xact_start+'3 hours'::interval) as xact_start,
                                  state,
                                  (state_change+'3 hours'::interval) as state_change,
                                  waiting,
                                  waiting_reason,
                                  query           
                          from pg_stat_activity 
                          Where usename = '{user}';
                          """
                     ],
                     try_cnt=try_cnt,
                     cooldown=cooldown
       )
    
    return res

def cancel_query(database, 
                 user, 
                 pid,
                 try_cnt=1, 
                 cooldown=0
    ):
    
    execute_in_GP(
                     [
                         'отмена запроса pid='+pid, 
                          [database, user], 
                          f"""
                          select pg_cancel_backend({pid});
                          select pg_terminate_backend({pid});
                          """
                     ],
                     try_cnt=try_cnt,
                     cooldown=cooldown
    )
    
def cursor_transfer(start_db,
                    sql,
                    end_db,
                    target_table,
                    drop=True,
                    batch_size=100000,
                    to_temporary_table=True,
                    debug=False,
                    try_cnt=1,
                    cooldown=0
    ):
    
    def transfer(target_table=target_table):
        try_connection(start_db)
        try_connection(end_db)

        if drop: 
            
            if to_temporary_table:
                pd.read_sql("""select 1""",  eval(end_db[0]))
                temp_table_schema_name=read_from_GP(
                                                      [
                                                       'получаем название схемы для временной таблички',
                                                        end_db,
                                                        """
                                                        select 'pg_temp_'||(select sess_id from pg_stat_activity where pid = pg_backend_pid())::text as "name"
                                                        """
                                                      ]
                                      )['name'][0]
                target_table=temp_table_schema_name+'.'+target_table
                
            sql_create=((f'DROP TABLE IF EXISTS {target_table};')+
                         generate_create_table_statement(df=sql, 
                                                         table_name=target_table, 
                                                         con_params=start_db
                        )
                       ) 

            if debug:
                time_print(sql_create)

            execute_in_GP(
                           [
                              'создание целевой таблички',
                               end_db,
                               sql_create
                           ]
            )

        with eval(start_db[0]).cursor(name='start') as cursor:
            cursor.itersize=batch_size

            time_print('cursor.execute')
            if debug:
                time_print(sql)

            cursor.execute(sql)
            time_print('успех')

            n=0
            while True:

                time_print(f"""перенесено {("{:,}").format(n).replace(',', ' ')} строк""")
                time_print(f"""начинаем перенос следующих {("{:,}").format(batch_size).replace(',', ' ')} строк""")

                time_print(f'прогрузка из {start_db[0]}')
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    time_print("успех, все строки перенесены")
                    break
                time_print('успех')

                time_print('перенос в csv')
                csv_io = io.StringIO()
                writer = csv.writer(csv_io)
                writer.writerows(rows)
                time_print('успех')

                time_print(f'прогрузка в {end_db[0]}')
                csv_io.seek(0)
                with eval(end_db[0]).cursor() as cursor2:
                    cursor2.copy_expert(f'COPY {target_table} FROM STDIN WITH (NULL \'\', DELIMITER \',\', FORMAT CSV)',
                                        csv_io
                    )
                    eval(end_db[0]).commit()

                n=n+batch_size
                time_print('успех')

    try_wrapper(transfer, try_cnt, cooldown)