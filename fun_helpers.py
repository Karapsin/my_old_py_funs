import warnings
import time
import re
from date_funs import *
from pandas.api.types import is_numeric_dtype

#time+print
def time_print(string):
    return print((get_current_time()+' '+string))

#получить схему или табличку из строки
def get_schema_or_table_from_string(table_string, schema_or_table):
    
    if len(re.sub(r'".*"', '""', table_string).split('.'))>2:
        time_print(re.sub(r'".*"', '""', table_string).split('.'))
        raise ValueError('Используйте schema.table')
    
    new_string = table_string.replace('"', '')
    
    if schema_or_table=='schema':
        res=new_string.split('.')[0].strip()
        
    elif schema_or_table=='table':
        res='.'.join(new_string.split('.')[1:]).strip()
    
    else:
        raise ValueError('schema_or_table принимает значения schema или table')
        
    return res

def is_var_exists(var):
    try:
        eval(var)
    except:
        res=False
    else:
        res=True
    return res

#пробует функцию n раз
def try_n_times(function, params_list, n, cooldown):
    try_cnt=1
    finish=False
    
    while try_cnt<=n and finish==False:
        if n>1:
            time_print('попытка '+str(try_cnt)+' из '+str(n))
        try:
            res=function(*params_list)
            finish=True
                
        except Exception as e: 
            time_print(str(e))
            try_cnt+=1
            time_print('попытка '+str(try_cnt-1)+' провалена, ждём '+str(cooldown)+' с. и повторяем')     
            time.sleep(cooldown)
    
    time_print('успех' if finish else 'попытки исчерпаны')
    
    if not(finish):
        raise ValueError('превышен лимит попыток')
        
    return res

#убрать scientific notation
def remove_scientific_notation(input_num, precision=1):
    return ('{:.'+str(precision)+'f}').format(input_num)

#создать файлик с паролем (можно использовать в get_password)
def create_password_file(file_name, password):
    with open('/home/'+[x for x in os.getcwd().split('/') if 'omega-sbrf-' in x][0]+'/notebooks/.'+file_name, 'w') as f:
        f.write(password)
        f.close()

#получить пароль из скрытого файла
def get_password(file_name):
    return str(open("/home/"+[x for x in os.getcwd().split('/') if 'omega-sbrf-' in x][0]+"/notebooks/."+file_name, "r").readlines()[0].replace('\n',''))

#декартово произведение двух листов, пример:
# лист 1: а, б, в
# лист 2: 1, 2, 3
# итог: а1, а2, а3, б1, б2, б3, в1, в2, в3
def cartesian_product(list_1, list_2, delimiter='_'):
    res_list=[]
    for i in range(0, len(list_1)):
        for j in range(0, len(list_2)):
            res_list.append(str(list_1[i])+delimiter+str(list_2[j]))
                       
    return res_list


#генерит лист с числами от start до end
def gen_num_seq_list(start, end, step=1):
    res_list=[start]
    while res_list[-1]<end:
        res_list.append(res_list[-1]+step)
        
    if res_list[-1]>end:
        res_list=res_list[0:(len(res_list)-1)]
    
    return res_list

#получаем string из txt
def get_str_from_txt(txt_file):
    file=open(txt_file, 'r')
    res_var=[]
    for line in file:
        res_var.append(line)
    return ''.join(res_var)

#map который не нужно оборачивать в лист
def my_map(function, args_list):
    return list(map(function, *args_list))

#приветствие
def get_greet():
    current_hour=datetime.datetime.now().hour
    if (current_hour>=0 and current_hour<5) or current_hour==23:
        greet='доброй ночи'
    elif current_hour>=5 and current_hour<12:
        greet='доброе утро'
    elif current_hour>=12 and current_hour<17:
        greet='доброго дня'
    elif current_hour>=17 and current_hour<23:
        greet='добрый вечер'
        
    return greet