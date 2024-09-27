from fun_helpers import *
path_to_modules_dir=(os.path.dirname(os.path.realpath(__file__)))+'/'

def clear_users_settings():
    gp_users="""\
from fun_helpers import *
def get_user_and_pass_file(input_username):
    
    if 1==0:
        user='Так не бывает'
        password='Так не бывает'
    
    else:
        raise ValueError('Неизвестный пользователь')
        
    return [user, password]
"""
    
    hive_users="""\
from fun_helpers import *
def get_hive_pass(input_username):
    
    if 1==0:
        password='Так не бывает'

    else:
        raise ValueError('Неизвестный пользователь')
        
    return password    
"""
    
    with open(path_to_modules_dir+'GP_users.py', 'w') as f:
        f.write(gp_users)
        f.close()
        
    with open(path_to_modules_dir+'hive_users.py', 'w') as f:
        f.write(hive_users)
        f.close()
        
    with open(path_to_modules_dir+'defaults.py', 'w') as f:
        f.write('')
        f.close()

def add_new_GP_db(python_name, db_name, host_name):
    code_str_list=get_str_from_txt(path_to_modules_dir+'GP_bd.py').split('\n')
    
    for i in range(len(code_str_list)):
        if code_str_list[i]==f"""    elif input_dbname=='{python_name}':""":
            raise ValueError('Уже существуют БД с таким именем')
            
    for i in range(len(code_str_list)):
        if code_str_list[i]=='    else:':
            else_line_num=i

    new_part_lines=[
    f"""    elif input_dbname=='{python_name}':""",
    f"""        host_name='{host_name}'""",
    f"""        db_name='{db_name}'""",
    ''
    ]

    new_code_str=(
                    code_str_list[0:else_line_num]+
                    new_part_lines+
                    code_str_list[else_line_num:(else_line_num+5)]
                )
    new_code_str='\n'.join(new_code_str)

    with open(path_to_modules_dir+'GP_bd.py', 'w') as f:
        f.write(new_code_str)
        f.close()

def remove_GP_db(to_remove):
    code_str_list=get_str_from_txt(path_to_modules_dir+'GP_bd.py').split('\n')
    
    for i in range(len(code_str_list)):
        if code_str_list[i]==f"""    elif input_dbname=='{to_remove}':""":
            elif_line_num=i

    if not('elif_line_num' in locals()):
        raise ValueError('Не существуют БД с таким именем')


    filtered_code='\n'.join([ele for i, ele in enumerate(code_str_list) if i not in [elif_line_num, elif_line_num+1, elif_line_num+2, elif_line_num+3]])
    with open(path_to_modules_dir+'GP_bd.py', 'w') as f:
        f.write(filtered_code)
        f.close()
        
def add_new_GP_user(user, pass_file):
    code_str_list=get_str_from_txt(path_to_modules_dir+'GP_users.py').split('\n')

    for i in range(len(code_str_list)):
        if code_str_list[i]==f"""    elif input_username=='{user}':""":
            raise ValueError('Уже существует такой пользователь')
            
    for i in range(len(code_str_list)):
        if code_str_list[i]=='    else:':
            else_line_num=i

    new_part_lines=[
    f"""    elif input_username=='{user}':""",
    f"""        user='{user}'""",
    f"""        password=get_password('{pass_file}')""",
    ''
    ]

    new_code_str=(
                    code_str_list[0:else_line_num]+
                    new_part_lines+
                    code_str_list[else_line_num:(else_line_num+5)]
                )
    new_code_str='\n'.join(new_code_str)

    with open(path_to_modules_dir+'GP_users.py', 'w') as f:
        f.write(new_code_str)
        f.close()
        
def remove_GP_user(to_remove):
    code_str_list=get_str_from_txt(path_to_modules_dir+'GP_users.py').split('\n')
    for i in range(len(code_str_list)):
        if code_str_list[i]==f"""    elif input_username=='{to_remove}':""":
            elif_line_num=i

    if not('elif_line_num' in locals()):
        raise ValueError('Не существует такого пользователя')

    filtered_code='\n'.join([ele for i, ele in enumerate(code_str_list) if i not in [elif_line_num, elif_line_num+1, elif_line_num+2, elif_line_num+3]])
    with open(path_to_modules_dir+'GP_users.py', 'w') as f:
        f.write(filtered_code)
        f.close()
    
def add_new_hive_user(user, pass_file):
    code_str_list=get_str_from_txt(path_to_modules_dir+'hive_users.py').split('\n')

    for i in range(len(code_str_list)):
        if code_str_list[i]==f"""    elif input_username=='{user}':""":
            raise ValueError('Уже существует такой пользователь')
            
    for i in range(len(code_str_list)):
        if code_str_list[i]=='    else:':
            else_line_num=i

    new_part_lines=[
    f"""    elif input_username=='{user}':""",
    f"""        password=get_password('{pass_file}')""",
    ''
    ]

    new_code_str=(
                    code_str_list[0:else_line_num]+
                    new_part_lines+
                    code_str_list[else_line_num:(else_line_num+5)]
                )
    new_code_str='\n'.join(new_code_str)

    with open(path_to_modules_dir+'hive_users.py', 'w') as f:
        f.write(new_code_str)
        f.close()
        
def remove_hive_user(to_remove):
    code_str_list=get_str_from_txt(path_to_modules_dir+'hive_users.py').split('\n')
    for i in range(len(code_str_list)):
        if code_str_list[i]==f"""    elif input_username=='{to_remove}':""":
            elif_line_num=i

    if not('elif_line_num' in locals()):
        raise ValueError('Не существует такого пользователя')

    filtered_code='\n'.join([ele for i, ele in enumerate(code_str_list) if i not in [elif_line_num, elif_line_num+1, elif_line_num+2, elif_line_num+3]])
    with open(path_to_modules_dir+'hive_users.py', 'w') as f:
        f.write(filtered_code)
        f.close()    

def set_defaults_for_mail(default_list):
    if len(default_list)!=2:
        raise ValueError('Должно быть 2 элемента в таком порядке: email, логин')
        
    vars_list=[f'my_email="{default_list[0]}"', f'my_login="{default_list[1]}"']

    with open(path_to_modules_dir+'defaults.py', 'w') as f:
        f.write('\n'.join(vars_list))
        f.close()