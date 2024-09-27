from fun_helpers import *
def get_user_and_pass_file(input_username):
    
    if 1==0:
        raise ValueError('Так не бывает')

    elif input_username=='':
        user=''
        password=get_password('')

    else:
        raise ValueError('Неизвестный пользователь')
        
    return [user, password]
