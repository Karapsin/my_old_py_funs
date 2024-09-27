def get_dbname_and_host(input_dbname):
    if 1==0:
        raise ValueError('Так не бывает')

    elif input_dbname=='':
        host_name=''
        db_name=''

    else:
        raise ValueError('Неизвестная база данных')
        
    return [db_name, host_name]
        