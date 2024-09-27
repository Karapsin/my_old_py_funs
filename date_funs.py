import datetime
import os
import sys
import pandas as pd

#заменить на datetime если string или date
def to_dttm(input_dt):
    
    if isinstance(input_dt, str):
        res=datetime.datetime.strptime(input_dt, '%Y-%m-%d')
        
    elif isinstance(input_dt, datetime.date):
        res=datetime.datetime(input_dt.year, input_dt.month, input_dt.day)
    else:
        res=input_dt
        
    return res

#заменить на string если datetime или date
def to_str(input_dt):
    
    if isinstance(input_dt, datetime.date) or isinstance(input_dt, datetime.datetime):
        res=str(input_dt)[0:10]
    else:
        res=input_dt
        
    return res

#сегодняшняя дата
def get_current_date(output='text'):
    return str(datetime.datetime.now())[0:10] if output=='text' else datetime.datetime.now()

#сегодняшняя дата с временем
def get_current_datetime(output='text'):
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M") if output=='text' else datetime.datetime.now()

#текущее время
def get_current_time(output='text'):
    return datetime.datetime.now().strftime("%H:%M") if output=='text' else datetime.datetime.now()

#номер текущего дня недели
def day_of_week_num(input_dt):
    return to_dttm(input_dt).isoweekday()

#первый день месяца
def first_day(input_dt, output='text'):
    
    input_dt=to_dttm(input_dt).replace(day=1)
            
    return (input_dt if output=='datetime' else to_str(input_dt))
    
#последний день месяца
def last_day(input_dt, output='text'):
    
    next_month = to_dttm(input_dt).replace(day=28) + datetime.timedelta(days=4)
    res=(next_month - datetime.timedelta(days=next_month.day))
    
    return (res if output=='datetime' else to_str(res))

#добавить n дней
def add_n_days(input_dt, n, output='text'):
    
    res=to_dttm(input_dt)+datetime.timedelta(days=n)
    
    return (res if output=='datetime' else to_str(res))

#первое число месяца +n относительно поданого
def go_n_months_ahead(input_dt, n, output='text'):
    
    input_dt=to_dttm(input_dt)
    
    abs_month_num=(input_dt.year-1)*12+input_dt.month
    year_new=((abs_month_num+n)//12)+1
    month_new=(abs_month_num+n)%12
    
    res=input_dt.replace(year=((year_new-1) if month_new==0 else year_new), month=(12 if month_new==0 else month_new) , day=1) 
    return (res if output=='datetime' else to_str(res))  

#генерит лист дат 
def generate_date_list(start_date, end_date, date_range, output='text'):
    
    if date_range=='month':
         
        start_date=to_dttm(start_date)
        end_date=to_dttm(end_date)
        
        res=[start_date]
        while last_day(go_n_months_ahead(res[len(res)-1], 1), 'datetime')<=end_date:
            res.append(last_day(go_n_months_ahead(res[len(res)-1], 1)))
            
        if output=='text':
            res=list(map(to_str, res))
        
    
    elif date_range=='day':
        
        start_date=to_str(start_date)
        end_date=to_str(end_date)
        
        res=list(map(lambda x: str(x)[0:10] if output=='text' else x,
                     pd.date_range(start_date, 
                                   end_date
                    )
            ))
    
    else:
        raise ValueError('неизвестный date_range')
        
    return res