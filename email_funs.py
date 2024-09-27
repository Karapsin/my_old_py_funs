import mimetypes
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.message import EmailMessage
from email.mime.base import MIMEBase
from email.utils import make_msgid
from email import encoders
import smtplib
import ssl
from GP_functions import *
from pretty_html_table_from_git import *
from defaults import * 

def send_email(receiver_email, 
               msg_title, 
               msg_text,
               shown_from_mail=my_email, #сюда можно вписать, например, 'FX_PM_Analytics@omega.sbrf.ru'
               files_list=None, 
               df_to_html_list=None,
               my_email=my_email, 
               my_login=my_login, 
               html_table_style='green_dark',
               debug=False,
               width_dict_list=[],
               default_table=False,
               img_in_body_list=[],
               even_bg_color='white',
               table_thousand_sep=',',
               text_align='left'
    ):
    
    if isinstance(receiver_email, list):
        for i in range(len(receiver_email)):
            receiver_email[i]='<'+receiver_email[i]+'>'
    else:
        receiver_email='<'+receiver_email+'>'
    
    
    msg_text=msg_text.replace('\n', '<br>')
    new_txt=[]
    for line in msg_text.splitlines():
        new_txt.append(re.sub(r"^\s+" , "&nbsp;", line))
    msg_text='\n'.join(new_txt)
    
    msg_text=f"""\
<html>
    <body>
    {msg_text}
    </body>
</html>
"""
    if debug:
        time_print(msg_text)
    
    message = MIMEMultipart('mixed')
    message['Subject'] = msg_title
    message['From'] = ('<'+shown_from_mail+'>')
    message['To'] = (', '.join(receiver_email) if isinstance(receiver_email, list) else receiver_email)
    message.preamble = 'This is a multi-part message in MIME format.'
    
    time_print('формирование тела письма')
    msgRoot = MIMEMultipart('alternative')
    
    if not(df_to_html_list is None):
        time_print('прикрепление html табличек в тело письма')
        for i in range(len(df_to_html_list)):
            
            names=list(df_to_html_list[i].columns)
            for name in names:
                if df_to_html_list[i].dtypes[name]=='float64' or df_to_html_list[i].dtypes[name]=='int64':
                    df_to_html_list[i][name] = df_to_html_list[i][name].apply(lambda x : ("{:,}").format(x).replace(',', table_thousand_sep))
              
            width_dict=width_dict_list[i] if len(width_dict_list)>0 else []

            if default_table:
                exec('table_'+str(i+1)+"=df_to_html_list[i].to_html(index=False)")
            else:
                exec('table_'+str(i+1)+f"""=build_table(df=df_to_html_list[i], 
                                                        color='{html_table_style}', 
                                                        width_dict={width_dict}, 
                                                        even_bg_color='{even_bg_color}', 
                                                        text_align='{text_align}'
                                            )"""
                )

    if len(img_in_body_list)>0:
        for i in range(len(img_in_body_list)):
            fp = open(img_in_body_list[i], 'rb')
            msgImage = MIMEImage(fp.read(), 'png')
            
            gen_id='image_'+str(i+1)
            
            fp.close()
            msgImage.add_header('Content-ID', f'<{gen_id}>')
            message.attach(msgImage)
            exec(f"""{gen_id}='<img src="cid:{gen_id}">'""")

    if debug:
        html_body=eval('f"""'+msg_text+'"""')
        time_print(html_body)
                
    msgRoot.attach(MIMEText(eval('f"""'+msg_text+'"""'), 'html'))
    message.attach(msgRoot)
    time_print('успешно')
    
    if not(files_list is None):
        time_print('прикрепление файлов')
        files=files_list

        for file in files:
            time_print('прикрепляем ' +file)
            ctype, encoding = mimetypes.guess_type(file)
            if ctype is None or encoding is not None:
                ctype = "application/octet-stream"

            maintype, subtype = ctype.split("/", 1)

            if 1==0:
                'так не бывает'
            #if maintype == "text":
                
                #fp = open(file)
                #attachment = MIMEText(fp.read(), _subtype=subtype)
                #fp.close()
                
            elif maintype == "image":
                fp = open(file, "rb")
                attachment = MIMEImage(fp.read(), _subtype=subtype)
                fp.close()
                
            elif maintype == "audio":
                fp = open(file, "rb")
                attachment = MIMEAudio(fp.read(), _subtype=subtype)
                fp.close()
                
            else:
                fp = open(file, "rb")
                attachment = MIMEBase(maintype, subtype)
                attachment.set_payload(fp.read())
                fp.close()
                encoders.encode_base64(attachment)

            attachment.add_header("Content-Disposition", "attachment", filename=file)
            message.attach(attachment)
            
        time_print('успешно')
            
    if debug:
        time_print(str(message))
        
    time_print('отправка письма')
    context = ssl._create_unverified_context()
    with smtplib.SMTP('smtp.omega.sbrf.ru', 2525) as server:
        server.ehlo()
        server.starttls(context=context)
        server.ehlo()
        server.login(my_login, get_user_and_pass_file(my_login)[1])
        server.sendmail(('<'+my_email+'>'), receiver_email,  message.as_string())
        server.quit()
        time_print('успешно')
                  
def send_email_with_GP_load(receiver_email, #лист с адресами получателей
                            msg_title, #титул письма
                            msg_text, #текст письма
                            files_list, #лист с названиями файлов, которые грузим из GP
                            con_params,  #параметры соединения, пример: ['capgp', '11111111']
                            sql_list, #sql запросы для files_list
                            file_type=None, #лист с указанием csv или html_table_in_body для files_list 
                            other_files=[], #другие файлы(загруженные до запуска функции, e.g.: photo.jpg)
                            my_email=my_email,  #почта отправителя
                            shown_from_mail=my_email,
                            my_login=my_login, #логин отправителя
                            html_table_style='green_dark',
                            debug=False,
                            order_list=[],
                            width_dict_list=[],
                            default_table=False,
                            img_in_body_list=[]
    ):
    
    if len(files_list)!=len(sql_list):
        raise ValueError('Листы с названиями файлов и sql запросами должны совпадать по длине')
    
    if file_type is None:
        file_type=['csv']*len(files_list)
    

    csv_list=[]
    df_to_html_list=[]
    for i in range(len(sql_list)):
        if file_type[i]=='csv':
            read_from_GP(
                          [
                            'считывание '+files_list[i],
                             con_params,
                             sql_list[i]
                          ],
                          order_list=order_list[i] if len(order_list)>0 else order_list,
                          debug=debug
            ).to_csv(os.getcwd()+'/'+files_list[i]+'.csv', 
                     decimal=',',  
                     sep=';', 
                     encoding='utf-8-sig', 
                     index=False
             )
            csv_list.append(files_list[i]+'.csv')
                
        elif file_type[i]=='html_table_in_body':
            
            (
               df_to_html_list
                .append(read_from_GP(
                                      [
                                        'считывание '+files_list[i],
                                         con_params,
                                         sql_list[i]
                                      ],
                                      order_list=order_list[i] if len(order_list)>0 else order_list,
                                      debug=debug
                        )
                )
            )
    
    send_email(receiver_email=receiver_email, 
               msg_title=msg_title, 
               msg_text=msg_text,
               files_list=list(set(csv_list+other_files)),
               df_to_html_list=df_to_html_list,
               my_email=my_email, 
               shown_from_mail=shown_from_mail,
               my_login=my_login, 
               html_table_style=html_table_style,
               debug=debug,
               width_dict_list=width_dict_list,
               default_table=default_table,
               img_in_body_list=img_in_body_list
    )
    
    for csv in csv_list:
        os.remove(os.getcwd()+'/'+csv)

del my_email
del my_login