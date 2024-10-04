import os
import psycopg2
import pandas as pd 
from sqlalchemy import create_engine
from datetime import datetime
from config.databases import database_dw, database_aws
from openpyxl import Workbook

wb = Workbook()

DATABASE_CONNECTION = f'mssql://{database_dw["user"]}:{database_dw["password"]}@{database_dw["server"]}/{database_dw["database"]}?ApplicationIntent=ReadOnly&driver={database_dw["driver"]}'


conn_dw = create_engine(DATABASE_CONNECTION)

conn = psycopg2.connect(host=database_aws["endpoint"], port=database_aws["port"], database=database_aws["dbname"], user=database_aws["user"], password=database_aws["passwd"], sslrootcert="SSLCERTIFICATE")

def pinkaws(script,colunas):
    cur = conn.cursor()
    cur.execute(script)
    query_results = cur.fetchall()
    result = pd.DataFrame(query_results,columns=colunas)
    return result

def extract_users():
    try:
        print(str(datetime.now()) + ":\t" +"Inicio do processo de Ingestão na PinkAWS.Users")

        colunas = ['id','workspace','store','username','last_login','active','email_address','updated_at']
        script = ("""select
                         id 
                        ,workspace
                        ,store
                        ,username
                        ,last_login
                        ,active
                        ,email_address
                        ,updated_at
                        from users 
                        where updated_at = (select max(updated_at) from users)
                  """)
        result = pinkaws(script,colunas)

        with conn_dw.begin() as con_mss:
            df_usermssql = pd.read_sql("Select * From [PinkAWS].[Users]", con=con_mss)
            user_carga = result.loc[~result['id'].isin(df_usermssql['id'])].reset_index(drop=True)
            user_carga.to_sql("Users",con =conn_dw, schema="PinkAWS",if_exists="append", index=False)

        print(str(datetime.now()) + ":\t" +"Fim do processo de Ingestão na PinkAWS.Users")

    except Exception as e:
        print("Database connection failed due to {}".format(e))

def extract_messages():
    try:
        print(str(datetime.now()) + ":\t" +"Inicio do processo de Ingestão na PinkAWS.Messages")

        colunas = ['id','t','rid','workspace','attendance_id','department_name','duration','ts','username','store','remote_jid','updated_at']
        script = ("""select
                         id
                        ,t
                        ,rid
                        ,workspace
                        ,attendance_id
                        ,department_name
                        ,duration
                        ,ts
                        ,username
                        ,store
                        ,remote_jid
                        ,updated_at
                        from messages 
                        where updated_at = (select max(updated_at) from messages)
                  """)
        result = pinkaws(script,colunas)

        with conn_dw.begin() as con_mss:
            df_messagesmssql = pd.read_sql("Select * From [PinkAWS].[messages]", con=con_mss)
            messages_carga = result.loc[~result['id'].isin(df_messagesmssql['id'])].reset_index(drop=True)
            #print(messages_carga)
            messages_carga.to_sql("messages",con =conn_dw, schema="PinkAWS",if_exists="append", index=False)
            
            #writer = pd.ExcelWriter('I:/NAE/Messages_AWS.xlsx', engine='xlsxwriter', mode='w')
            #messages_carga.to_excel(writer, sheet_name='Messages_AWS',index=False)

            #writer.close()

        print(str(datetime.now()) + ":\t" +"Fim do processo de Ingestão na PinkAWS.Messages")

    except Exception as e:
        print("Database connection failed due to {}".format(e))
    
def extract_rooms():
    try:
        print(str(datetime.now()) + ":\t" +"Inicio do processo de Ingestão na PinkAWS.Rooms")

        colunas = ['id','remote_jid','name','store','t','requested_terms','workspace','updated_at']
        script = ("""select
                         id
                        ,remote_jid
                        ,name
                        ,store
                        ,t
                        ,requested_terms
                        ,workspace
                        ,updated_at
                        from rooms
                        where updated_at = (select max(updated_at) from rooms)
                   """)
        result = pinkaws(script,colunas)

        with conn_dw.begin() as con_mss:
            df_roomsmssql = pd.read_sql("Select * From [PinkAWS].[rooms]", con=con_mss)
            rooms_carga = result.loc[~result['id'].isin(df_roomsmssql['id'])].reset_index(drop=True)
            rooms_carga.to_sql("rooms",con =conn_dw, schema="PinkAWS",if_exists="append", index=False)
        print(str(datetime.now()) + ":\t" +"Fim do processo de Ingestão na PinkAWS.Rooms")

    except Exception as e:
        print("Database connection failed due to {}".format(e))

def transform_messages():

    try:
        print(str(datetime.now()) + ":\t" +"Inicio do processo de Ingestão na PinkAWS.Fato_Messages")

        script = ("""Select
                    DateMessage
                ,	Rid
                ,	Workspace
                ,	DateHourInitial = MIN(ts)
                ,	DateHourEnd =  MAX(ts)
                ,	DurationTotal = DATEDIFF(second, MIN(ts), MAX(ts))
                ,	DurationService = isnull(SUM(durationTotal),0)
                ,	[Updated_at]
                From (
                        Select
                            [DateMessage] = Cast(convert(char(10),m.ts, 112) as datetime)
                        ,	m.[rid]
                        ,	m.t
                        ,	m.ts
                        ,	[DurationTotal] = isnull(m.duration,0)
                        ,	m.[updated_at]
                        ,	m.username
                        ,	workspace = isnull(m.workspace,'')
                        From [PinkAWS].[Messages] m
                        Left join [PinkAWS].[Fato_Messages] fMessages on m.updated_at = fMessages.Updated_at
                        Where 1=1
                        And fMessages.DateMessage is null
                ) x
                Group by DateMessage, rid, workspace,[Updated_at]
                Order by DateHourInitial
                """)
        
        with conn_dw.begin() as con_mss:
            df_fato_message = pd.read_sql(script, con=con_mss)
            df_fato_message.to_sql("Fato_Messages",con =conn_dw, schema="PinkAWS",if_exists="append", index=False)
        print(str(datetime.now()) + ":\t" +"Fim do processo de Ingestão na PinkAWS.Fato_Messages")

    except Exception as e:
        print("Database connection failed due to {}".format(e))
    
if __name__ == "__main__":
    extract_users()
    extract_messages()
    extract_rooms()
    transform_messages()

conn.close()
