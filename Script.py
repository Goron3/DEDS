import pandas as pd
import pyodbc
import sqlite3
from datetime import datetime

DB = {'servername': r'DESKTOP-I3QFF0F\SQLEXPRESS', 'database': 'DWH'}
now = datetime.now()
sqlite_connectie = sqlite3.connect('DWH')
export_conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + DB['servername'] + ';DATABASE=' + DB['database'] + ';Trusted_Connection=yes')
export_cursor = export_conn.cursor()
export_cursor


current_time= now.strftime("%d/%m/%Y %H:%M:%S")
current_time

staff_id = 12
satisfaction_data = {
    'SATISFACTION_SK' : [(pd.read_sql_query('SELECT MAX(SATISFACTION_SK) FROM SATISFACTION', export_conn) + 1).iloc[0, 0]],
    'SATISFACTION_YEAR_number': [2010],
    'SATISFACTION_STAFF_id': [staff_id],
    'SATISFACTION_TYPE_id': [1],
    'SATISFACTION_TYPE_description': ["goed"],
    'SALES_FSK': [(pd.read_sql_query("select MAX(SALES_SK) from SALES where SALES_STAFF_id = " + str(staff_id), export_conn)).iloc[0, 0]],
    'Changedate': [current_time]
}

dbobject = pd.DataFrame(satisfaction_data)

for index, row in dbobject.iterrows():
    query_satisfaction = f"INSERT INTO SATISFACTION VALUES ({row['SATISFACTION_SK']}, {row['SATISFACTION_YEAR_number']},'{row['SATISFACTION_STAFF_id']}', '{row['SATISFACTION_TYPE_id']}', '{row['SATISFACTION_TYPE_description']}', '{row['SALES_FSK']}', '{row['Changedate']}')"
export_cursor.execute(query_satisfaction)

export_conn.commit()

sales_staff_id = 12
sales_data = { 
    'SALES_SK':[changeSK([("tabel1", "SALES"),("tabel2", "SATISFACTION")], "SALES", sales_staff_id)],
    'SALES_STAFF_id':[sales_staff_id],
    'SALES_FIRST_name':["Hans"],
    'SALES_LAST_name':["Koning"],
    'SALES_EXTENSION_number':[25],
    'SALES_EMAIL_name':["@mail.com"],
    'SALES_MANAGER_id':[11],
    'SALES_BRANCH_id':[3],
    'SALES_ADDRESS1_name':["straat3"],
    'SALES_ADDRESS2_name':["straat4"],
    'SALES_POSITION_name':["boss"],
    'SALES_WORK_PHONE_number':[12345],
    'SALES_FAX_number':[12],
    'SALES_HIRED_date' : ["5/12/2024"],
    'SALES_COUNTRY_id' : [3],
    'SALES_REGION_name' : ["W"],
    'SALES_CITY_name' : ["Utrecht"],
    'SALES_POSTAL_ZONE_name' : ["2325KL"],
    'SALES_FSK' : [(pd.read_sql_query("select MAX(SALES_SK) from SALES where SALES_STAFF_id = " + str(13), export_conn)).iloc[0, 0]],
    'Changedate': [current_time]
}

dbobject2 = pd.DataFrame(sales_data)

for index, row in dbobject2.iterrows():
    query_sales = f"INSERT INTO SALES VALUES ({row['SALES_SK']},'{row['SALES_STAFF_id']}', '{row['SALES_FIRST_name']}', '{row['SALES_LAST_name']}', '{row['SALES_EXTENSION_number']}', '{row['SALES_EMAIL_name']}', '{row['SALES_MANAGER_id']}', '{row['SALES_BRANCH_id']}', '{row['SALES_ADDRESS1_name']}', '{row['SALES_ADDRESS2_name']}', '{row['SALES_POSITION_name']}', '{row['SALES_WORK_PHONE_number']}', '{row['SALES_FAX_number']}', '{row['SALES_HIRED_date']}', '{row['SALES_COUNTRY_id']}', '{row['SALES_REGION_name']}', '{row['SALES_CITY_name']}', '{row['SALES_POSTAL_ZONE_name']}', '{row['SALES_FSK']}', '{row['Changedate']}')"
export_cursor.execute(query_sales)
export_conn.commit()
export_conn.close()


def changeSK(tables, table, id):
    SK = (pd.read_sql_query(f'SELECT MAX({table}_SK) FROM {table}', export_conn) + 1).iloc[0, 0]
    for tbl, tbl_name in tables:
        query_change = f"UPDATE {tbl_name} SET {table}_FSK = {SK} WHERE {tbl_name}_STAFF_id = {id}"
        export_cursor.execute(query_change)
    return SK

