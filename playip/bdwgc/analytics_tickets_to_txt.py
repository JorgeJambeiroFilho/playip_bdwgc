import pyodbc
import datetime

from playip.bdwgc.sql_import_analytics_tickets import sql_analytics_tickets

server = '170.238.84.12'
database = 'WGC'
username = 'wgcplayip'
password = 'teste01*'
cnxn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + database + ';UID=' + username + ';PWD=' + password)
cursor = cnxn.cursor()
cursor.execute(sql_analytics_tickets)
columns = [column[0] for column in cursor.description]
print(columns)
p = 0
row = cursor.fetchone()
while row:
    prow = []
    for v in row:
        if isinstance(v, datetime.datetime) or isinstance(v, datetime.date) :
            v = str(v.year) + "-" + str(v.month) + "-" + str(v.day)
        elif isinstance(v, str):
            v = v.replace(",", " ")
        prow.append(v)
    print(prow)
    row = cursor.fetchone()
    p += 1

#print("count ", p)