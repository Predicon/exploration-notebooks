import pymysql
import pymssql

#bank app
username_bank_app = 'bankreview'
password_bank_app = 'Freedom!23'
host_bank_app = '192.168.4.115'
port_bank_app = 3306
db_bank_app = 'bankreviewdb'

#iloans app
server = '192.168.4.117'
database = 'FreedomCashLenders'
username = 'FreedomCashLendersAll'
mssql_password = 'Freedom123$'


def get_bank_app_conn():
    conn = pymysql.connect(host=host_bank_app,
                                port=port_bank_app,
                                db=db_bank_app,
                                user=username_bank_app,
                                password=password_bank_app)
    return conn


def get_iloans_conn():
    conn = pymssql.connect(server, username, mssql_password, database, port=1433)
    return conn