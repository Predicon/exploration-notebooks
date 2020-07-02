#important libraries
import warnings
warnings.filterwarnings('ignore')
import pandas as pd
import numpy as np
import pymysql
import pymssql
import sys
from datetime import datetime
sys.path.insert(0, '/home/shared/utils')
sys.path.insert(0, '/home/vishal/refactoring_pipeline')

from db_utils import *
import kpi_count

#connections
iloans_conn = get_iloans_conn()
predicon_conn = get_predicon_db_conn()
bank_app_conn = get_bank_app_conn()


def streaming_kpi_values():
    try:
        bank_app = kpi_count.extract_bankapp(bank_app_conn)
        model = kpi_count.extract_model(predicon_conn)
        funded_loans = kpi_count.extract_funded_loans(iloans_conn)
        md_bnk = kpi_count.modify_bankapp_db(bank_app)
        md_mod = kpi_count.modify_model_db(model)
        md_fl = kpi_count.modify_fundedloans_db(funded_loans)
        kpi = kpi_count.get_kpi(md_bnk, md_mod, md_fl)
        return kpi
    except:
        print("some error in streaming function....pls recheck")
        return 0    

#dumping to csv
filename = '/home/shared/kpi/kpi_dump_'+datetime.now().strftime("%Y%m%d-%H%M%S")+'.csv'
try:
    streaming_kpi_values().to_csv(filename, index = False)
    print("kpi dump generated succesfully for {}".format(datetime.now().strftime("%Y%m%d-%H%M%S")))
except:
    print("error genrating kpi dump...check the to_csv function")


