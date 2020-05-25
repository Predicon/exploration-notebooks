import sys
import os
import pandas as pd

sys.path.insert(0,os.getcwd())
from db_utils import *


def fetch_bank_app_loans(start,end):
    """Fetches bank app data between given dates.
    
    Args:
    start (str): Start date
    end: (str): End date
    
    Returns:
    df (dataframe): contains required columns with loan id and decisions
    """
    query =   f'''select l.loan_id, 
                  l.final_decision,
                  l.reasons_for_decision,
                  l.entered_date,
                  lv.dti_percentage,
                  lv.in1_is_direct_deposite,
                  lv.pay_day_test_result_amount,
                  lv.is_pds_history_found

                  from loan l
                  LEFT JOIN loan_view_table lv 
                  on l.loan_id = lv.loan_id
                  where campaign like '%Production%'
                  and STR_TO_DATE(entered_date ,'%m/%d/%Y') >= STR_TO_DATE({start},'%Y-%m-%d')
                  and STR_TO_DATE(entered_date ,'%m/%d/%Y') < STR_TO_DATE({end},'%Y-%m-%d')
               '''
    bank_app_conn = get_bank_app_conn()
    
    df = pd.read_sql_query(query, con = bank_app_conn)
    return df



def fetch_funded_mature_loans(start,end):
    """Fetch funded loans between given dates.
    
    Args:
    start (str): Start date
    end: (str): End date
    
    Returns:
    df (dataframe): contains required columns from iloans
    """
    
    query = f'''select LN.LoanId,
                       LC.LoanCount,
                       LN.OriginationDate,
                       GC.BankReportData,
                       GC.TimeAdded as ReportTimeAdded,
                       LN.Campaign,
                       LN.MonthlyGrossIncome,
                       LN.DateOfBirth,
                       LN.IsFirstDefault
                       
                from view_FCL_Loan LN
                LEFT JOIN view_FCL_CustomerLoanCount LC ON LC.CustomerId = LN.CustomerId
                LEFT JOIN view_FCL_GetCreditDataLoan GCD ON LN.LoanId = GCD.LoanId
                LEFT JOIN view_FCL_GetCreditData GC ON GC.BankTransactionId = GCD.BankTransactionId
                
                
                where LN.OriginationDate >= {start}
                and LN.OriginationDate <= {end} 
                and LN.IsFirstDefault IS NOT NULL
                and LN.MerchantId IN (15, 18)
                and GC.ReportStatus = 'COMPLETE' '''
    iloans_conn = get_iloans_conn()
    df = pd.read_sql_query(query,con = iloans_conn)
    df = df.drop_duplicates('LoanId')
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    return df



def get_esign_time_diff(start,end):
    """Get Esign time difference

    Args:
    start (str): Start date
    end: (str): End date

    Returns:
    df (pandas df): DataFrame with LoanId and esign time difference(sec)
    """
    query = f'''SELECT LN.LoanId,
                       ESIG.AccessCount,
                       ESIG.EsigTimeSignedDiff_In_SEC
                FROM view_FCL_Loan LN
                LEFT JOIN view_FCL_EsignatureCustomerData ESIG ON LN.LoanId = ESIG.LoanId
                WHERE LN.OriginationDate >= {start} 
                and LN.OriginationDate <= {end}
                and LN.IsFirstDefault IS NOT NULL
                and LN.MerchantId IN (15, 18) 
            '''
    
    iloans_conn = get_iloans_conn()
    df = pd.read_sql_query(query,con=iloans_conn)
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    return df



def get_examples(start_date,end_date):
    """Extract evaluation dataset with columns from all DBs and sources required for preprocessing.
    
    Args:
    start_date (str): Start date
    end_date: (str): End date
    
    Returns:
    df (dataframe): contains all columns from iloans and bank app
    """
    start_date = "'" + start_date + "'"
    end_date = "'" + end_date + "'"
    df_funded_mature_loans = fetch_funded_mature_loans(start_date,end_date)
    df_esign = get_esign_time_diff(start_date,end_date)
    df_funded_mature_loans = pd.merge(df_funded_mature_loans,df_esign,how = 'left',on = 'LoanId')
    df_BV_loans = fetch_bank_app_loans(start_date,end_date)
    df = pd.merge(df_funded_mature_loans,df_BV_loans,how = 'inner',left_on = 'LoanId',right_on = 'loan_id')
    df = df.drop('loan_id',axis = 1)
    return df