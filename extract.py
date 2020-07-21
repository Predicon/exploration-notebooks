import pandas as pd
import sys
sys.path.insert(0, '/home/shared/utils')
import query as q
from datetime import date


def extract_bankapp(start, end = '"'+str(date.today())+'"'):
    """Extracts required features from bankapp 

    Args:
        bank_app_conn (pymysql connection): connection object

    Returns:
        pandas df: dataframe containing required features fetched from bankapp server
    """
    query_bank_app_reviewed = f'''
                            select 
                                    l.loan_id As LoanId, 
                                    l.final_decision,
                                    l.entered_date,
                                    fd.underwriting_final_decision
                            from loan l
                            left join final_decision fd on l.loan_id  = fd.loan_id
                            where STR_TO_DATE(l.entered_date , '%m/%d/%Y') >= STR_TO_DATE({start},'%Y-%m-%d')
                          '''
    bank_app = q.bankapp(query_bank_app_reviewed)
    return bank_app

def extract_model():
    """Extracts required features from predicon server

    Args:
        predicon_conn(pymysql connection): connection object

    Returns:
        pandas df: dataframe containing required features fetched from predicon server
    """
    model = q.predicon('''SELECT 
                                LoanId,
                                TimeAdded,
                                Score
                             FROM FreedomScores 
                             WHERE Version='v1.3.0' 
                             GROUP BY LoanId''', 'staging')
    return model

def extract_loan_history(start, end = str(date.today())):
    """Extracts required features from iloans

    Args:
        iloans_conn (pymssql connection): connection object

    Returns:
        pandas df: dataframe containing required featurs fetched from iloans server
    """
    query_funded = f'''
                SELECT 
                    LoanId,
                    LoanStatus,
                    TimeAdded,
                    (CASE when LoanStatus = 'Lender Approved' then 1 else 0 END) as LenderApproved
                FROM view_FCL_Loan_History
                WHERE TimeAdded >= {start}
               '''
    funded_loans = q.iloans(query_funded)
    return funded_loans