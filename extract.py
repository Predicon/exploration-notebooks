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
    query_bank_app_reviewed = f'''SELECT
                                        l.loan_id as LoanId,
                                        l.final_decision,
                                        l.entered_date,
                                        fd.underwriting_final_decision,
                                        fd.len_to_many_open_loans_101,
                                        fd.len_return_found_102,
                                        fd.len_missing_payments_103,
                                        fd.bk_verif_bal_on_payday_130,
                                        fd.bk_verif_high_negative_curr_bal_131,
                                        fd.bk_verif_high_pct_negative_bal_132,
                                        fd.bk_verif_bad_acc_type_133,
                                        fd.bk_verif_savings_acc_134,
                                        fd.bk_verif_business_acc_135,
                                        fd.bk_verif_pre_debit_card_acc_136,
                                        fd.bk_verif_stop_payment_or_revoked_137
                                  FROM loan l
                                  LEFT JOIN final_decision fd on l.loan_id = fd.loan_id
                                  WHERE STR_TO_DATE(l.entered_date , '%m/%d/%Y') >= STR_TO_DATE({start},'%Y-%m-%d')
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
    query = f'''
                SELECT 
                    LoanId,
                    LoanStatus,
                    TimeAdded,
                    (CASE when LoanStatus = 'Lender Approved' then 1 else 0 END) as LenderApproved
                FROM view_FCL_Loan_History
                WHERE TimeAdded >= {start}
               '''
    approved_loans = q.iloans(query)
    return approved_loans

def extract_performance(start, end = str(date.today())):


    query = f'''
             SELECT LoanId,
                    IsFirstDefault
            FROM view_FCL_Loan
            WHERE LeadTimeAdded >= {start}
            AND IsFirstDefault IS not null
            '''
    funded_loans = q.iloans(query)
    return funded_loans

    
    

