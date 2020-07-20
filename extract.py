import sys
sys.path.insert(0, '/home/shared/utils')
sys.path.insert(0, '/home/vishal/refactoring_pipeline')
from helper import parse_dates
import query as q
import pandas as pd
import json


def fetch_required_bank_reports(start, end):
    """Fetches all bank statements submitted by accepted leads

    Args:
        start (str): start date
        end (str): end date

    Returns:
        [pandas df]: all required bank statements between the given dates
    """
    query = f'''
                SELECT  
                        GCDL.LoanId,
                        GCD.BankTransactionId,
                        GCD.BankReportData,
                        LA.EmployerName,
                        GCD.TimeAdded
                FROM view_FCL_GetCreditData GCD
                LEFT JOIN view_FCL_GetCreditDataLoan GCDL ON GCD.BankTransactionId = GCDL.BankTransactionId
                LEFT JOIN view_FCL_LeadAccepted  LA ON LA.LoanId = GCDL.LoanId
                WHERE GCD.TimeAdded >= {start}
                AND GCD.TimeAdded <= {end}
                AND GCD.ReportStatus  = 'COMPLETE'
                AND LA.MerchantId in (15, 18)
            '''
    df = q.iloans(query)
    return df


def fetch_required_bank_app(start, end):
    """Fetches all required bankapp data

    Args:
        start (str): start date
        end (str): end date

    Returns:
        [pandas df]: all required data from bankapp
    """
    query = f'''
                SELECT
                    loan_id as LoanId,
                    json,
                    account_auto,
                    is_acc_matched,
                    is_acc_validated
                FROM loan
                WHERE campaign like '%Production%'
                AND is_acc_matched IS NOT NULL
                AND STR_TO_DATE(entered_date, '%m/%d/%Y') >= STR_TO_DATE({start}, '%Y-%m-%d')
                AND STR_TO_DATE(entered_date, '%m/%d/%Y') <= STR_TO_DATE({end}, '%Y-%m-%d')
             '''
    df = q.bankapp(query)
    return df