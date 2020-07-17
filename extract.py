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
        end ([type]): end date

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


def preprocess_bank_statements(df):
    """Modifies extracted bank statement data
    Args:
        df (pandas df): Dataframe consisting all the required columns from iloans database
    Returns:
        pandas df: Modified dataframe
    """
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    df['TimeAdded'] = pd.to_datetime(df['TimeAdded'].map(lambda x : x.date()))
    return df

def preprocess_bank_app_accounts(df):
    """Modifies bank app data
    Args:
        df (pandas df): Dataframe consisting all the required columns from bankapp database
    Returns:
        pandas df: Modified dataframe
    """
    df['is_matched'] = df['is_acc_matched'].map(lambda x : x[:-1].split('x0')[1])
    df = df[df['is_matched'] == "1"]
    return df[['LoanId', 'json', 'account_auto']]

def fetch_checking_acct_txns(json_string, loanid):
    """
    Parse all checking account transactions in the bank report
    
    Args:
    json_string(str): json containing bank report
    
    Returns:
    df_txn (pandas df): contains transactions of all checking accounts in the report
    
    """
    j = json.loads(json_string)
    df_txn = pd.DataFrame()
    
    acct_numbers = []
    for accts in j['accounts']:
        
        if ('transactions' in accts.keys()) and (len(accts['transactions']) > 0) and (accts['accountNumber'] not in acct_numbers) and (accts['accountType'].strip().lower() == 'checking'):
            
            df_txn_temp = pd.DataFrame(accts['transactions'])
            df_txn_temp['account_number'] = accts['accountNumber']
            df_txn = df_txn.append(df_txn_temp, ignore_index=True)
            df_txn['LoanId'] = loanid
            
            df_txn['posted_date'] = df_txn['postedDate'].map(lambda json_date: parse_dates(json_date))
            df_txn['category'] = df_txn['contexts'].map(lambda x: x[0]['categoryName'] if len(x) > 0 else np.nan)
            acct_numbers.append(accts['accountNumber'])
    
    if 'pending' in df_txn.columns:
        df_txn = df_txn[df_txn['pending'] == False]
    return df_txn


def get_ground_truth_income(json_string, loanid):
    """Returns all the actual incomes

    Args:
        json_string (json): information entered by bankapp agents
        loanid ([type]): loan id of each applicant

    Returns:
        [pandas df]: all actual incomes of the particular loan id entered by bankapp agents
    """
    try:
        source = json.loads(json_string)['incomeReview']['data']['sources'][0]['sourceName']
        df_income = pd.DataFrame(json.loads(json_string)['incomeReview']['data']['sources'][0]['records']).drop('balance', axis = 1)
        df_income['source'] = source
        df_income['LoanId'] = loanid
        df_income['date'] = pd.to_datetime(df_income['date'])
        return df_income
    except:
        pass