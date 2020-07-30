#from helper import parse_dates
import pandas as pd
import json
#from functools import reduce
#from operator import add
from fuzzywuzzy import fuzz, process
from sklearn.metrics import confusion_matrix as cm



'''def fetch_checking_acct_txns(json_string, loanid, employer_name):
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
            df_txn['EmployerName'] = employer_name
            
            df_txn['posted_date'] = df_txn['postedDate'].map(lambda json_date: parse_dates(json_date))
            df_txn['category'] = df_txn['contexts'].map(lambda x: x[0]['categoryName'] if len(x) > 0 else np.nan)
            acct_numbers.append(accts['accountNumber'])
    
    if 'pending' in df_txn.columns:
        df_txn = df_txn[df_txn['pending'] == False]
    return df_txn[['LoanId', 'EmployerName']]


def get_ground_truth_income(json_string, loanid):
    """Returns all the actual incomes as identified by the bankapp agents

    Args:
        json_string (json): information entered by bankapp agents
        loanid (str): loan id of each applicant

    Returns:
        [pandas df]: all actual incomes of the particular loan id entered by bankapp agents
    """
    try:
        income = []
        for inc in range(int(json.loads(json_string)['incomeReview']['data']['incomeSources'])):
            source = json.loads(json_string)['incomeReview']['data']['sources'][inc]['sourceName']
            txns = json.loads(json_string)['incomeReview']['data']['sources'][inc]['records']
            for rec in txns:
                rec.update({'source' : source})
            income.append(txns)
            income = reduce(add, income)
        df_income = pd.DataFrame(income)
        df_income['LoanId'] = loanid
        df_income['date'] = pd.to_datetime(df_income['date'])
        return df_income
    except:
        pass

def check_if_employer_name_isin_txns(json_string, loan_id, employer_name):
    """Returns all the credit txns which match with the employer name

    Args:
        json_string (json): json containing bank report
        loan_id (str): unique id of each applicant
        employer_name (str): employer name provided by the applicant

    Returns:
        pandas df: dataframe consisting all credit txns having very similar memo as that of the employer name
    """
    try:
        txns = fetch_checking_acct_txns(json_string, loan_id, employer_name)
        txns = txns[txns['amount'] > 0]
        memos = list(txns['memo'].values)
        res = process.extractBests(employer_name.lower(), memos)
        top_score = res[0][1]
        txn_memo = list(set([x[0] for x in res if top_score >= 50]))
        return txns[txns['memo'].isin(txn_memo)].reset_index(drop = True)[['LoanId', 'account_number', 'posted_date',
                                                                           'amount', 'memo', 'category', 'EmployerName']]
    except:
        pass'''

def fetch_txns_above_50_from_bankapp(json_string, loanid, employer_name, account):
    """Get all the txns returned by the api to bankapp

    Args:
        json_string (json): json containing bankapp results
        loanid (str): Loan id of applicant
        employer_name (str): company in which the applicant works
        account (str): account to be screen for

    Returns:
        [pandas df]: all the txns above some threshold from bankapp
    """
    try:
        cred_txns = json.loads(json_string)['incomeReview']['data']['incomeTransactions']
        df_credit_txns = pd.DataFrame(cred_txns)
        df_credit_txns['account_number'] = account
        df_credit_txns['LoanId'] = loanid
        df_credit_txns['EmployerName'] = employer_name
        df_credit_txns.rename(columns = {'postedDate' : 'posted_date'}, inplace = True)
        df_credit_txns['posted_date'] = pd.to_datetime(df_credit_txns['posted_date'])
        return df_credit_txns
    except:
        pass


def is_hit_or_miss(row):
    """custom function to check if a string is present in a text using fuzzy match approach

    Args:
        row (pandas series): [description]

    Returns:
        [bool]: whether the match was good or bad
    """
    score = fuzz.WRatio(row['EmployerName'].lower(), row['memo'])
    if score > 50:
        return True
    else:
        return False