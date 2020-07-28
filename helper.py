import datetime
import json
import pandas as pd



def stringify_account_ids(loan_id_list):
    """
    Convert account_id list into comma-separated string of account_ids
    :return: string containing comma-separated account_ids
    """
    return '(' + ', '.join([str(i) for i in loan_id_list]) + ')'


def parse_dates(json_date):
    '''
    Converts json formatted date to pandas datetime.
    
    Args:
    JSON date (JSON).
    
    Returns:
    Pandas datetime object.
    
    '''
    
    #return datetime.fromtimestamp(int(json_date)/1000.0).strftime('%Y-%m-%d')
    return datetime.datetime.utcfromtimestamp(int(json_date)/1000).date()


def fetch_checking_acct_txns(json_string):
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
            
            df_txn['posted_date'] = df_txn['postedDate'].map(lambda json_date: parse_dates(json_date))
            df_txn['category'] = df_txn['contexts'].map(lambda x: x[0]['categoryName'] if len(x) > 0 else np.nan)
            acct_numbers.append(accts['accountNumber'])
    
    if 'pending' in df_txn.columns:
        df_txn = df_txn[df_txn['pending'] == False]
    return df_txn