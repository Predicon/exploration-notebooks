from helper import fetch_checking_acct_txns
import pandas as pd
import joblib
import re
import json

def preprocess_iloans(df):
    df['Campaign'] = df['Campaign'].str.lower()
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    return df

def preprocess_bank_reports(df):
    df = df.drop_duplicates('LoanId')
    return df


def create_lender_vars(loanid,report_string,time_added,pr_acct):
    """Compute lender variables from primary account transactions

    Args:
    loanid (float)
    report_string (str)
    time_added (pd datetime)
    pr_acct (str)

    Returns:
    lender_vars(pandas dataframe):
    """

    lender_vars = dict()   
    lender_vars['LoanId'] = loanid
    lender_vars['LenderAmountDeb'] = 0.0
    lender_vars['LenderCountCred'] = 0.0
    lender_vars['LenderAmountCred30'] = 0.0
    lender_vars['LenderCountDeb'] = 0.0
    lender_vars['LenderAmountDeb30'] = 0.0
    lender_vars['LenderCountCred30'] = 0.0
    lender_vars['LenderCountDeb30'] = 0.0
    lender_vars['LenderAmountCred'] = 0.0
    lender_vars['UniqLenderCount'] = 0.0

    #load lending company list
    lend_cos=joblib.load('./lend_cos.pkl')

    #get primary checking account transactions
    df_checking_txns = fetch_checking_acct_txns(report_string) 
    df_pr_acct_txns = df_checking_txns[df_checking_txns['account_number']==pr_acct]
    
    
    #prepare lender transactions dataframe
    df_lender_txns=df_pr_acct_txns.loc[df_pr_acct_txns['memo'].str.contains('|'.join(lend_cos),case=False,na=False)]
    
    #check for empty transactions
    if df_lender_txns.empty is False:
        df_lender_txns['lenderName'] = df_lender_txns['memo'].str.extract("(" + "|".join(lend_cos) +")",flags = re.IGNORECASE)
        df_lender_txns['days_diff'] = (time_added.date()-df_lender_txns['posted_date']).dt.days
        df_lender_txns['amount'] = df_lender_txns['amount'].round(2)


        #conditions to determine lender variables
        is_amount_positive = (df_lender_txns['amount']>0)
        is_credit_last_30days = is_amount_positive & (df_lender_txns['days_diff']<=30)
        is_amount_negative = (df_lender_txns['amount']<0)
        is_debit_last_30days = is_amount_negative & (df_lender_txns['days_diff']<=30)

        #prepare lender variables
        lender_vars['LenderAmountDeb'] = float(df_lender_txns.loc[is_amount_negative,'amount'].sum())
        lender_vars['LenderCountCred'] = float(df_lender_txns[is_amount_positive].shape[0])
        lender_vars['LenderAmountCred30'] = float(df_lender_txns.loc[is_credit_last_30days,'amount'].sum())
        lender_vars['LenderCountDeb'] = float(df_lender_txns[is_amount_negative].shape[0])
        lender_vars['LenderAmountDeb30'] = float(df_lender_txns.loc[is_debit_last_30days,'amount'].sum())
        lender_vars['LenderCountCred30'] = float(df_lender_txns.loc[is_credit_last_30days].shape[0])
        lender_vars['LenderCountDeb30'] = float(df_lender_txns.loc[is_debit_last_30days].shape[0])
        lender_vars['LenderAmountCred'] = float(df_lender_txns.loc[is_amount_positive,'amount'].sum())
        lender_vars['UniqLenderCount'] = float(df_lender_txns['lenderName'].nunique())

    return pd.DataFrame(lender_vars,index=[0])

def gc_summary_vars(loanid, bank_report_string, primary_acct):
    """Summary variables from bank report

    Args:
    loanid
    bank_report_string (str): bank report
    primary_account (str): Primary checking account

    Returns:
    df_gc_vars (pandas df): contains summary vars
    """

    
    bank_report_json = json.loads(bank_report_string)
    df_all_accounts = pd.DataFrame(bank_report_json['accounts'])
    df_all_accounts= df_all_accounts.drop_duplicates('accountNumber')
    df_pr_account = df_all_accounts[df_all_accounts['accountNumber'] == primary_acct]
    cols = ['noOfDaysNegative', 'noOfDaysPositive']
    df_gc_vars = df_pr_account[cols]
    df_gc_vars.reset_index(drop = True, inplace = True)
    df_gc_vars['LoanId'] = loanid
    return df_gc_vars