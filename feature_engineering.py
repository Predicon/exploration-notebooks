from helper import fetch_checking_acct_txns
import pandas as pd
import multiprocessing as mp
import re
import joblib
import numpy as np




NCPU = mp.cpu_count() - 2 if mp.cpu_count() > 2 else 1

def get_primary_account(bankreport):
    """
    Flag primary checking account (account having max transaction count)
    
    Args:
    bankreport (json)
    loanid (str)
    
    Returns:
    account number (str) : account number of primary account
    """
    df_txn = fetch_checking_acct_txns(bankreport)
    if df_txn.empty is False:
        df_txns_count = df_txn['account_number'].value_counts()
        return df_txns_count.idxmax()


def get_transaction_days_count(primary_account,bank_report):
    """Checks if number of transaction days >=60 given an account
    
    Args:
    primary_account (str): Account number of primary account
    bank_report (str): bank report string

    Returns:
    True or False (bool)
    """ 
    df_checking_txns = fetch_checking_acct_txns(bank_report)
    if df_checking_txns.empty is False:
        df_primary_account_txns = df_checking_txns[df_checking_txns['account_number']==primary_account]
        df_primary_account_txns= df_primary_account_txns.sort_values(by='posted_date')
        first_txn_date = df_primary_account_txns['posted_date'].iloc[0]
        last_txn_date = df_primary_account_txns['posted_date'].iloc[-1]
        txn_days_count = (last_txn_date - first_txn_date).days
        return txn_days_count >= 60

def calculate_age(current_date, dob):
    """Calculates age in years
    
    Args:
    current_date (pandas datetime object)
    dob (pandas datetime object): Date of Birth

    Returns:
    age (int): Age in years
    """
    
    age = len(pd.date_range(start=dob,end=current_date,freq='Y'))
    return age

lead_provider_list = [
    "MarketBullet",
    "StopNGo",
    "Nimbus",
    "EPCVIP",
    "PingBid",
    "LeapThry",
    "Acquir",
    "RoundSky",
    "Zero",
    "LeadPie",
    "ITMedia",
    "LeadsMarket",
    "freedom"
]



def fe_iloans(df):
    
    df['Age'] = df.apply(lambda x: calculate_age(x['OriginationDate'],x['DateOfBirth']), axis = 1)
    
    
    lead_providers = [provider.lower() for provider in lead_provider_list]
    df['LeadProvider'] = df.loc[df['Campaign'].notnull(), 'Campaign'].str.extract("(" + "|".join(lead_providers) +")")
    df.replace({'LeadProvider': {'Roundsky': 'RoundSky'}}, inplace = True)
    
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
    lender_vars['loan_id'] = loanid
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


def get_transaction_time_series(primary_account, bank_report, loan_id):
    df_checking_txns = fetch_checking_acct_txns(bank_report)
    if df_checking_txns.empty is False:
        df_txns = df_checking_txns.loc[df_checking_txns['account_number']==primary_account, :]
        df_txns['posted_date'] = pd.to_datetime(df_txns['posted_date'])
        df_txns['LoanId'] = loan_id
        return df_txns   


def daily_txn_summary(primary_account, bank_report, loan_id):
    df = get_transaction_time_series(primary_account, bank_report, loan_id)
    df_debit = df[(df['LoanId'] == loan_id) & (df['account_number'] == primary_account) & (df['amount'] < 0)]
    df_credit = df[(df['LoanId'] == loan_id) & (df['account_number'] == primary_account) & (df['amount'] >= 0)]
    df_debit_amount = df_debit.groupby('posted_date', as_index = False)['amount'].sum().set_index('posted_date').resample('D').sum()
    df_credit_amount = df_credit.groupby('posted_date', as_index = False)['amount'].sum().set_index('posted_date').resample('D').sum()
    df_debit_count = df_debit.groupby('posted_date', as_index = False)['amount'].count().set_index('posted_date').resample('D').sum()
    df_credit_count = df_credit.groupby('posted_date', as_index = False)['amount'].count().set_index('posted_date').resample('D').sum()
    df_balance = df[df['LoanId'] == loan_id].groupby('posted_date', as_index = False)['balance'].first().set_index('posted_date').asfreq('D').fillna(method = 'ffill')
    #setting into dataframe
    df_final = pd.concat([df_debit_amount, df_credit_amount, df_credit_count, df_debit_count, df_balance], axis = 1, verify_integrity = True, ignore_index = True)
    df_final.rename(columns = {0 : 'debit_amt',
                                1 : 'credit_amt',
                                2 : 'credit_count',
                                3 : 'debit_count',
                                4 : 'balance'}, inplace = True)
    df_final.fillna(0.0, inplace = True)
    avg_daily_debit = df_final['debit_amt'].resample('D').sum().mean()
    avg_daily_credit = df_final['credit_amt'].resample('D').sum().mean()
    avg_daily_debit_count = int(df_final['debit_count'].resample('D').sum().mean())
    avg_daily_credit_count = int(df_final['credit_count'].resample('D').sum().mean())
    avg_daily_balance = df_final['balance'].resample('D').sum().mean()
    avg_weekly_debit = df_final['debit_amt'].resample('W').sum().mean()
    avg_weekly_credit = df_final['credit_amt'].resample('W').sum().mean()
    avg_weekly_debit_count = int(df_final['debit_count'].resample('W').sum().mean())
    avg_weekly_credit_count = int(df_final['credit_count'].resample('W').sum().mean())
    avg_weekly_balance = df_final['balance'].resample('W').sum().mean()
    return [avg_daily_debit, avg_daily_credit, avg_daily_debit_count, avg_daily_credit_count, avg_daily_balance,
            avg_weekly_debit, avg_weekly_credit, avg_weekly_debit_count, avg_weekly_credit_count, avg_weekly_balance,
            loan_id]


def create_balance_vars(loan_id,report_string,pr_acct):
    """Difference between positive and negative balance days
    Args:
    loanid (int) : LoanId
    bank_report_string (str): bank report
    primary_account (str): Primary checking account
    Returns:
    list : conatains LoanId and respective differences bewteen positive ans negative transaction days
    """
    df_checking_txns = fetch_checking_acct_txns(report_string)
    df_pr_acct_txns = df_checking_txns[df_checking_txns['account_number']==pr_acct]
    df_daily_closing_balance = df_pr_acct_txns.groupby('posted_date', as_index = False)['balance'].first()
    df_daily_closing_balance['posted_date'] = pd.to_datetime(df_daily_closing_balance['posted_date'])
    df_daily_closing_balance.set_index('posted_date', inplace = True)
    df_daily_closing_balance_resampled = df_daily_closing_balance.asfreq('D')
    df_daily_closing_balance_resampled = df_daily_closing_balance_resampled.fillna(method = 'ffill')
    pos = df_daily_closing_balance_resampled[df_daily_closing_balance_resampled['balance'] > 0].shape[0]
    neg = df_daily_closing_balance_resampled[df_daily_closing_balance_resampled['balance'] <= 0].shape[0]
    return [loan_id, pos - neg]


def fe_bank_reports(df):
    with mp.Pool(processes=NCPU) as pool:
        result_primary_accts = pool.map(get_primary_account, df['BankReportData'])
    
    df['primary_account'] = result_primary_accts

    with mp.Pool(processes=NCPU) as pool:
        txn_days_count = pool.starmap(get_transaction_days_count, zip(df['primary_account'],df['BankReportData']))
    df['txn_days_count'] = txn_days_count

    has_gt_60_days_txns = (df['txn_days_count'] == True)
    df = df[has_gt_60_days_txns]
    
    df_lender_vars = pd.DataFrame()
    with mp.Pool(processes=NCPU) as pool:
        df_lender_vars_temp = pool.starmap(create_lender_vars, zip(df['loan_id'],df['BankReportData'],df['ReportTimeAdded'],df['primary_account']))
    df_lender_vars=pd.concat(df_lender_vars_temp,ignore_index=True)

    df_lender_vars.reset_index(drop=True,inplace=True)
    df = pd.merge(df,df_lender_vars,how='left',on='loan_id')

    df_stats = pd.DataFrame()
    with mp.Pool(processes = NCPU) as pool:
        stats_temp = pool.starmap(daily_txn_summary, zip(df['primary_account'], df['BankReportData'],  df['loan_id']))
    df_stats = pd.DataFrame(stats_temp)
    df_stats.rename(columns = {0 : 'avg_daily_debit', 
                               1 : 'avg_daily_credit', 
                               2 : 'avg_daily_debit_count', 
                               3 : 'avg_daily_credit_count', 
                               4 : 'avg_daily_balance',
                               5 : 'avg_weekly_debit', 
                               6 : 'avg_weekly_credit', 
                               7 : 'avg_weekly_debit_count', 
                               8 : 'avg_weekly_credit_count', 
                               9 : 'avg_weekly_balance',
                               10 : 'loan_id'}, inplace = True)
    df = pd.merge(df, df_stats, how = 'left', on = 'loan_id')

    df_balance_vars = pd.DataFrame()
    with mp.Pool(processes=NCPU) as pool:
        df_balance_vars_temp = pool.starmap(create_balance_vars, zip(df['loan_id'],df['BankReportData'],df['primary_account']))
    df_balance_vars = pd.DataFrame(df_balance_vars_temp, columns = ['loan_id', 'diff_pos_neg_days'])
    df = pd.merge(df, df_balance_vars, on = 'loan_id', how = 'left')
    
    return df   



