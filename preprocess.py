"""This module is for preprocessing of different data scources
Eg:Rename columns, clean strings, handle or not missing values
"""


from helper import fetch_checking_acct_txns
import pandas as pd
import joblib
import re
import json
from imputations import dti_outlier_treat

def preprocess_iloans(df):
    df['Campaign'] = df['Campaign'].str.lower()
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    df.rename(columns = {"LoanId":"loan_id"}, inplace = True)
    loans = list(df['loan_id'])
    joblib.dump(loans,"loans.pkl")
    return df

def preprocess_bank_reports(df):
    df = df.drop_duplicates('LoanId')
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    df.rename(columns = {"LoanId":"loan_id"}, inplace = True)
    return df

def preprocess_esign(df):
    df = df.drop_duplicates('LoanId')
    df.drop('EsigTimeSignedDiff_In_SEC', axis = 1, inplace = True)
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    df.rename(columns = {"LoanId":"loan_id"}, inplace = True)
    return df


def preprocess_bank_app(df):
    df.rename(columns = {"final_decision":"bank_app_decision","dti_percentage":"dti"}, inplace = True)
    df['dti'] = df['dti'].map(dti_outlier_treat)
    df.replace({'in1_is_direct_deposite': {'': 'Unknown'}}, inplace = True)
    return df