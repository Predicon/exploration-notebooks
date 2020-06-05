"""This module is for preprocessing of different data scources
Eg:Rename columns, clean strings, handle or not missing values
"""


from helper import fetch_checking_acct_txns
import pandas as pd
import joblib
import re
import json

def preprocess_iloans(df):
    df['Campaign'] = df['Campaign'].str.lower()
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    df = df.rename(columns = {"LoanId":"loan_id"})
    loans = list(df['loan_id'])
    joblib.dump(loans,"loans.pkl")
    return df

def preprocess_bank_reports(df):
    df = df.drop_duplicates('LoanId')
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    df = df.rename(columns = {"LoanId":"loan_id"})
    return df


def preprocess_bank_app(df):
    df = df.rename(columns = {"final_decision":"bank_app_decision","dti_percentage":"dti"})
    return df