from helper import parse_dates
import pandas as pd


def preprocess_bank_reports(df):
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