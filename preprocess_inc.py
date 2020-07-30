#from helper import parse_dates
import pandas as pd
import string
from nltk.corpus import stopwords
import re

#for preprocessing memos
PUNCT_TO_REMOVE = string.punctuation
STOPWORDS = set(stopwords.words('english'))
pattern = '[0-9]'


def preprocess_bank_reports(df):
    """Modifies extracted bank statement data
    Args:
        df (pandas df): Dataframe consisting all the required columns from iloans database
    Returns:
        pandas df: Modified dataframe
    """
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    df['TimeAdded'] = pd.to_datetime(df['TimeAdded'].map(lambda x : x.date()))
    return df[['LoanId', 'EmployerName']]

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

def remove_punctuation(text):
    """custom function to remeove punctuations

    Args:
        text (str): text which has to be cleaned

    Returns:
        [str]: cleaned text without punctuations
    """
    return text.translate(str.maketrans('', '', PUNCT_TO_REMOVE))

def remove_stopwords(text):
    """custom function to remove stopwords

    Args:
        text (str): text which has to be cleaned

    Returns:
        [str]: cleaned text without punctuations
    """
    return " ".join([word for word in str(text).split() if word not in STOPWORDS])

def remove_nums(text):
    """custom function to remove numbers

    Args:
        text (str): test which has to be cleaned

    Returns:
        [str]: cleaned text without punctuations
    """
    return re.sub(pattern, '', text)