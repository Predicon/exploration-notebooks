import pandas as pd
import utilities as util
import numpy as np


def preprocess_bankapp_db(df):
    """Modifies bankapp data

    Args:
        df (pandas df): Dataframe consisting all the required columns from bankapp

    Returns:
        pandas df: Modified dataframe
    """
    df = df.drop_duplicates('LoanId')
    rename_columns = {'final_decision':'agent_decision'}
    df.rename(columns = rename_columns, inplace = True)
    df['entered_date'] = pd.to_datetime(df['entered_date'])
    is_BV_uncertain_approved = df['agent_decision'].isin(['Bank Validation Uncertain', 'Bank Validation Approved'])
    df = df[is_BV_uncertain_approved]
    return df

def preprocess_model_db(df):
    """Modifies Model scored data

    Args:
        df (pandas df): Dataframe consisting all the required columns from predicon model database

    Returns:
        pandas df: Modified dataframe
    """
    df['LoanId'] = df['LoanId'].astype(str)
    df['TimeAdded'] = pd.to_datetime(df['TimeAdded'].map(lambda x : x.date()))
    df = df.loc[df['TimeAdded'] >= '2020-06-09', :].reset_index(drop = True)
    df = df.merge(df['Score'].map(util.pos).reset_index(drop = True).rename('Decision'), left_index = True, right_index = True)
    return df[['LoanId', 'Decision']]

def preprocess_loan_history_db(df):
    """Modifies Funded Loans data

    Args:
        df (pandas df): Dataframe consisting all the required columns from funded loans database

    Returns:
        pandas df: Modified dataframe
    """
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    df['TimeAdded'] = df['TimeAdded'].dt.date
    df['TimeAdded'] = pd.to_datetime(df['TimeAdded'])
    df = df.groupby('LoanId', as_index = False).sum()
    df['LenderApproved'] = df['LenderApproved'].map(util.is_lender_approved)
    return df

def preprocess_loan_performance(df):
    df['LoanId'] = df['LoanId'].astype(int).astype(str)
    df['IsFirstDefault'] = df['IsFirstDefault'].astype(str)
    return df
    

def preprocess_lender_reject_sub_categories(df, cols):
    
    for i in cols:
        try:
            df[i] = df[i].map(lambda x : str(x)[:-1].split('x')[1][-1] if x != None else x)
        except:
            continue
    no_sale_categories = pd.DataFrame([x for x in np.where(df == '1', df.columns,'').tolist() if len(x) > 0]).values
    no_sale_categories = [x.tolist() for x in no_sale_categories]
    no_sale = []
    for x in no_sale_categories:
        temp = []
        for y in x:
            if len(y) != 0:
                temp.append(y)
        no_sale.append(temp)
    df = pd.merge(df, pd.DataFrame([' + '.join(x) for x in no_sale], columns = ['sub_category']), left_index = True, right_index = True)
    return df[['LoanId', 'entered_date', 'underwriting_final_decision', 'sub_category', 'Decision', 'LenderApproved', 'IsFirstDefault']]

