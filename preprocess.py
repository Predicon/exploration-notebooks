import pandas as pd
import utilities as util
import numpy as np


def preprocess_bankapp_db(bankapp_db):
    """Modifies bankapp data

    Args:
        bankapp_db (pandas df): Dataframe consisting all the required columns from bankapp

    Returns:
        pandas df: Modified dataframe
    """
    bankapp_db = bankapp_db.drop_duplicates('LoanId')
    rename_columns = {'final_decision':'agent_decision'}
    bankapp_db.rename(columns = rename_columns, inplace = True)
    bankapp_db['entered_date'] = pd.to_datetime(bankapp_db['entered_date'])
    return bankapp_db

def preprocess_model_db(model_db):
    """Modifies Model scored data

    Args:
        model_db (pandas df): Dataframe consisting all the required columns from predicon model database

    Returns:
        pandas df: Modified dataframe
    """
    model_db['LoanId'] = model_db['LoanId'].astype(str)
    model_db['TimeAdded'] = pd.to_datetime(model_db['TimeAdded'].map(lambda x : x.date()))
    model_db = model_db.loc[model_db['TimeAdded'] >= '2020-06-09', :].reset_index(drop = True)
    model_db = model_db.merge(model_db['Score'].map(util.pos).reset_index(drop = True).rename('Decision'), left_index = True, right_index = True)
    return model_db[['LoanId', 'Decision']]

def preprocess_loan_history_db(fundedloans_db):
    """Modifies Funded Loans data

    Args:
        fundedloans_db (pandas df): Dataframe consisting all the required columns from funded loans database

    Returns:
        pandas df: Modified dataframe
    """
    fundedloans_db['LoanId'] = fundedloans_db['LoanId'].astype(int).astype(str)
    fundedloans_db['TimeAdded'] = fundedloans_db['TimeAdded'].dt.date
    fundedloans_db['TimeAdded'] = pd.to_datetime(fundedloans_db['TimeAdded'])
    fundedloans_db = fundedloans_db.groupby('LoanId', as_index = False).sum()
    fundedloans_db['LenderApproved'] = fundedloans_db['LenderApproved'].map(util.is_lender_approved)
    return fundedloans_db

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
    return df[['LoanId', 'entered_date', 'underwriting_final_decision', 'sub_category', 'Decision', 'LenderApproved']]
