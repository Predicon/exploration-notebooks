#importing useful libraries
import numpy as np
import pandas as pd

def esisgn_outlier_treat(x):
    """Removes outliers 
    
    Args:
    x (float)
    
    Returns: 
    x (float)
    
    """
    
    if x < 0 or x > 432000:
        return np.nan
    else:
        return x



def dti_outlier_treat(x):
    """Removes outliers 
    
    Args:
    x (float)
    
    Returns: 
    x (float)
    
    """
    
    if x == 0 or x > 100:
        return np.nan
    else:
        return x


def clean_bankapp_feat(df_bankapp):
    """Imputes nulls and cleans all the features from the bankapp database

    Args:
    parameters : df_bankapp(pandas df)
    -------
    returns : pandas df with a cleaner version of bankapp dataset.
    """
    #direct_deposit
    
    df_bankapp['in1_is_direct_deposite'] = df_bankapp['in1_is_direct_deposite'].fillna(df_bankapp['in1_is_direct_deposite'].mode(dropna = True).iloc[0])
    #dti
    df_bankapp['dti_percentage'] = df_bankapp['dti_percentage'].fillna(df_bankapp['dti_percentage'].median(skipna = True))
    #pds_history
    df_bankapp['is_pds_history_found'] = df_bankapp['is_pds_history_found'].fillna(df_bankapp['is_pds_history_found'].mode(dropna = True).iloc[0])
    #pay_day_test_result_amount
    df_bankapp['pay_day_test_result_amount'] = df_bankapp['pay_day_test_result_amount'].fillna(df_bankapp['pay_day_test_result_amount'].median(skipna = True))
    return df_bankapp