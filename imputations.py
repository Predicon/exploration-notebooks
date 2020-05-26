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


def impute(df, col, strategy):
    """Imputes nulls from the passed dataframe

    Args:
    col (string)
    df (pandas df)
    strategy (string)
 
    returns : pandas df with an imputed version of the dataframe passed.
    """
    
    if strategy == 'mode':
        df[col] = df[col].fillna(df[col].mode(dropna = True).iloc[0])
    elif strategy == 'median':
        df[col] = df[col].fillna(df[col].median(skipna = True))
    elif strategy == 'mean':
        df[col] = df[col].fillna(df[col].mean(skipna = True))
    
    return df