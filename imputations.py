#importing useful libraries
import numpy as np
import pandas as pd
from sklearn.impute import SimpleImputer
from sklearn_pandas import CategoricalImputer

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


def impute(df, cols, strategy):
    """Imputes nulls for the specified columns from the passed dataframe, startegy must be same for all  the cols passed. For mode, only single column is accepted.

    Args:
    col (list): list of columns to be transformed
    df (pandas df): dataframe containing the columns
    strategy (string): how to impute
 
    returns : Imputes learned object.
    """
    
    imp_mean = SimpleImputer(missing_values = np.nan, strategy = 'mean')
    imp_median = SimpleImputer(missing_values = np.nan, strategy = 'median')
    imp_mode = CategoricalImputer(missing_values = np.nan, strategy = 'most_frequent')
    
    if strategy == 'mean':
        imp_mean.fit(df[cols])
        return imp_mean
    elif strategy == 'median':
        imp_median.fit(df[cols])
        return imp_median
    elif strategy == 'most_frequent':
        imp_mode.fit(df[cols])
        return imp_mode
    else:
        return("Invalid strategy: enter one of the startegies among mean, median and most_frequent")
    

