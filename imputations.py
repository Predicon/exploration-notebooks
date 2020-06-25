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
    imp_mode = CategoricalImputer(missing_values = np.nan)
    
    if strategy == 'mean':
        imp_mean.fit(df[cols])
        return imp_mean
    elif strategy == 'median':
        imp_median.fit(df[cols])
        return imp_median
    elif strategy == 'mode':
        imp_mode.fit(df[cols])
        return imp_mode
    else:
        return("Invalid strategy: enter one of the startegies among mean, median and most_frequent")
    

def iloans_impute(df):
    df['LeadProvider'].fillna('Freedom', inplace = True)
    return df

def impute_esign(df):
    imp_acc_count = impute(df, ['AccessCount'], 'median')
    df[['AccessCount']] = imp_acc_count.transform(df[['AccessCount']])
    return df, imp_acc_count

def impute_bankapp (df):
    imp_dti = impute(df, ['dti'], 'median')
    imp_pay_day = impute(df, ['pay_day_test_result_amount'], 'median')
    imp_in1_cycle = impute(df, 'in1_income_cycle', 'mode')
    imp_miss_loan_pay = impute(df, ['missing_loan_payment'], 'median')
    imp_is_latest_sal_least = impute(df, 'is_latest_sal_least', 'mode')
    imp_rolling_sal = impute(df, ['rolling_sal_mean'], 'median')
    imp_income_type = impute(df, 'income_type', 'mode')
    imp_net_sal_change = impute(df, ['net_sal_change'], 'median')
    df[['dti']] = imp_dti.transform(df[['dti']])
    df[['pay_day_test_result_amount']] = imp_pay_day.transform(df[['pay_day_test_result_amount']])
    df[['in1_income_cycle']] = imp_in1_cycle.transform(df[['in1_income_cycle']])
    df[['missing_loan_payment']] = imp_miss_loan_pay.transform(df[['missing_loan_payment']])
    df[['is_latest_sal_least']] = imp_is_latest_sal_least.transform(df[['is_latest_sal_least']])
    df[['rolling_sal_mean']] = imp_rolling_sal.transform(df[['rolling_sal_mean']])
    df[['income_type']] = imp_income_type.transform(df[['income_type']])
    df[['net_sal_change']] = imp_net_sal_change.transform(df[['net_sal_change']])
    return df, imp_dti, imp_pay_day, imp_in1_cycle, imp_miss_loan_pay, imp_is_latest_sal_least, imp_rolling_sal, imp_income_type, imp_net_sal_change

def impute_learning(df, testing_set = False):
    if testing_set:
        df['LeadProvider'].fillna('freedom', inplace = True)
        return df
    df['LeadProvider'].fillna('freedom', inplace = True)
    imp_acc_count = impute(df, ['AccessCount'], 'median')
    df[['AccessCount']] = imp_acc_count.transform(df[['AccessCount']])
    imp_dti = impute(df, ['dti'], 'median')
    imp_pay_day = impute(df, ['pay_day_test_result_amount'], 'median')
    df[['dti']] = imp_dti.transform(df[['dti']])
    df[['pay_day_test_result_amount']] = imp_pay_day.transform(df[['pay_day_test_result_amount']])
    return df, imp_acc_count, imp_dti, imp_pay_day

