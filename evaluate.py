import numpy as np
import pandas as pd

def get_KS(df_pred, target, prob):
    """Returns KS given scores
    
    Args:
    df_pred (pandas df): DataFrame containing target variable and model score
    target (string): Name of the target column
    prob (string): Name of the column name containing the prediction probabilities
    
    Returns:
    float: KS value
    """
    df_scores = df_pred.sort_values(by = prob)
    total_good = (df_scores[target] == 'False').sum()
    total_bad = (df_scores[target] == 'True').sum()
    df_scores['cum_good_perc'] = (df_scores[target] == 'False').cumsum()/total_good
    df_scores['cum_bad_perc'] = (df_scores[target] == 'True').cumsum()/total_bad
    df_scores['cum_diff'] = np.abs((df_scores['cum_good_perc'] - df_scores['cum_bad_perc']))
    return df_scores['cum_diff'].max()

def quantile_table_and_score_bins(df_pred, target, prob, n = 10):
    """Returns a quantile table given model scores (default is decile)
    
    Args:
    df_pred (pandas df): DataFrame containing target variable and model score
    target (string): Name of the target column
    prob (string): Name of the column name contains the prediction probabilities
    
    Returns:
    df_scores_deciles (pandas df): Pandas dataframe containing quantiles
    score_bin: quantile range
    """
    df_scores = df_pred.sort_values(by = prob)
    df_scores['decile'],score_bin = pd.qcut(df_scores[prob],10,labels=[1,2,3,4,5,6,7,8,9,10],retbins = True)
    df_scores[target] = df_scores[target].astype(int)
    df_scores_deciles = df_scores.groupby('decile',as_index=False).agg({prob:['count','min','max','mean'],target:'sum'})
    df_scores_deciles.columns = ['decile','count','min_score','max_score','mean_score','bad_count']
    df_scores_deciles['perc_bad'] = (df_scores_deciles['bad_count']/df_scores_deciles['count']) * 100
    return df_scores_deciles, score_bin

