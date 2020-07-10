import pandas as pd
import numpy as np
import seaborn as sns
from matplotlib import pyplot as plt
import missingno as msno
import ppscore as pps

def initial_eda_checks(df):
    """
    Checks for nulls in the passed Dataframe
    Args:
    df(pandas dataframe)
    returns:
    pandas dataframe with nulls info
    """
    if df.isnull().sum().sum() > 0:
        mask_total = df.isnull().sum().sort_values(ascending = False) 
        total = mask_total[mask_total > 0]

        mask_percent = df.isnull().mean().sort_values(ascending = False) 
        percent = mask_percent[mask_percent > 0] 

        missing_data = pd.concat([total, percent], axis = 1, keys = ['Total nulls', 'Percentage of nulls'])

        g = msno.matrix(df, labels = True)
    
        #print(f'Total and Percentage of NaN:\n {missing_data}')
        return pd.DataFrame(missing_data), g
    else: 
        print('No NaN found.')
        return 0

def counting_unique_values_cat(df):
    """Checks if there are any categorical columns with only one unique value

    Args:
        df (pandas df): dataframe consisting all the features and target

    Returns:
        list: all columns having only 1 unique value
    """
    cat_cols = df.select_dtypes(include = ['object', 'bool']).columns.values
    def count_uniq(x):
        if x.nunique() == 1:
            return x.name

    cat_cols_with_one_uniq = [x for x in df[cat_cols].apply(count_uniq).values if x != None]
    return cat_cols_with_one_uniq

def counting_unique_values_num(df):
    """Checks if there are any numeric columns with only one unique value

    Args:
        df (pandas df): dataframe consisting all the features and target

    Returns:
        list: all columns having only 1 unique value
    """
    num_cols = df.select_dtypes(include = [np.number]).columns.values
    def count_uniq(x):
        if x.nunique() == 1:
            return x.name

    num_cols_with_one_uniq = [x for x in df[num_cols].apply(count_uniq).values if x != None]
    return num_cols_with_one_uniq

    
def histograms_numeric_columns(df):
    """
    Calculates the histogram and density plot of numeric features
    Args:
    df(pandas dataframe):dataframe consisting of all the columns
    Returns:
    seaborn plot object
    """
    num_cols = df.select_dtypes(include = [np.number]).columns.values
    cols_to_plot = [x for x in num_cols if df[x].isnull().sum() != df.shape[0]]
    df = df[cols_to_plot]
    f = pd.melt(df, value_vars = cols_to_plot) 
    g = sns.FacetGrid(f, col = 'variable',  col_wrap = 4, sharex = False, sharey = False, height = 8, aspect = 1)
    g = g.map(sns.distplot, 'value')
    return g

def boxplot_numerical_columns(df):
    """
    Calculates the boxplot of numeric features
    Args:
    df(pandas dataframe):dataframe consisting of all the columns
    Returns:
    seaborn plot object
    """
    num_cols = df.select_dtypes(include = [np.number]).columns.values
    cols_to_plot = [x for x in num_cols if df[x].isnull().sum() != df.shape[0]] 
    df = df[cols_to_plot]
    f = pd.melt(df, value_vars = cols_to_plot) 
    g = sns.FacetGrid(f, col = 'variable',  col_wrap = 4, sharex = False, sharey = False, height = 8, aspect = 1)
    g = g.map(sns.boxplot, 'value')
    return g 

def countplot_categorical_columns(df, force = False, cols = None):
    """
    Calculates countplots of categorical columns having value counts > 1 and <= 13
    Args:
    df(pandas df) : dataframe consisting all the features and target
    force(bool) : whether to print the columns having more than 13 unique values or not
    cols(list) : list of columns whose countplot is to be fetched. If None, then all psooible columns are taken.
    Returns:
    seaborn plotted object
    """

    try:
        cat_cols = df[cols].select_dtypes(include = ['object', 'bool']).columns.values
    except:
        cat_cols = df.select_dtypes(include = ['object', 'bool']).columns.values
    try:
        def det_cat_col(x):
            if len(x.value_counts()) <= 13:
                if len(x.value_counts()) > 1:
                    return x.name

        if force == False:
            cat_cols_to_plot = [x for x in df[cat_cols].apply(det_cat_col).values if x != None] 
        else:
            cat_cols_to_plot = cat_cols

        def countplot(y, **kwargs):
            ax = sns.countplot(y = y)
            total = df.shape[0]
            for p in ax.patches:
                percentage = '{:.1f}%'.format(100 * p.get_width()/total)
                x = p.get_x() + p.get_width() + 0.02
                y = p.get_y() + p.get_height()/2
                ax.annotate(percentage, (x, y))

        f = pd.melt(df, value_vars = cat_cols_to_plot)
        g = sns.FacetGrid(f, col='variable',  col_wrap=2, sharex=False, sharey=False, height=10)
        g = g.map(countplot, 'value')
        return g
    except:
        print("Too much values to handle....try setting force to True")

def heatmap_numeric_w_dependent_variable(df, dependent_variable):
    """
    Returns a heatmap of independent variables' correlations with dependent variable
    Args:
    df(pandas dataframe) : dataframe consisting of all the features and taregt
    dependent_variable(str) : target
    Returns:
    seaborn plotted object
    """
    plt.figure(figsize = (8, 10))
    g = sns.heatmap(df.corr()[[dependent_variable]].sort_values(by = dependent_variable), 
                    annot = True, 
                    cmap = 'coolwarm', 
                    vmin = -1,
                    vmax = 1) 
    return g


def get_pps(df, dependent_variable):
    """
    Calculates the PPS score between all the features and the target
    Args:
    df(pandas dataframe) : The dataframe consisting the whole dataset along with the new feature
    target(string) : Name of the target, as in the dataframe
    Returns:
    array of dict : An array of dictionary in which each element of the array is a dictionary representing the complete pps
    procedure of each feature wrt target
    """
    
    # pps score
    pps_feat_tar = {}
    for feature in df.drop([dependent_variable], axis = 1).columns:
        pps_feat_tar[feature] = pps.score(df, feature, dependent_variable)['ppscore']
        
    return pd.DataFrame(pps_feat_tar.items(), columns = ['Features', 'PPS scores'])
    

'''def dendrogram(df):
    """
    Calcuates the hierarchial relationship between features
    Args:
    df(pandas dataframe)
    Returns:
    g(plotted object)
    """
    g = msno.dendrogram(df)
    return g'''

'''def nullity_heatmap(df):
    g = msno.heatmap(df)
    return g'''
