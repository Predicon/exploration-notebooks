import sys
sys.path.insert(0, '/home/vishal/kpi/exploration-notebooks')
import EDA as eda
import pandas as pd

def is_lender_approved(x):
        if x > 0:
            return 1
        else:
            return 0

def pos(x):
        if x <= 0.51:
            return 1
        else:
            return 0

def get_confusion_matrix(df, norm = False):
    """Get the confusion matrix between lender approved and model decisions

    Args:
        df (pandas df): merged dataframe consisting bankapp, model and loan history data
        norm (str) : whether to normalize along rows, columns or values, default:False

    Returns:
        pandas df: a confusion matrix b/w lender approveds and model decisions
    """
    is_lender_approved_present = df['LenderApproved'].notnull()
    return pd.crosstab(df['LenderApproved'][is_lender_approved_present], df['Decision'][is_lender_approved_present], normalize = norm)

def get_lender_approved_model_disapproved_reasons(df):
    #lender_reject_model_accept = set(df[(df['LenderApproved'] == 0) & (df['Decision'] == 1)]['LoanId'].values)
    #df_lender_reject_model_accept = df[df['LoanId'].isin(lender_reject_model_accept)][['underwriting_final_decision', 'agent_decision']]
    return df['underwriting_final_decision'].value_counts(), eda.countplot_categorical_columns(df, cols = ['underwriting_final_decision'])