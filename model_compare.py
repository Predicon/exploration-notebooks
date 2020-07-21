import pandas as pd
import utilities as util

def get_kpi(modified_bankapp_db, modified_model_db, modified_fundedloans_db):
    """Generates kpi metircs by combing all the metrics from the 3 databases for model comparison

    Args:
        modified_bankapp_db (pandas df): Modified bankapp data
        modified_model_db (panadas df): Modified predicon model data
        modified_fundedloans_db (pandas df): Modified funded loans data

    Returns:
        pandas df: complete KPI metric for model comparison
    """
    merged_db = pd.merge(pd.merge(modified_bankapp_db, modified_model_db, on = 'LoanId', how = 'left'), 
                                  modified_fundedloans_db, on = 'LoanId', how = 'left')
    
    
    merged_db = merged_db.merge(merged_db['LenderApproved'].map(util.is_lender_approved).reset_index(drop = True).rename('isLenderApproved'), left_index = True, right_index = True)
    
    #generating bankapp kpi metrics
    bankapp_agg = merged_db.groupby('entered_date', as_index = False)['manual_decision'].count()
    #generating model kpi metrics
    model_agg = pd.merge(merged_db.groupby(['entered_date', 'Decision'], as_index = False)['Score'].count().loc[merged_db.groupby(['entered_date', 'Decision'], as_index = False)['Score'].count()['Decision'] == 'Positive', :].reset_index(drop = True), 
                         merged_db.groupby('entered_date', as_index = False)['Score'].count(), on = 'entered_date', how = 'inner')
    model_agg.drop('Decision', axis = 1, inplace = True)
    model_agg.rename(columns = {'Score_x' : '#positive_scored', 'Score_y' : '#model_scored'}, inplace = True)
    #generating funded loans kpi metrics
    funded_agg = merged_db.groupby('entered_date', as_index = False)['isLenderApproved'].sum()
    #combining all the metrics as one
    kpi = pd.merge(pd.merge(bankapp_agg, model_agg, on = 'entered_date', how = 'left'),
                   funded_agg, on = 'entered_date', how = 'left')
    kpi.rename(columns = {'LoanId' : '#Lender Approved'}, inplace = True)
    return kpi