import pandas as pd


def extract_bankapp(bank_app_conn):
    """Extracts required features from bankapp

    Args:
        bank_app_conn (pymysql connection): connection object

    Returns:
        pandas df: dataframe containing required features fetched from bankapp server
    """
    query_bank_app_reviewed = '''
                            select loan_id As LoanId, 
                            final_decision,
                            reasons_for_decision,
                            entered_date,
                            path                             
                            from loan 
                            where campaign like '%Production%'
                            and STR_TO_DATE(entered_date , '%m/%d/%Y') >= STR_TO_DATE('06/09/2020','%m/%d/%Y')
                          '''
    bank_app = pd.read_sql_query(query_bank_app_reviewed, con = bank_app_conn)
    return bank_app

def extract_model(predicon_conn):
    """Extracts required features from predicon server

    Args:
        predicon_conn(pymysql connection): connection object

    Returns:
        pandas df: dataframe containing required features fetched from predicon server
    """
    model = pd.read_sql_query('''SELECT * FROM 
                                     FreedomScores 
                                     WHERE Version='v1.3.0' 
                                     GROUP BY LoanId''', con = predicon_conn)
    return model

def extract_funded_loans(iloans_conn):
    """Extracts required features from iloans

    Args:
        iloans_conn (pymssql connection): connection object

    Returns:
        pandas df: dataframe containing required featurs fetched from iloans server
    """
    query_funded = '''
                SELECT 
                    LoanId,
                    LoanStatus,
                    TimeAdded,
                    (CASE when LoanStatus = 'Lender Approved' then 1 else 0 END) as LenderApproved
                FROM view_FCL_Loan_History
                WHERE TimeAdded >= '2020-06-09'
               '''
    funded_loans = pd.read_sql_query(query_funded,con = iloans_conn)
    return funded_loans


def modify_bankapp_db(bankapp_db):
    """Modifies bankapp data

    Args:
        bankapp_db (pandas df): Dataframe consisting all the required columns from bankapp

    Returns:
        pandas df: Modified dataframe
    """
    bankapp_db = bankapp_db.drop_duplicates('LoanId')
    rename_columns = {'final_decision':'manual_decision',
                       'reasons_for_decision':'manual_decision_reason'}
    bankapp_db.rename(columns = rename_columns, inplace = True)
    bankapp_db['entered_date'] = pd.to_datetime(bankapp_db['entered_date'])
    return bankapp_db

def modify_model_db(model_db):
    """Modifies Model scored data

    Args:
        model_db (pandas df): Dataframe consisting all the required columns from predicon model database

    Returns:
        pandas df: Modified dataframe
    """
    model_db['LoanId'] = model_db['LoanId'].astype(str)
    model_db.drop('EsigTimeSignedDiff_In_SEC', axis = 1, inplace = True)
    model_db['TimeAdded'] = pd.to_datetime(model_db['TimeAdded'].map(lambda x : x.date()))
    model_db = model_db.loc[model_db['TimeAdded'] >= '2020-06-09', :].reset_index(drop = True)
    def pos(x):
        if x <= 0.51:
            return 'Positive'
        elif x > 0.51:
            return 'Neutral'
    model_db = model_db.merge(model_db['Score'].map(pos).reset_index(drop = True).rename('Decision'), left_index = True, right_index = True)
    return model_db

def modify_fundedloans_db(fundedloans_db):
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
    return fundedloans_db

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
    
    def is_lender_approved(x):
        if x > 0:
            return 1
        else:
            return 0
    
    merged_db = merged_db.merge(merged_db['LenderApproved'].map(is_lender_approved).reset_index(drop = True).rename('isLenderApproved'), left_index = True, right_index = True)
    
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