import pandas as pd
from matplotlib import pyplot as plt
import seaborn as sns

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

def get_model_performance_wrt_matured_loans(df, model_decision):
    if model_decision.lower() == 'positive':
        decision = 1
    elif model_decision.lower() == 'neutral':
        decision = 0
    else:
        return ("Please check the model_decision parameter...must be positive or neutral")
    is_approved_ = (df['Decision'] == decision) & (df['LenderApproved'] == 1)
    plt.figure(figsize = (5, 5))
    ax = sns.countplot(data = df[is_approved_], y = 'IsFirstDefault',
                   order = df.loc[is_approved_,'IsFirstDefault'].value_counts().index)
    total = df[is_approved_]
    total = total[total['IsFirstDefault'].notnull()].shape[0]
    for p in ax.patches:
        percentage = '{:.1f}%'.format(100 * p.get_width()/total)
        x = p.get_x() + p.get_width() + 0.02
        y = p.get_y() + p.get_height()/2
        ax.annotate(percentage, (x, y))
    plt.title("Loan Performance for {} applicants".format(model_decision))
    return ax

def get_no_sale_lender_bank_verification_subcategories(df):
    no_sale_flags = ['No Sale Lender', 'No Sale Bank Verification']
    is_no_sale_lender_bank_verification = df['underwriting_final_decision'].isin(no_sale_flags)
    no_sale_lender_bank_verification = df[is_no_sale_lender_bank_verification]
    no_sale_lender = no_sale_lender_bank_verification[no_sale_lender_bank_verification['underwriting_final_decision'] == 'No Sale Lender']
    no_sale_bank_verification = no_sale_lender_bank_verification[no_sale_lender_bank_verification['underwriting_final_decision'] == 'No Sale Bank Verification']
    plt.figure(figsize = (8, 8))
    ax1 = sns.countplot(data = no_sale_lender, y = 'sub_category',
                   order = no_sale_lender['sub_category'].value_counts().index)
    total = no_sale_lender.shape[0]
    for p in ax1.patches:
        percentage = '{:.2f}%'.format(100 * p.get_width()/total)
        count = p.get_width()
        x1 = p.get_x() + p.get_width() + 0.02
        y1 = p.get_y() + p.get_height()/2
        x2 = p.get_x() + p.get_width() + 0.02
        y2 = p.get_y() + p.get_height()/2 + 0.3
        ax1.annotate(percentage, (x1, y1))
        ax1.annotate(count, (x2, y2))
    plt.title("No Sale Lender")
    plt.show()
    plt.figure(figsize = (8, 8))
    ax2 = sns.countplot(data = no_sale_bank_verification, y = 'sub_category',
                   order = no_sale_bank_verification['sub_category'].value_counts().index)
    total = no_sale_lender.shape[0]
    for p in ax2.patches:
        percentage = '{:.2f}%'.format(100 * p.get_width()/total)
        count = p.get_width()
        x1 = p.get_x() + p.get_width() + 0.02
        y1 = p.get_y() + p.get_height()/2
        x2 = p.get_x() + p.get_width() + 0.02
        y2 = p.get_y() + p.get_height()/2 + 0.3
        ax2.annotate(percentage, (x1, y1))
        ax2.annotate(count, (x2, y2))
    plt.title("No Sale Bank Verification")
    return ax1, ax2