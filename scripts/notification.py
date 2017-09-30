import numpy as np
import pandas as pd

from datetime import datetime, date

from scrapper import SAVE_PATH, save_df


def read_df(fname):
    df_path = '{}/{}'.format(SAVE_PATH, fname)
    df = pd.read_csv(df_path, parse_dates=['branch_issue_deadline', 'exam_date', 'interview_date', 'ppt_date', 'updated_at', 'willingness_deadline'])
    return df


def query_dept_dates(df, dept, date_col, min_date=None):
    if min_date is None:
        min_date = datetime.now()
    return df[(df[dept] == 1) & (df[date_col] > min_date)].copy()


def create_notification_df(df, dept, date_col, min_date=None, suffix=None):
    notification_df = query_dept_dates(df, dept, date_col, min_date)

    if suffix is None:
        suffix = date_col

    if notification_df.shape[0]:
        notification_df.loc[:, 'email_msg_{}'.format(suffix)] = create_emails(notification_df)
    return notification_df.reset_index(drop=True)


def email(x):
    company = x['company_name']
    package_ctc = x['idd_imd_ctc'] / 10**5
    package_home = x['idd_imd_home'] / 10**5
    willingness = x['willingness_deadline']
    msg = ('Willingness for {} is open till {}. '
           'The Company is offering a CTC of {:.2f} Lakhs and Take Home of {:.2f} Lakhs'
           .format(company, willingness, package_ctc, package_home))
    return msg


def create_emails(df):
    return df.apply(email, axis=1)


if __name__ == '__main__':
    df = read_df('companies.csv')
    depts = ['mat', 'bce']
    for dept in depts:
        df = create_notification_df(df, dept, 'willingness_deadline', min_date=date(2017, 9, 29), suffix='willingness')
        if dept == 'mat':
            dept = 'apm'
        save_df(df, 'willingness_{}.csv'.format(dept))
