import os
import requests
import lxml.html
import bs4

import numpy as np
import pandas as pd


LOGIN_URL = 'https://www.placement.iitbhu.ac.in/accounts/login'
COMPANY_URL = 'https://www.placement.iitbhu.ac.in/company/calendar'
USERNAME = os.environ['TPO_USERNAME']
PASSWORD = os.environ['TPO_PASSWORD']


data_mapper = {
    'updated_at': 'updated_at',
    'company_name': 'company_name',
    'profile': 'company_profile',
    'purpose': 'purpose',
    'x_percent': 'x',
    'xii_percent': 'xii',
    'cgpa': 'cgpa',
    'course': 'course',
    'dept': 'dept',
    'active_backlog': 'a_backlog',
    'total_backlog': 't_backlog',
    'ppt_date': 'ppt_date',
    'exam_date': 'exam_date',
    'interview_date': 'interview_date',
    'status': 'status',
    'branch_issue_deadline': 'branch_issue_dead',
    'willingness_deadline': 'willingness_dead',
}


clips = {
    'active_backlog': 'Active backlog allowed :'.find(':'),
    'active_backlog2': 'Active backlog allowed :'.find(':') ,
    'active_backlog3': 'Active backlog allowed :'.find(':') ,
    'branch_issue_deadline': 'Branch Issues Deadline :'.find(':') + 1,
    'cgpa': 'cgpa :'.find(':'),
    'cgpa1': 'cgpa :'.find(':'),
    'cgpa2': 'cgpa :'.find(':'),
    'course': 'Course(s) :'.find(':'),
    'course1': 'Course(s) :'.find(':'),
    'course2': 'Course(s) :'.find(':'),
    'dept': 'Department(s) :'.find(':'),
    'dept1': 'Department(s) :'.find(':'),
    'dept2': 'Department(s) :'.find(':'),
    'exam_date': 'Exam Date :'.find(':'),
    'interview_date': 'Interview Date :'.find(':') ,
    'ppt_date': 'PPT Date : '.find(':'),
    'status': 'Status : '.find(':'),
    'total_backlog': 'Total backlog allowed :'.find(':'),
    'updated_at': 'st Updated:'.find(':') + 2,
    'willingness_deadline': 'Willingness Deadline :'.find(':') + 1,
    'x_percent': 'X :'.find(':'),
    'x_percent1': 'X :'.find(':'),
    'x_percent2': 'X :'.find(':'),
    'xii_percent': 'XII :'.find(':'),
    'xii_percent1': 'XII :'.find(':'),
    'xii_percent2': 'XII :'.find(':')
}


def make_login_form(session, username, password):
    login = session.get(LOGIN_URL)
    login_html = lxml.html.fromstring(login.text)
    hidden_inputs = login_html.xpath(r'//form//input[@type="hidden"]')
    form = {x.attrib['name']: x.attrib['value'] for x in hidden_inputs}
    form['login'] = username
    form['password'] = password
    return form


def get_package_data(company_div, company):
    package = company_div.find('div', attrs={'class': 'package'})
    for tr in package.find_all('tr'):
        course, num = False, 0
        course_name = None
        for td in tr.find_all('td'):
            if course:
                if num == 0 and td.text:
                    company['{}_ctc'.format(course_name)] = td.text
                if num == 1 and td.text:
                    company['{}_home'.format(course_name)] = td.text
                if num == 1:
                    course = False
                num += 1
            if td.text.lower() in ['b.tech', 'idd/imd', 'm.tech', 'phd']:
                course, num = True, 0
                course_name = td.text.lower().replace('/', '_').replace('.', '')


def generate_names(name):
    i = 0
    while True:
        if i:
            yield '{}{}'.format(name, i)
        else:
            yield name
        i += 1


def get_data(company_data, data_mapper):
    companies = []
    for company_div in company_data.find_all('div', attrs={'class': 'row company'}):
        company = {}
        for name, class_name in data_mapper.items():
            names = generate_names(name)
            for name, tag in zip(names, company_div.find_all(attrs={'class': class_name})):
                company[name] = tag.text
        get_package_data(company_div, company)
        companies.append(company)
    return companies


def clip_data(companies, clips):
    for company in companies:
        for clip_name, clip_num in clips.items():
            num = clip_num + 2
            if clip_name in company.keys():
                company[clip_name] = company[clip_name][num:]
    return companies


def find_depts(df):
    depts = set([])
    for value in df.dept.unique():
        for dept in value.split(' '):
            depts.add(dept)
    return depts


def _average(values):
    ans, num = 0, 0
    for val in values:
        try:
            ans += float(val)
            num += 1
        except ValueError:
            continue
    if num:
        return ans / num


def package_value(value):
    if not isinstance(value, float):
        ans = _average(value.split(' '))
        if ans:
            if ans < 100:
                ans *= 10**5
            return ans

    return value


def package_value_series(packages):
    return packages.apply(package_value)


def process_packages(df):
    packages = []
    courses = ['btech', 'idd_imd', 'mtech', 'phd']
    packages.extend(map(lambda x: '{}_ctc'.format(x), courses))
    packages.extend(map(lambda x: '{}_home'.format(x), courses))

    df.loc[:, packages] = df[packages].replace('Yet to Decide', np.nan)
    df.loc[:, packages] = df[packages].replace('[lL]akhs', '', regex=True)
    df.loc[:, packages] = df[packages].replace('LPA', '', regex=True)
    df.loc[:, packages] = df[packages].replace('\+ plus other benefits', '', regex=True)
    df.loc[:, packages] = df[packages].replace(',', '', regex=True)
    df.loc[:, packages] = df[packages].replace('[-/]', ' ', regex=True)
    df.loc[:, packages] = df[packages].replace('Depending on experience', np.nan, regex=True)
    df.loc[:, packages] = df[packages].replace('Inclusive of 20% variable pay', np.nan)
    df.loc[:, packages] = df[packages].replace('Attached in JD', np.nan)
    df.loc[:, packages] = df[packages].replace('Attached', np.nan)

    df.loc[:, packages] = df[packages].apply(package_value_series)
    df.loc[df.company_name == 'VMWare', ['idd_imd_ctc', 'btech_ctc']] = [2202108, 2202108]
    df.loc[:, packages] = df[packages].astype(np.float64)

    return df


def process_df(companies):
    df = pd.DataFrame(companies)

    df.loc[:, 'active_backlog'] = df['active_backlog'].replace({'N/A': 0}).astype(np.float64)
    df.loc[:, 'total_backlog'] = df['total_backlog'].replace({'N/A': 0}).astype(np.float64)

    df.loc[:, 'cgpa'] = df[['cgpa', 'cgpa1', 'cgpa2']].astype(np.float64).mean(axis=1)
    df.loc[:, 'exam_date'] = pd.to_datetime(df['exam_date'])
    df.loc[:, 'interview_date'] = pd.to_datetime(df['interview_date'])
    df.loc[:, 'ppt_date'] = pd.to_datetime(df['ppt_date'])
    df.loc[:, 'branch_issue_deadline'] = pd.to_datetime(df['branch_issue_deadline'].str.replace('|', ','))
    df.loc[:, 'updated_at'] = pd.to_datetime(df['updated_at'])
    df.loc[:, 'x_percent'] = df[['x_percent', 'x_percent1', 'x_percent2']].apply(lambda x: x.str[:-1]).astype(np.float64).mean(axis=1)
    df.loc[:, 'xii_percent'] = df[['xii_percent', 'xii_percent1', 'xii_percent2']].apply(lambda x: x.str[:-1]).astype(np.float64).mean(axis=1)
    df.loc[:, 'profile'] = df['profile'].replace({'': np.nan})
    df.loc[:, 'willingness_deadline'] = pd.to_datetime(df['willingness_deadline'].str.replace('|', ','))

    df.loc[:, 'dept'] = df['dept1'].fillna('') + ' ' + df['dept2'].fillna('') + df['dept'].fillna('')
    for dept in find_depts(df):
        df.loc[:, dept] = df.dept.apply(lambda x: 1 if dept in x else 0)

    df.loc[:, 'course'] = df[['course', 'course1', 'course2']].fillna('').sum(axis=1)
    for course in ['btech', 'idd', 'mtech', 'phd']:
        df.loc[:, course] = df.course.apply(lambda x: 1 if course in x else 0)

    df = process_packages(df)
    df = df.drop(['course', 'purpose', 'active_backlog1', 'active_backlog2',
                 'total_backlog1', 'total_backlog2', 'cgpa1', 'cgpa2',
                 'course1', 'course2', 'dept', 'dept1', 'dept2',
                 'x_percent1', 'x_percent2', 'xii_percent1', 'xii_percent2'], axis=1)

    return df


def scrape():
    session = requests.session()
    form = make_login_form(session, USERNAME, PASSWORD)
    params = {'Referer': LOGIN_URL}
    login = session.post(LOGIN_URL, data=form, headers=params)
    params = {'page_size': 250}
    data = session.get(COMPANY_URL, params=params)
    company_data = bs4.BeautifulSoup(data.text, 'html.parser')
    companies = get_data(company_data, data_mapper)
    clip_data(companies, clips)
    df = process_df(companies)
    df.to_csv('data/companies.csv', index=False)


if __name__ == '__main__':
    scrape()
