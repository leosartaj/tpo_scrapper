import requests
import lxml.html
import bs4

import numpy as np
import pandas as pd

USER_AGENT = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36'
LOGIN_URL = 'https://www.placement.iitbhu.ac.in/accounts/login'
COMPANY_URL = 'https://www.placement.iitbhu.ac.in/company/calendar'
USERNAME = 'arun.meena.apm13@itbhu.ac.in'
PASSWORD = 'arunsanjaysartaj'

# Name of column -> Classname
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
    'willingness_deadline': 'willingness_dead'
}

# Column name -> start index
clips = {
    'active_backlog': 'Active backlog allowed :'.find(':') ,
    'branch_issue_deadline': 'Branch Issues Deadline :'.find(':') + 1,
    'cgpa': 'cgpa :'.find(':'),
    'course': 'Course(s) :'.find(':'),
    'dept': 'Department(s) :'.find(':'),
    'exam_date': 'Exam Date :'.find(':'),
    'interview_date': 'Interview Date :'.find(':') ,
    'ppt_date': 'PPT Date : '.find(':'),
    'status': 'Status : '.find(':'),
    'total_backlog': 'Total backlog allowed :'.find(':'),
    'updated_at': 'st Updated:'.find(':') + 2,
    'willingness_deadline': 'Willingness Deadline :'.find(':') + 1,
    'x_percent': 'X :'.find(':'),
    'xii_percent': 'XII :'.find(':')
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


def get_data(company_date, data_mapper):
    companies = []
    for company_div in company_data.find_all('div', attrs={'class': 'row company'}):
        company = {}
        for name, class_name in data_mapper.items():
            company[name] = company_div.find(attrs={'class': class_name}).text
        get_package_data(company_div, company)
        companies.append(company)
    return companies


def clip_data(companies, clips):
    for company in companies:
        for clip_name, clip_num in clips.items():
            num = clip_num + 2
            company[clip_name] = company[clip_name][num:]
    return companies


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
            if ans > 1000 and ans < 10000:
                ans *= 10**4
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

    df.loc[:, packages] = df[packages].apply(package_value_series)
    df.loc[:, packages] = df[packages].astype(np.float64)

    return df


def find_depts(df):
    depts = set([])
    for value in df.dept.unique():
        for dept in value.split(' '):
            depts.add(dept)
    return depts


def process_df(companies):
    df = pd.DataFrame(companies)
    df.loc[:, 'active_backlog'] = df['active_backlog'].replace({'N/A': 0}).astype(np.float64)
    df.loc[:, 'total_backlog'] = df['total_backlog'].replace({'N/A': 0}).astype(np.float64)
    df.loc[:, 'cgpa'] = df['cgpa'].astype(np.float64)
    df.loc[:, 'exam_date'] = pd.to_datetime(df['exam_date'])
    df.loc[:, 'interview_date'] = pd.to_datetime(df['interview_date'])
    df.loc[:, 'ppt_date'] = pd.to_datetime(df['ppt_date'])
    df.loc[:, 'branch_issue_deadline'] = pd.to_datetime(df['branch_issue_deadline'].str.replace('|', ','))
    df.loc[:, 'updated_at'] = pd.to_datetime(df['updated_at'])
    df.loc[:, 'x_percent'] = df['x_percent'].str[:-1].astype(np.float64)
    df.loc[:, 'xii_percent'] = df['xii_percent'].str[:-1].astype(np.float64)
    df.loc[:, 'profile'] = df['profile'].replace({'': np.nan})
    df.loc[:, 'willingness_deadline'] = pd.to_datetime(df['willingness_deadline'].str.replace('|', ','))

    for dept in find_depts(df):
        df.loc[:, dept] = df.dept.apply(lambda x: 1 if dept in x else 0)

    for course in ['btech', 'idd', 'mtech', 'phd']:
        df.loc[:, course] = df.course.apply(lambda x: 1 if course in x else 0)

    df = process_packages(df)
    df = df.drop(['dept', 'course', 'purpose'], axis=1)
    return df


if __name__ == '__main__':
    session = requests.session()

    form = make_login_form(session, USERNAME, PASSWORD)

    headers = {'Referer': LOGIN_URL, 'User-Agent': USER_AGENT}
    login = session.post(LOGIN_URL, data=form, headers=headers)

    params = {'page_size': 250}
    data = session.get(COMPANY_URL, params=params)

    company_data = bs4.BeautifulSoup(data.text, 'html.parser')

    companies = get_data(company_data, data_mapper)
    clip_data(companies, clips)
    df = process_df(companies)

    df.to_csv('companies.csv', index=False)
    df = pd.read_csv('companies.csv', parse_dates=['branch_issue_deadline', 'exam_date', 'interview_date', 'ppt_date', 'updated_at', 'willingness_deadline'])
    print(df.head())
