import pandas as pd

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


ID = ''
PASSWORD = ''
HOST = ''


def login(host, id, password):
    server = smtplib.SMTP(host)
    server.ehlo()
    server.starttls()
    server.login(ID, PASSWORD)
    return server


def create_msg(frm, to, subject, body):
    msg = MIMEMultipart()
    msg['To'] = to
    msg['From'] = frm
    msg['Subject'] = subject
    msg.attach(MIMEText(body, 'plain'))
    return msg


def send_msg(server, msg):
    server.send_message(msg)


def get_subscribers(fname):
    subscribers = []
    with open(fname) as f:
        for subscriber in map(lambda x: x.strip(), f):
            'sanjay.singh.bce13@itbhu.ac.in'
            dept = subscriber.split('@')[0]
            dept = dept.split('.')
            dept = dept[2][:-2]
            yield subscriber, dept


def subject_msg(company_name):
    return '{} willingness deadline'.format(company_name)


def send_emails(host, email_id, password, subscribers):
    with login(host, email_id, password) as server:
        for subscriber, dept in get_subscribers(subscribers):
            df = pd.read_csv('data/willingness_{}.csv'.format(dept))
            df = df[df['email_count_willingness'] < 2]
            df.loc[:, 'email_count_willingness'] += 1
            df.loc[:, 'subjects'] = df['company_name'].apply(lambda x: subject_msg(x))
            df.to_csv('willingness_{}.csv'.format(dept))
            for subject, msg, num in zip(df['subjects'], df['email_msg_willingness'], df['email_count_willingness']):
                msg = create_msg(ID, subscriber, subject, msg)
                send_msg(server, msg)


def send():
    send_emails(HOST, ID, PASSWORD, 'subscribers.txt')


if __name__ == '__main__':
    send()
