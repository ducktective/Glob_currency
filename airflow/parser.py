import url_creater
import urllib
import json
import pandas as pd
import vertica_python
import re
import logging
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException

import connection
import url_creator
import list_of_currency


def take_data_from_url(url, date):
    '''
    Taking hhtp response file and transform it in to json.
    input: url. It is url adress.
    return: data. It is dictionary.
    '''
    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        logging.info()
        return data
    except Exception as ex:
        logging.error(
            f'Error while transforming httpresponse to json(dictionary) for {date}')


def check_dates(start_date, today_date):
    '''
    This function making verification for date compliance.
    '''
    if re.match(r'\d{4}-\d{2}-d{2}', start_date) and re.match(r'\d{4}-\d{2}-d{2}', today_date):
        start_date = strptime(start_date, '%Y-%m-%d')
        today_date = strptime(today_date, '%Y-%m-%d')
        if start_date < today_date:
            logging.info('Start date and today date have correct fromat.')
            return start_date, today_date
    else:
        logging.error('One of dates or all of them have uncorrect format.')
        raise AirflowException


def crete_data_interval(start_date='2020-01-01', today_date=''):
    if today_date == '':
        today_date = datetime.now().strftime('%Y-%m-%d')

    start_date_datatime, today_date_datetime = check_dates(
        start_date, today_date)
    logging.info(f'Start day is {start_date}. Today date is {today_date}.')
    tomorow_date_datetime = today_date_datetime + timedelta(days=1)
    tomorow_date = tomorow_date_datetime.strftime('%Y-%m-%d')
    dates_list = []
    while start_date_datatime <= tomorow_date_datetime:
        dates_list.append(start_date_datatime.strftime('%Y-%m-%d'))
        start_date_datatime += timedelta(days=1)
    logging.info(
        f'List of dates from start date {start_datedate} to tomorow date {tomorow_date}: {dates_list}')
    return dates_list
