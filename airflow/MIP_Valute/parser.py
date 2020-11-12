import url_creater
import urllib
import json
import pandas as pd
import vertica_python
import re
import logging
from datetime import datetime, timedelta
from airflow.exceptions import AirflowException
import os

import connection
import url_creator
import list_of_currency
from MIP_Lead_VALUTE.connection import Connection


def get_url(date):
    '''
    input : date. If date is empty, we take current date from our host. 
    '''
    try:
        if re.match(r'\d{4}-\d{2}-d{2}', date):
            date_list = re.split(r'-', date)
            year = date_list[0]
            month = date_list[1]
            day = date_list[2]
            url = f'https://www.cbr-xml-daily.ru//archive//{year}//{month}//{day}//daily_json.js'
            logging.info(f'url = {url}')
        return url
    except Exception as ex:
        logging.error(f'Can not create url, because of {ex}')


def take_data_from_url(url, date):
    '''
    Taking hhtp response file and transform it in to json.
    input: url. It is url adress.
    return: data. It is dictionary.
    '''
    try:
        response = urllib.request.urlopen(url)
        data = json.loads(response.read())
        # logging.info()
        return data
    except Exception as ex:
        logging.error(
            f'Error while transforming httpresponse to json(dictionary) for {date}')


# def check_dates(start_date, today_date):
#     '''
#     This function making verification for date compliance.
#     '''
#     if re.match(r'\d{4}-\d{2}-d{2}', start_date) and re.match(r'\d{4}-\d{2}-d{2}', today_date):
#         start_date = strptime(start_date, '%Y-%m-%d')
#         today_date = strptime(today_date, '%Y-%m-%d')
#         if start_date < today_date:
#             logging.info('Start date and today date have correct fromat.')
#             return start_date, today_date
#     else:
#         logging.error('One of dates or all of them have uncorrect format.')
#         raise AirflowException


def crete_data_interval(start_date, today_date):
    # if today_date == '':
    #     today_date = datetime.now().strftime('%Y-%m-%d')

    # start_date_datatime, today_date_datetime = check_dates(
    #     start_date, today_date)
    # logging.info(f'Start day is {start_date}. Today date is {today_date}.')
    # tomorow_date_datetime = today_date_datetime + timedelta(days=1)
    # tomorow_date = tomorow_date_datetime.strftime('%Y-%m-%d')
    # dates_list = []
    # while start_date_datatime <= tomorow_date_datetime:
    #     dates_list.append(start_date_datatime.strftime('%Y-%m-%d'))
    #     start_date_datatime += timedelta(days=1)
    # logging.info(
    #     f'List of dates from start date {start_datedate} to tomorow date {tomorow_date}: {dates_list}')
    # return dates_list
    logging.info(f'Start day is {start_date.strftime(' % Y - %m - %d')}. Today date is {today_date.strftime(' % Y - %m - %d')}.')
    tomorow_date_datetime = today_date + timedelta(days=1)
    dates_list = []
    while start_date <= tomorow_date:
        dates_list.append(start_date.strftime('%Y-%m-%d'))
        start_date += timedelta(days=1)
    logging.info(
        f'List of dates from start date {start_datedate} to tomorow date {tomorow_date}: {dates_list}')
    return dates_list


def check_ods_and_stg_is_empty():
    lead_gen = Variable.get("mip_lead_gen_valute", deserialize_json=True)
    vertica_config = Connection(lead_gen['conn_id_leadgen']).get_settings()
    connection_info = {'host': vertica_config['host'],
                       'port': vertica_config['port'],
                       'user': vertica_config['user'],
                       'password': vertica_config['password'],
                       'database': vertica_config['db']
                       }

    sql = cfg.sqls.is_database_empty[ods]
    number_of_rows_ods = execute_sql_check(connection_info, sql, 'ods')

    sql = cfg.sqls.is_database_empty[stg]
    number_of_rows_stg = execute_sql_check(connection_info, sql, 'stg')

    if (number_of_rows_stg + number_of_rows_ods) == 0:
        return True
    else:
        return False


def execute_sql_check(connection_info, sql, table_slice):
    with vertica_python.connect(**connection_info) as connection:
        try:
            cur = connection.cursor()
            logging.info(f"Executing sql {sql}")
            answer = cur.execute(sql).fetchall()
            cur.execute('COMMIT;')
            logging.info('Transaction commited;')
            cur.close()
            logging.info(f'Table {table_slice} is empty.')
            return answer
        except Exception as ex:
            logging.warning(
                f'Checking {table_slice} for emptiness end wrong: {ex.args}')
            raise AirflowException


def parse_and_load():
    is_empty = check_ods_and_stg_is_empty()
    today_date = datetime.now()
    if is_empty == False:
        start_date = today_date - timedelta(days=7)
        list_of_dates = crete_data_interval(
            start_date=start_date, today_date=today_date)
    elif is_empty == True:
        start_date = datetime(2010, 1, 1)
        list_of_dates = crete_data_interval(
            start_date=start_date, today_date=today_date)
    logging.info(f'Number of dates: {len(list_of_dates)}.')
    df_total = pd.DataFrame()

    for date in list_of_dates:
        url = get_url(date)
        data = take_data_from_url(url, date)
        date_changed = date
        check_for_error = 'code' in data.keys() and 'error' in data.values()
        if check_for_error:
            try:
                while check_for_error
                    date_changed = date - timedelta(days=1)
                    url = get_url(date)
                    data = take_data_from_url(url, date_changed)
                    check_for_error = 'code' in data.keys() and 'error' in data.values()
            except Exception as ex:
                logging.error(f'Something went wrong when changed date: {ex}')
        try:
            df = pd.DataFrame(data['Valute'])
            if data != data_changed:
                current_date = data_changed.strftime('%Y-%m-%d')
            current_date = date.strftime('%Y-%m-%d')
            df = df.T
        except Exception as ex:
            logging.error(
                f'Did not created DataFrame or DataFrame did not transpose: {ex}')
        try:
            df.drop(['Previous'], axis=1, inplace=True)
            df['Nominal'] = df['Nominal'].astype(int)
            df['Value'] = df['Value'].astype(float)
            df['Date'] = re.match(r'\d{4}-\d{2}-\d{2}', current_date).group(0)
            logging.info('Changing was success.')
        except Error as ex:
            logging.error(f'Error when change DataFrame: {ex}')
        try:
            df_total = df_total.append(df)
            logging.info(
                'DataFrame was successfuly appended to total DataFrame.')
        except Exception as ex:
            logging.error(f'Error when appended DataFrame: {ex}.')
    try:
            # формируем csv для загрузки
        df_total.to_csv(
            os.path.join(output_dir, filename_without_ext +
                         '_' + source_name + '.csv'),
            sep=vertica_load.delimiter,
            line_terminator=vertica_load.line_terminator,
            header=True,
            index=False,
            encoding='utf-8',
            na_rep=' '
        )
        # на этом моменте файл успешно parsed
        successfully_parsed_files.append(
            filename_without_ext + '_' + source_name + '.csv')
