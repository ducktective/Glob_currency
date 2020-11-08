import re
import logging


def get_url(date):
    '''
    input : date. If date is empty, we take current date from our host. 
    '''
    try:
        if date == 0:
            url = 'https://www.cbr-xml-daily.ru/daily_json.js'
            logging.info(f'url = {url}')
        else:
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
