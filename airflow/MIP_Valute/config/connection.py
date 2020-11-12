''' 
vertica connection lib 
'''

from airflow.hooks.base_hook import BaseHook


class Connection:
    '''
    get Vertica connection from Airflow
    '''

    def __init__(self, conn_info):
        self.conn_name = conn_info

    def get_settings(self):
        settings = {}

        connection = BaseHook.get_connection(self.conn_name)
        password = connection.password
        host = connection.host
        login = connection.login
        db = connection.schema
        settings['db'] = db
        settings['password'] = password
        settings['host'] = host
        settings['user'] = login
        settings['port'] = 5433

        return settings
