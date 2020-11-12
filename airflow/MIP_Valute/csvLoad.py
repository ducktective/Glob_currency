"""
Библиотека для загрузки источников лидогенерации
"""
import os
import vertica_python
#from MIP_Lead_Generation.util_functions import write_filenames_to_txt_in_outputdir as write_to_txt
import logging
from airflow.exceptions import AirflowException

delimiter = '\x1F'
line_terminator = '\x1E'


def upload_data_to_vertica(connection_info, output_dir, table_header, rej_table, list_of_files_to_load=None):
    """
    load csv to vertica
    :param rej_table: name of table to insert rejected rows
    :param list_of_files_to_load: if list of files to be loaded is specified
    :param table_header: table name and columns to load
             example ODS_ICIS.SIBUR_FORECASTS (PRODUCT_NAME, DATE_FACT, DATE_PREDICT, QUOTE_VALUE)
    :param output_dir: dir with csv
    :param connection_info: conn info to vertica db
    :return: list of successfully loaded files
    """
    successfully_loaded_files = []
    unsuccessfully_loaded_files = []
    # creating connection

    with vertica_python.connect(**connection_info) as connection:
        copy_req = """COPY {table_header}
                FROM STDIN PARSER
                fcsvparser(
                    type='traditional',
                    delimiter='{delimiter}',
                    header='true',
                    record_terminator='{line_terminator}',
                    reject_on_materialized_type_error = 'true'
                ) REJECTED DATA AS TABLE {rej_table};
            """.format(
            table_header=table_header,
            delimiter=delimiter,
            line_terminator=line_terminator,
            rej_table=rej_table
        )

        if not list_of_files_to_load:
            files_without_subdirs = [
                f for f in os.listdir(output_dir)
                if os.path.isfile(os.path.join(output_dir, f)) and f.endswith('csv')
            ]
        else:
            files_without_subdirs = list_of_files_to_load

        if not files_without_subdirs:
            logging.error('No files to load')

        else:
            logging.info(f'Found {len(files_without_subdirs)} csv,'
                         f' List of csv to load in dir {output_dir}: {files_without_subdirs}')
            try:
                cur = connection.cursor()
                cur.execute("START TRANSACTION;")
                cur.execute("SET SESSION AUTOCOMMIT TO OFF;")
                logging.info("Opening transaction;")
            except Exception as ex:
                logging.error(f'csvLoad.py:upload_data_to_vertica: {ex.args}')

            # IMPORTANT: all or none strategy on load
            for filename in files_without_subdirs:
                logging.info(f'Loading file {filename}')

                with open(os.path.join(output_dir, filename), "rb") as fs:
                    try:

                        cur.copy(copy_req, fs, buffer_size=65536)
                        cur.execute('select get_num_rejected_rows();')
                        after = cur.fetchone()

                        logging.info(f'. after {after}')
                        if after[0] > 0:
                            unsuccessfully_loaded_files.append(filename)
                        else:
                            successfully_loaded_files.append(filename)
                    except Exception as ex:
                        unsuccessfully_loaded_files.append(filename)
                        logging.error(f'csvLoad.py:upload_data_to_vertica: {ex.args}')
                        cur.execute("ROLLBACK;")
                        if ex.args[0].find('Insufficient privilege') > -1:
                            raise AirflowException

            cur.execute("COMMIT;")
            logging.info("Transaction commited;")
            cur.close()
            logging.info(f'Csv file was successfully loaded.')
            
    return successfully_loaded_files, unsuccessfully_loaded_files
