import os
from datetime import datetime

from airflow.operators.dagrun_operator import TriggerDagRunOperator

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.exceptions import AirflowException

# from MIP_Lead_Generation.util_functions import remove_files, copy_files, emis_check_parsed_and_loaded_files_equality, \
#     check_and_create_dirs, get_dirs, move_unloaded_xlsx_files, emis_pkl_check_handling, remove_irrelevant_unloaded_files
from MIP_Valute.util_functions import get_dirs
import MIP_Valute.config.column_names as cfg_column_names
import MIP_Valute.config.headers as cfg_headers
import MIP_Valute.config.paths as cfg_paths
import MIP_Valute.config.sqls as cfg_sqls

import MIP_Valute.parser as prs

airflow_env = os.environ["AIRFLOW_ENV"]
def_args = {'owner': 'beltikovad'}

with DAG('valute_load_to_STG',
         default_args=def_args,
         description='loading valute to vertica',
         schedule_interval='0 0 */2 ? * *',
         start_date=datetime(2020, 9, 14),
         catchup=False) as dag:

    # def truncate_stg_valute(**kwargs):
    #     prs.truncate_stg_emis()
    #     return 'STG tables truncated'

    # def move_from_exchange(**kwargs):
    #     exchange_dir = get_dirs(
    #         keys_with_env=['exchange_dir'], env=airflow_env, var_name="mip_lead_gen_emis")[0]
    #     input_dir = get_dirs(dir_to_concatenate=[cfg_paths.path_dict['input']],
    #                          concatenate_with='data_dir', var_name="mip_lead_gen_emis")[0]

    #     copy_files(exchange_dir, input_dir, ext='xlsx')
    #     remove_files([exchange_dir])
    #     return f"files moved from exchange_dir to input_dir"

    def parse_input():
        prs.parse()
        return f'OK ok ok ok ok ok ok'

    def load_stg():
        where_to_read = get_dirs(
            keys_with_env=['exchange_dir'], env=airflow_env, var_name='mip_valute')[0]
        prs.load_valute(where_to_read)
        return f'loaded'

    # ClearData = PythonOperator(dag=dag,
    #                            task_id='clear_data',
    #                            python_callable=clear_data
    #                            )
    # Move = PythonOperator(dag=dag,
    #                       task_id='move_from_W',
    #                       python_callable=move_from_exchange
    #                       )
    Parse = PythonOperator(dag=dag,
                           task_id='parse_input',
                           python_callable=parse_input
                           )
    Load = PythonOperator(dag=dag,
                          task_id='load_stg',
                          python_callable=load_stg
                          )
    Parse >> Load

    # TruncateSTG = PythonOperator(dag=dag,
    #                              task_id='truncate_stg_emis',
    #                              python_callable=truncate_stg_emis
    #                              )
    # load_ods = TriggerDagRunOperator(task_id='load_ods',
    #                                  trigger_dag_id='lead_gen_load_EMIS_ODS')

   #  ClearData >> TruncateSTG >> Move >> Parse >> dummy_operator_start

   #  dummy_operator_end >> load_ods >> final_check
