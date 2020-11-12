from airflow.models import Variable
import datetime
import logging
import os
import pathlib
import re
import shutil
import numpy as np


def get_dirs(keys=None, keys_with_env=None, env=None, dir_to_concatenate=None, concatenate_with=None,
             var_name="mip_lead_gen"):
    lead_gen = Variable.get(var_name, deserialize_json=True)
    dirs = []

    if keys is not None:
        for key in keys:
            try:
                dirs.append(os.path.expandvars(lead_gen[key]))
            except Exception as ex:
                logging.error(
                    f'{ex.args} while trying to get key {key} from Variable mip_lead_gen')

    if keys_with_env is not None and env is not None:
        for key in keys_with_env:
            try:
                dirs.append(os.path.expandvars(lead_gen[env][key]))
            except Exception as ex:
                logging.error(
                    f'{ex.args} while trying to get key {key} with env {env} from Variable mip_lead_gen')

    if dir_to_concatenate is not None and concatenate_with is not None:
        try:
            concatenate_dir = os.path.expandvars(lead_gen[concatenate_with])
            for dir in dir_to_concatenate:
                res_concatenate = os.path.join(concatenate_dir, dir)
                dirs.append(res_concatenate)

        except Exception as ex:
            logging.error(
                f'{ex.args} while trying to get key {concatenate_with} from Variable mip_lead_gen')

    logging.info(f'utils.get_dirs: result is {dirs}')

    return dirs


def check_and_create_dirs(pack_of_dirs):
    if not pack_of_dirs:
        logging.info('utils.check_and_create_dirs: No dirs to check')
        return

    for dir_name in pack_of_dirs:
        logging.info(f'Checking dir {dir_name}')
        if not os.path.exists(dir_name):
            logging.info(f'Creating dir {dir_name}')
            try:
                pathlib.Path(dir_name).mkdir(parents=True, exist_ok=True)
            except Exception as ex:
                logging.error(f'{ex.args} while trying to create {dir_name}')
        else:
            logging.info(f'Dir already exists {dir_name}')


def get_files_with_ext_list(path, ext=None, concat_path=True, source_name=None) -> list:
    """
    :param source_name: for regex file filter
    :param path: where to search
    :param ext: file extension (optional, if not returns all files in directory)
    :param concat_path: if true - returns 'path/filename', else just 'filename'
    :return: list
    """
    if not os.path.exists(path):
        logging.warning(
            'The specified path {path} does not exist'.format(path=path))
        return []
    match_pattern = ''
    if source_name:
        year_day_month = '\d{8}'
        time = '\d{4}'
        custom_str = '.*'
        match_pattern = f'^{source_name}_{year_day_month}_{time}_{custom_str}\.{ext}$'
    result = []
    for f in os.listdir(path):
        f_full = os.path.join(path, f)
        if os.path.isfile(f_full) and (not ext or f.endswith(ext)) \
                and (not source_name or re.search(match_pattern, f)):
            result.append(f_full if concat_path else f)
    logging.info(f'Found files: {result}')
    return result


def remove_files(pack_of_dir_to, ext=None):
    """
    remove all files with extension ext in each directory dir_to in pack, using function get_files_with_ext_list
    :param pack_of_dir_to: where remove files
    :param ext: file(s) extension, relates to function get_files_with_ext_list
    :return: none
    """
    for dir_to in pack_of_dir_to:
        if not os.path.exists(dir_to):
            logging.warning(
                'The specified path {path} does not exist and will be created'.format(path=dir_to))
            pathlib.Path(dir_to).mkdir(parents=True, exist_ok=True)

    for dir_to in pack_of_dir_to:
        for file in get_files_with_ext_list(dir_to, ext=ext, concat_path=True):
            logging.info(f'Removed file {file}')
            try:
                os.remove(file)
            except Exception as ex:
                logging.error(ex.args)
                raise Exception from ex


def copy_files(dir_from, dir_to, ext=None, rename=False, source_name=None, readme=False):
    """
    From dynprice lib
    move all files with extension ext from dir_from to
    dir_to, optionally rename them with current '%Y_%m_%d_%H_%M'
    :param readme: if true, doesnt move readme.txt file
    :param source_name: for filter files with mask
    :param dir_from: from where move
    :param dir_to: move to
    :param ext: extension of files needed to be moved
    :param rename: Boolean, for avoiding file collision
    :return: none
    """
    if not os.path.exists(dir_to):
        logging.info(
            'The specified path {path} does not exist'.format(path=dir_to))
        pathlib.Path(dir_to).mkdir(parents=True, exist_ok=True)

    for file in get_files_with_ext_list(dir_from, ext=ext, concat_path=False, source_name=source_name):
        if readme and file == 'readme.txt':
            continue
        if rename:
            fparts = file.split('.')
            dttm = datetime.datetime.now().strftime('%Y_%m_%d_%H_%M')
            name = '{0}{1}.{2}'.format(''.join(fparts[:-1]), dttm, fparts[-1])
        else:
            name = file
        try:
            shutil.copy(os.path.join(dir_from, file),
                        os.path.join(dir_to, name))
            logging.info(f'Copied file {name} from {dir_from}  to {dir_to}')
        except Exception as ex:
            logging.error(ex.args)


def write_filenames_to_txt_in_outputdir(filenames, output_dir, txt_name):
    if not os.path.exists(output_dir):
        logging.info(f'The specified path {output_dir} does not exist')
        return
    try:

        with open(os.path.join(output_dir, txt_name + '.txt'), 'a') as txt:
            for file in filenames:
                txt.write(file + '\n')
        logging.info(
            f'Written filenames {filenames} to file {output_dir + txt_name}.txt ')

    except Exception as ex:
        logging.error(
            f'{ex.args} while trying to write in file {output_dir + txt_name}.txt ')


def read_filenames_from_txt_in_outputdir(output_dir, txt_name):
    if not os.path.exists(output_dir):
        logging.info(f'The specified path {output_dir} does not exist')
        return
    if not os.path.isfile(os.path.join(output_dir, txt_name + '.txt')):
        logging.info(
            f'The specified path {output_dir} does not contain {txt_name}.txt file')
        return

    filenames = []
    try:

        with open(os.path.join(output_dir, txt_name + '.txt'), 'r') as txt:
            for line in txt:
                filenames.append(line.strip('\n'))
        logging.info(
            f'Read filenames {filenames} from file {output_dir + txt_name}.txt ')

    except Exception as ex:
        logging.warning(
            f'{ex.args} while trying to read from file {output_dir + txt_name}.txt ')

    return filenames


def pkl_check_handling(pack_of_output_dirs, exchange_dir_main, exchange_dir_loaded, exchange_dir_unloaded):
    err = False
    if not os.path.exists(exchange_dir_loaded) or not os.path.exists(exchange_dir_unloaded) \
            or not os.path.exists(exchange_dir_main):
        logging.info(
            f'The specified path {exchange_dir_loaded} does not exist')
        return

    loaded = []
    unloaded = []
    for output_dir in pack_of_output_dirs:
        parsed_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'successfully_parsed_files')
        unparsed_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'unsuccessfully_parsed_files')

        logging.info(f'Successfully parsed files: {parsed_files} ')
        logging.info(f'Unparsed files: {unparsed_files} ')

        loaded = loaded + parsed_files
        unloaded = unloaded + unparsed_files

    loaded_filename_without_dir = [os.path.basename(file) for file in loaded]
    unloaded_filename_without_dir = [
        os.path.basename(file) for file in unloaded]
    if len(unloaded_filename_without_dir):
        err = True
    for file in loaded_filename_without_dir:
        try:
            logging.info(
                f'trying to move file {os.path.join(exchange_dir_main, file)} to {exchange_dir_loaded}')
            shutil.move(os.path.join(exchange_dir_main, file),
                        exchange_dir_loaded)
            logging.info(f'Moved file {file} to {exchange_dir_loaded}')
        except Exception as ex:
            logging.warning(ex.args)

    for file in unloaded_filename_without_dir:
        try:
            logging.info(
                f'trying to move file {os.path.join(exchange_dir_main, file)} to {exchange_dir_unloaded}')
            shutil.move(os.path.join(exchange_dir_main, file),
                        exchange_dir_unloaded)
            logging.info(f'Moved file {file} to {exchange_dir_unloaded}')
        except Exception as ex:
            logging.warning(ex.args)

    return err


def check_parsed_and_loaded_files_equality(pack_of_output_dirs, exchange_dir):
    err = False
    trash = []
    err_files = []
    for output_dir in pack_of_output_dirs:
        parsed_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'successfully_parsed_files')
        loaded_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'successfully_loaded_files')

        unloaded_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'unsuccessfully_loaded_files')
        unparsed_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'unsuccessfully_parsed_files')

        logging.info(f'Successfully parsed files: {parsed_files} ')
        logging.info(f'Successfully loaded files: {loaded_files} ')

        logging.info(f'Failed to parse files: {unparsed_files} ')
        logging.info(f'Failed to load files: {unloaded_files} ')

        if unparsed_files:
            unparsed_files_with_dr = [os.path.join(
                output_dir, file) for file in unparsed_files]
            err_files = err_files + unparsed_files_with_dr
            err = err or True

        if unloaded_files:
            unloaded_files_with_dr = [os.path.join(
                output_dir, file) for file in unloaded_files]
            err_files = err_files + unloaded_files_with_dr
            trash = trash + unloaded_files_with_dr
            err = err or True

    if err:
        logging.warning(f'List of unloaded/unparsed files {err_files}')
        throw_unloaded_files_to_W(trash, exchange_dir)

    return err


def emis_pkl_check_handling(pack_of_output_dirs, exchange_dir_main, exchange_dir_loaded, exchange_dir_unloaded, input_dir):
    err = False
    if not os.path.exists(exchange_dir_loaded) or not os.path.exists(exchange_dir_unloaded) \
            or not os.path.exists(exchange_dir_main):
        logging.info(
            f'The specified path {exchange_dir_loaded} does not exist')
        return

    loaded = []
    unloaded = []
    for output_dir in pack_of_output_dirs:
        parsed_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'successfully_parsed_files')
        unparsed_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'unsuccessfully_parsed_files')

        logging.info(f'Successfully parsed files: {parsed_files} ')
        logging.info(f'Unparsed files: {unparsed_files} ')

        loaded = loaded + parsed_files
        unloaded = unloaded + unparsed_files

    loaded_filename_without_dir = [os.path.basename(file) for file in loaded]
    unloaded_filename_without_dir = [
        os.path.basename(file) for file in unloaded]
    if len(unloaded_filename_without_dir):
        err = True

    for file in unloaded_filename_without_dir:
        try:
            logging.info(
                f'trying to move file {os.path.join(input_dir, file)} to {exchange_dir_unloaded}')
            shutil.move(os.path.join(input_dir, file),
                        exchange_dir_unloaded)
            os.remove(os.path.join(input_dir, file))
            logging.info(
                f'Moved unparsed file {file} to {exchange_dir_unloaded}')
        except Exception as ex:
            logging.warning(ex.args)

    return err


def emis_check_parsed_and_loaded_files_equality(pack_of_output_dirs, exchange_dir):
    """
    return: loaded csv files and marker false/true
    """
    err = False
    trash = []
    err_files = []

    loaded_csv_files = []
    for output_dir in pack_of_output_dirs:
        parsed_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'successfully_parsed_files')
        loaded_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'successfully_loaded_files')

        unloaded_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'unsuccessfully_loaded_files')
        unparsed_files = read_filenames_from_txt_in_outputdir(
            output_dir, 'unsuccessfully_parsed_files')

        logging.info(f'Successfully parsed files: {parsed_files} ')
        logging.info(f'Successfully loaded files: {loaded_files} ')

        logging.info(f'Failed to parse files: {unparsed_files} ')
        logging.info(f'Failed to load files: {unloaded_files} ')

        if unparsed_files:
            unparsed_files_with_dr = [os.path.join(
                output_dir, file) for file in unparsed_files]
            err_files = err_files + unparsed_files_with_dr
            err = err or True

        if unloaded_files:
            unloaded_files_with_dr = [os.path.join(
                output_dir, file) for file in unloaded_files]
            err_files = err_files + unloaded_files_with_dr
            trash = trash + unloaded_files_with_dr
            err = err or True

        loaded_csv_files = loaded_csv_files + loaded_files

    if err:
        logging.warning(f'List of unloaded/unparsed files {err_files}')
        throw_unloaded_files_to_W(trash, exchange_dir)

    return err, loaded_csv_files


def throw_unloaded_files_to_W(trash_pack_with_dirs, dir_to):
    """
    copying files to dir_to
    special for returning unloaded files back to W:
    :param trash_pack_with_dirs:
    :param dir_to:
    :return:
    """
    if not os.path.exists(dir_to):
        logging.info(
            'The specified path {path} does not exist'.format(path=dir_to))
        return

    for file in trash_pack_with_dirs:
        try:
            shutil.copy(file, dir_to)
            logging.info(f'Copied file {file} to {dir_to}')
        except Exception as ex:
            logging.error(ex.args)


def divide_df(df, chunk_size):
    logging.info(f'Dividing large df into smaller dfs')
    list_of_df = list()
    try:
        number_chunks = len(df) // chunk_size + 1
        logging.info(f'number_chunks: {number_chunks}')
        for i in range(number_chunks):
            list_of_df.append(df[i * chunk_size:(i + 1) * chunk_size])
    except Exception as ex:
        logging.warning(f'{ex.args} while trying divide dataframe')

    return list_of_df


def replacer(df):
    try:
        df.replace({'-': np.nan}, inplace=True)
        logging.info('finished replacing "-" with "nan"')
    except Exception as ex:
        logging.warning(f'{ex.args} while trying to replace "-" with "nan"')

    try:
        df.replace({'NM': np.nan}, inplace=True)
        logging.info('finished replacing "NM"  with "nan"')
    except Exception as ex:
        logging.warning(f'{ex.args} while trying to replace "NM" with "nan"')

    try:
        df = df.replace(r'/\\', ' ', regex=True)
        logging.info('finished replacing "/\\" with "space"')
    except Exception as ex:
        logging.warning(
            f'{ex.args} while trying to replace "/\\" with "space"')

    try:
        df['Product Description'] = df['Product Description'].replace(
            '   ', ';', regex=True)
        df['Product Description'] = df['Product Description'].replace(
            '\n\n', ';', regex=True)
        logging.info(
            'finished replacing Product Description separator "3 spaces" with ";"')
    except Exception as ex:
        logging.warning(
            f'{ex.args} trying to replace Product Description separator "3 spaces" with ";"')

    return df


def get_list_of_intersected_files(input_dir, unloaded_dir, ext_unload):
    """
    deleting table's name and file extension, compare with file in input directory
    param input_dir: place where all files store
    param unloaded_dir: place where unloaded files store
    return list of intersected files
    """
    try:
        all_files = get_list_files(input_dir, 'xlsx')
        unloaded_files = get_list_files(unloaded_dir, ext_unload)
        intersected_files = list(set(all_files) & set(unloaded_files))
        logging.info(f'Intersected files {intersected_files}')
        return intersected_files
    except Exception as ex:
        logging.error(f'Error when comparing unloaded and input files : {ex}')


def remove_unloaded_specific_files(input_dir, unloaded_dir):
    """
    removing unloaded xlsx files from input directory
    """
    files_for_deleting = get_list_of_intersected_files(
        input_dir, unloaded_dir, 'csv')
    try:
        if files_for_deleting:
            for file in files_for_deleting:
                file = file + '.xlsx'
                f_full = os.path.join(input_dir, file)
                logging.info(
                    f'file {f_full} will be removed from input directory')
                os.remove(f_full)
        logging.info(
            f'Removing unloaded xlsx files from input directory was succssesful')
    except Exception as ex:
        logging.error(
            f'Error while removing unloaded xlsx files from input directory : {ex}')


def get_list_files(path, ext):
    """
    geting files without extension
    :param path: where to search
    :return :list of files in dirrectory
    """
    try:
        if not os.path.exists(path):
            logging.warning(
                'The specified path {path} does not exist'.format(path=path))
            return []
        result = []
        logging.info(os.listdir(path))
        if ext == 'xlsx':
            for f in os.listdir(path):
                f_full = os.path.join(path, f)
                if os.path.isfile(f_full):
                    result.append(re.sub(f'\.{ext}$', '', f))
            logging.info(f'Found files without extension: {result} in {path}')
        elif ext == 'csv':
            for f in os.listdir(path):
                f_full = os.path.join(path, f)
                if os.path.isfile(f_full):
                    result.append(re.sub(
                        f'\_companies.{ext}|\_industry_emis.{ext}|\_industry_naics.{ext}|\_key_executives\.{ext}$', '',
                        f))
            logging.info(f'Found files without extension: {result} in {path}')
        return result
    except Exception as ex:
        logging.error(
            f'Error when getting list of files without extension: {ex}')


def move_unloaded_xlsx_files(input_dir, unloaded_csv_dir, unloaded_xlsx_dir):
    """
    moving files from input directory to unloaded directory
    """
    files_for_deleting = get_list_of_intersected_files(
        input_dir, unloaded_csv_dir, ext_unload='csv')
    try:
        if files_for_deleting:
            for file in files_for_deleting:
                file = file + '.xlsx'
                f_full = os.path.join(input_dir, file)
                shutil.copy(f_full, os.path.join(unloaded_xlsx_dir, file))
                os.remove(f_full)
                logging.info(
                    f'File {f_full} was copied to unloaded directory and removed from input directory')
        logging.info(
            'All unloaded row xlsx files were succssesfuly moved to unloaded directory removed from input directory')
    except Exception as ex:
        logging.error(f'Error when moved files to unloaded directory: {ex}')


def remove_irrelevant_unloaded_files(input_dir, unloaded_csv_dir, unloaded_xlsx_dir, loaded_csv_files):
    """
    removing loaded files from unloaded directories 
    """
    files_for_deleting = get_list_of_intersected_files(
        input_dir, unloaded_xlsx_dir, ext_unload='xlsx')
    try:
        if files_for_deleting:
            for file in files_for_deleting:
                file = file + '.xlsx'
                file_full = os.path.join(unloaded_xlsx_dir, file)
                logging.info(f'file {file_full} will be removed')
                os.remove(file_full)
        logging.info(f'Removing irrelevant unloaded files was succssesful')
    except Exception as ex:
        logging.error(f'Error when removed irrelevant xlsx files : {ex}')

    unloaded_csv_files = get_files_with_ext_list(unloaded_csv_dir, ext='csv')
    try:
        unloaded_filename_without_dir = [
            os.path.basename(file) for file in unloaded_csv_files]
        for file in unloaded_filename_without_dir:
            if file in loaded_csv_files:
                file_full = os.path.join(unloaded_csv_dir, file)
                logging.info(f'file {file_full} will be removed')
                os.remove(file_full)
    except Exception as ex:
        logging.error(f'Error when irrelevant csv files: {ex}')
