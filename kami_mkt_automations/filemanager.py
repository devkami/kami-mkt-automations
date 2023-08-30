#!/usr/bin/env python
# -*- coding: utf-8 -*-
import csv
import json
import logging
from os import listdir, remove, system, walk
from os.path import isfile, join
from os.path import split as split_filename

from dotenv import load_dotenv
from kami_logging import benchmark_with, logging_with

filemanager_logger = logging.getLogger('filemanager')

load_dotenv()


@benchmark_with(filemanager_logger)
@logging_with(filemanager_logger)
def csv_to_json(csv_file_path, json_file_path):
    json_array = []
    with open(csv_file_path, encoding='utf-8') as csvf:
        csv_reader = csv.DictReader(csvf)
        for row in csv_reader:
            json_array.append(row)

    with open(json_file_path, 'w', encoding='utf-8') as jsonf:
        json_string = json.dumps(json_array, indent=4)
        jsonf.write(json_string)


@benchmark_with(filemanager_logger)
@logging_with(filemanager_logger)
def get_file_list_from(folder_path):
    try:
        file_list = [
            folder_path + '/' + f
            for f in listdir(folder_path)
            if isfile(join(folder_path, f))
        ]
        filemanager_logger.info(
            f'Folder: {folder_path} \t Files List:{file_list}'
        )
        return file_list
    except Exception as e:
        filemanager_logger.exception('An unknow error occurred:', e)


@benchmark_with(filemanager_logger)
@logging_with(filemanager_logger)
def get_folder_list_from(rootdir):
    folders = []
    for (
        rootdir,
        dirs,
        files,
    ) in walk(rootdir):
        for subdir in dirs:
            folders.append(join(rootdir, subdir))

    return folders


@benchmark_with(filemanager_logger)
@logging_with(filemanager_logger)
def delete_old_files(old_files_path):
    old_files = get_file_list_from(old_files_path)
    if len(old_files):
        for old_file in old_files:
            old_filepath, old_filename = split_filename(old_file)
            filemanager_logger.info(
                f'deleting {old_filename} from {old_filepath}'
            )
            remove(old_file)


@benchmark_with(filemanager_logger)
@logging_with(filemanager_logger)
def create_local_folders():
    folders = get_folder_list_from('.')
    local_folders = [f.replace('./', '') for f in folders]

    if 'bkp' not in local_folders:
        system('mkdir bkp')

    if 'results' not in local_folders:
        system('mkdir results')
        system('mkdir results/xlsx')
        system('mkdir results/xlsx/uno')
        system('mkdir results/xlsx/uno/mestres')
        system('mkdir results/xlsx/uno/produtos')
        system('mkdir results/xlsx/uno/financeiro')
        system('mkdir results/xlsx/uno/diretoria')

    if 'results/xlsx' not in local_folders:
        system('mkdir results/xlsx')
        system('mkdir results/xlsx/uno')
        system('mkdir results/xlsx/uno/mestres')
        system('mkdir results/xlsx/uno/produtos')
        system('mkdir results/xlsx/uno/financeiro')
        system('mkdir results/xlsx/uno/diretoria')

    if 'mkdir results/xlsx/uno' not in local_folders:
        system('mkdir results/xlsx/uno')
        system('mkdir results/xlsx/uno/mestres')
        system('mkdir results/xlsx/uno/produtos')
        system('mkdir results/xlsx/uno/financeiro')
        system('mkdir results/xlsx/uno/diretoria')
