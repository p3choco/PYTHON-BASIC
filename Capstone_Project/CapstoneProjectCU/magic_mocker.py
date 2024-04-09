#!/usr/bin/env python3
import json
import re
import sys
import os
import time
import uuid
import math
import random
import logging
from concurrent.futures import ProcessPoolExecutor
from configurator import Configurator
from file_outputter import FileOutputter

class MagicMocker:

    def __init__(self):
        configurator = Configurator()
        args = configurator.prepare_args()
        self.files_count = args.files_count
        self.data_lines = args.data_lines
        self.path_to_save_files = args.path_to_save_files
        self.file_name = args.file_name
        self.file_prefix = args.file_prefix
        self.clear_path = args.clear_path
        self.data_schema = args.data_schema
        self.multiprocessing = args.multiprocessing

        self.file_outputter = FileOutputter()

        logging.basicConfig()
        logging.getLogger().setLevel(logging.INFO)

    def fill_with_mock_data(self, schema_dict):
        logging.info('Generating mock data')
        mock_data_dict = {}
        for key,value in schema_dict.items():
            try:
                d_type, data = value.split(':')
            except ValueError:
                logging.error(f'Wrong pattern of scheme no ":" in value of dictionary')
                sys.exit(1)

            list_pattern = r'\[.*\]'
            rand_f_t_pattern = r'rand\(\d+, \d+\)'
            if d_type == 'timestamp':
                if data:
                    logging.warning(
                    """timestamp does not support any values and it will be ignored""")
                generated_value = time.time()
                mock_data_dict[key] = generated_value
            elif d_type == 'str':
                if data == 'rand':
                    mock_data_dict[key] = str(uuid.uuid4())

                elif re.fullmatch(list_pattern, data):
                    elem_list = re.findall(r"'([^']*)'", data)
                    mock_data_dict[key] = random.choice(elem_list)

                elif data == 'rand(from, to)':
                    logging.error('String data type used for rand(from, to) value')
                    sys.exit(1)

                elif not data:
                    mock_data_dict[key] = ''

                else:
                    mock_data_dict[key] = data

            elif d_type == 'int':

                if data == 'rand':
                    mock_data_dict[key] = random.randint(0, 10000)

                elif re.fullmatch(list_pattern, data):
                    elem_list = re.findall(r'\d+', data)
                    elem_list = list(map(int, elem_list))
                    mock_data_dict[key] = random.choice(elem_list)

                elif re.fullmatch(rand_f_t_pattern, data):
                    f, t = re.findall(r'\d+', data)
                    mock_data_dict[key] = random.randint(int(f), int(t))

                elif not data:
                    mock_data_dict[key] = None

                else:
                    if data.isdigit():
                        mock_data_dict[key] = int(data)
                    else:
                        logging.error(f'{data} could not be converted to int type')
                        sys.exit(1)
            else:
                logging.error('Wrong data type, possible values: int/str/timestamp')
                sys.exit(1)
        logging.info('Generating mock data ended')
        return mock_data_dict

    def get_schema_dict(self, json_string):
        logging.info('Getting schema')
        schema_pattern = r'\{.*\}'
        path_pattern = r'^.+/[^/]+\.json$'

        if re.fullmatch(schema_pattern, str(json_string)):
            try:
                schema_dict = json.loads(json_string)
                return schema_dict
            except json.JSONDecodeError as e:
                logging.error(f'Wrong pattern of scheme {e}')
                sys.exit(1)
        elif re.fullmatch(path_pattern, str(json_string)):
            try:
                with open(json_string, 'r') as file:
                    data = file.read()
                    schema_dict = json.loads(data)
                    return schema_dict
            except FileNotFoundError as e:
                logging.error(f'While using path to scheme error occured {e}')
                sys.exit(1)
        else:
            logging.error('Wrong pattern of scheme')
            sys.exit(1)

        logging.info('Getting schema ended')

    def get_mocked_data_dict(self):
        json_string = ''.join(self.data_schema)
        schema_dict = self.get_schema_dict(json_string)
        mock_data_dict = self.fill_with_mock_data(schema_dict)
        return mock_data_dict

    def manage_multiprocessing(self, multiprocessing_value):
        if multiprocessing_value < 0:
            logging.error('Multiprocessing value below zero')
        elif multiprocessing_value > os.cpu_count():
            return os.cpu_count()
        else:
            return multiprocessing_value

    def handle_file_writing(self, start_index, end_index,
                            file_prefixes, directory_path):
        for i in range(start_index, end_index):

            lines = [self.get_mocked_data_dict() for _ in range(self.data_lines)]
            file_path = ''.join([directory_path,
                                 str(file_prefixes[i]), '.jsonl'])
            print(file_path)
            self.file_outputter.write_to_file(lines, file_path)

    def run(self):

        if self.files_count < 0:
            logging.error('Files count below zero')
            sys.exit(1)
        elif self.files_count == 0:
            print(self.get_mocked_data_dict())
        else:
            file_prefixes = ['']

            if self.files_count > 1:
                if self.file_prefix == 'count':
                    file_prefixes = [str(i) for i in range(self.files_count)]
                elif self.file_prefix == 'random':
                    file_prefixes = random.sample(range(self.files_count*10), self.files_count)
                elif self.file_prefix == 'uuid':
                    file_prefixes = [str(uuid.uuid4()) for _ in range(self.files_count)]

            directory_path = self.file_outputter.handle_path(self.path_to_save_files,
                                                             self.clear_path, self.file_name)

            self.multiprocessing = self.manage_multiprocessing(self.multiprocessing)
            files_per_process = max(1, math.ceil(self.files_count/self.multiprocessing))

            with ProcessPoolExecutor(max_workers=self.multiprocessing) as executor:
                for i in range(0, self.files_count, files_per_process):
                    start_index = i
                    end_index = min(self.files_count, i + files_per_process)

                    process_args = (start_index, end_index, file_prefixes, directory_path)

                    executor.submit(self.handle_file_writing, *process_args)

def main():
    mocker = MagicMocker()
    mocker.run()

if __name__ == '__main__':
    main()


