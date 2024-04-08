import glob
import os.path
import json
import sys
import logging

class FileOutputter:

    def handle_path(self, path, clear_path, file_name):
        if path == ".":
            path = os.getcwd()

        if not os.path.exists(path):
            logging.error(f'Given file_path does not exist {path}')
            sys.exit(1)

        if not os.path.isdir(path):
            logging.error('Given path_to_save_files exist but is not a directory')
            sys.exit(1)

        if clear_path:
            logging.info(f'Searching for files to delete in: {path} with pattern: {file_name}')
            pattern = os.path.join(path, file_name + '*.jsonl')
            matching_files = glob.glob(pattern)
            if not matching_files:
                logging.info('No matching files found.')
            for file_path in matching_files:
                os.remove(file_path)
                logging.info(f'LOG: file {file_path} deleted')

        return path + '/' + file_name

    def write_to_file(self, lines, file_path):
        logging.info(f'Writing mock data to file {file_path}')
        with open(file_path, 'w') as file:
            for line in lines:
                json.dump(line, file)
                file.write('\n')
        logging.info(f'Writing mock data to file {file_path} successful')
