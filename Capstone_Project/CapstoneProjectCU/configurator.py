import argparse
import configparser
import logging

class Configurator:

    def __init__(self):
        self.args = self.prepare_args()

    def save_config(self):
        logging.info('Saving config')
        config = configparser.ConfigParser()
        config['DEFAULT'] = {'path_to_save_files': '.',
                             'files_count': '1',
                             'file_name': 'basic_file',
                             'file_prefix': 'random',
                             'data_schema': """
                             {\"date\": \"timestamp:abc\",\"name\": \"str:rand\",\"type\": \"str
                             :['client', 'partner', 'government']\",\"age\":\"int:rand(1, 90)\"}
                             """,
                             'data_lines': '10',
                             'multiprocessing': '1',
                             }
        with open('default.ini', 'w') as configfile:
            config.write(configfile)
        logging.info('Saving config ended')

    def read_configparser(self):
        logging.info('Reading Default Config')
        config = configparser.ConfigParser()
        config.read('default.ini')
        logging.info('Reading Default Config successful')
        return config

    def prepare_args(self):
        logging.info('Getting arguments')
        config = self.read_configparser()
        parser = argparse.ArgumentParser(prog='MagicMocker')
        parser.add_argument('--path_to_save_files',
                            default=config['DEFAULT']['path_to_save_files'],
                            help='Where all files need to save.\
                                Default: %(default)s')

        parser.add_argument('--files_count',
                            default=config['DEFAULT']['files_count'],
                            type=int,
                            help='How much json files to generate.\
                                Default: %(default)s')

        parser.add_argument('--file_name',
                            default=config['DEFAULT']['file_name'],
                            help="""Base file_name.
                                          If there is no prefix, the final\
                                           file name will be file_name.json.
                                          With prefix full file name will\
                                           be file_name_file_prefix.json
                                          Default: %(default)s""")

        parser.add_argument('--file_prefix',
                            default=config['DEFAULT']['file_prefix'],
                            choices=['count', 'random', 'uuid'],
                            help='What prefix for file name to use if\
                                more than 1 file needs to be generated.\
                                 Default: %(default)s')

        parser.add_argument('--data_schema',
                            default=config['DEFAULT']['data_schema'],
                            help="""It’s a string with json schema.
                                          It could be loaded in two ways: 
                                          1) With path to json file with schema 
                                          2) with schema entered to command line.
                                          Default: %(default)s""")

        parser.add_argument('--data_lines',
                            default=config['DEFAULT']['data_lines'],
                            type=int,
                            help='Count of lines for each file. Default: %(default)s')

        parser.add_argument('--clear_path',
                            default=config['DEFAULT'].getboolean('clear_path'),
                            action='store_true',
                            help="""If this flag is on, before the script starts\
                                creating new data files, all files in path_to_save_files\
                                 that match file_name will be deleted. Default: %(default)s""")

        parser.add_argument('--multiprocessing',
                            default=config['DEFAULT']['multiprocessing'],
                            type=int,
                            help="""The number of processes used to create files.
                                          Divides the “files_count” value equally and\
                                           starts N processes to create an equal number\
                                            of files in parallel.
                                          Optional argument.\
                                           Default value: 1. Default: %(default)s""")
        logging.info('Getting arguments ended')
        return parser.parse_args()
