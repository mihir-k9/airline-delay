import os
import logging

logger = logging.getLogger('etl')

DIR_NAME = os.getcwd()
PARENT_DIR = os.path.abspath(os.path.join(DIR_NAME, os.pardir))
