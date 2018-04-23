import csv
import os
import sys
import logging
log = logging.getLogger(__name__)


def setup_logging():
    log_formatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    # logger_file_name = os.path.join(LOG_DIR, "importer.log")
    # file_handler = logging.FileHandler(logger_file_name)
    # file_handler.setFormatter(log_formatter)
    # root_logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

def add_record(headers, row):
    return None

def index_csv_file(reader):
    headers = None

    for row in reader:
        if headers is None:
            headers = list(row)
        else:
            add_record(headers, row)

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python csv_open.py file_name.csv")
        sys.exit(1)

    setup_logging()

    input_file_name = sys.argv[1]

    with open(input_file_name, 'rb') as file_pointer:
        reader = csv.reader(file_pointer)
        index_csv_file(reader)
    # Open file
    # Index all contents
    # Prompt for query
    # Fire a query
