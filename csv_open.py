import csv
import os
import sys
import logging
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.qparser import QueryParser
import whoosh.index as index
import uuid
import signal
import datetime
import readline
import hashlib


log = logging.getLogger(__name__)

DROP_DIR = "drop"

def setup_logging():
    log_formatter = logging.Formatter("")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)


class Timer():

    _start_time = None
    def __init__(self):
        self._start_time = datetime.datetime.now()

    def end(self):
        end_time = datetime.datetime.now()
        delta = end_time - self._start_time
        return str(delta)

# __________      .__.__       .___ ________                                             __
# \______   \__ __|__|  |    __| _/ \______ \   ____   ____  __ __  _____   ____   _____/  |_
#  |    |  _/  |  \  |  |   / __ |   |    |  \ /  _ \_/ ___\|  |  \/     \_/ __ \ /    \   __\
#  |    |   \  |  /  |  |__/ /_/ |   |    `   (  <_> )  \___|  |  /  Y Y  \  ___/|   |  \  |
#  |______  /____/|__|____/\____ |  /_______  /\____/ \___  >____/|__|_|  /\___  >___|  /__|
#         \/                    \/          \/            \/            \/     \/     \/
def add_record(index_writer, headers, row):

    document = {}

    for i in range(len(headers)):
        if row[i].strip():
            document[headers[i]] = row[i].decode("utf-8")

    # print(str(document))
    index_writer.add_document(**document)

    return None

# __________      .__.__       .___ .__            .___
# \______   \__ __|__|  |    __| _/ |__| ____    __| _/____ ___  ___
#  |    |  _/  |  \  |  |   / __ |  |  |/    \  / __ |/ __ \\  \/  /
#  |    |   \  |  /  |  |__/ /_/ |  |  |   |  \/ /_/ \  ___/ >    <
#  |______  /____/|__|____/\____ |  |__|___|  /\____ |\___  >__/\_ \
#         \/                    \/          \/      \/    \/      \/

def index_csv_file(reader, index_location):
    headers = None
    schema = Schema()
    index_doc = None
    index_writer = None
    # index_directory = os.path.join(DROP_DIR, str(uuid.uuid4().hex))
    index_directory = index_location

    if not os.path.exists(index_directory):
        os.makedirs(index_directory)

    log.info("indexing to directory: %s", index_directory)
    log.info("indexing please wait....")

    first_row = reader.next()
    headers = list(first_row)

    for header in headers:

        if " " in header:
            raise Exception("Header contains space!!")

        schema.add(header, TEXT(stored=True), glob=False)

    index_doc = create_in(index_directory, schema)
    num_documents = 0

    with index_doc.writer() as index_writer:

        for row in reader:
            add_record(index_writer, headers, row)
            num_documents = num_documents + 1

            if num_documents > 1 and num_documents % 500 == 0:
                log.info("Added documents: %d", num_documents)

    log.info("Documents added: %d", num_documents)
    return (headers, index_doc)


def md5(fname):
    md5_timer = Timer()

    hash_md5 = hashlib.md5()
    with open(fname, "rb") as f:
        for chunk in iter(lambda: f.read(4096), b""):
            hash_md5.update(chunk)

    log.info("Time taken to compute md5 of file: %s is : %s", fname, md5_timer.end())

    return hash_md5.hexdigest()


def get_headers_of_csv(input_csv_file):

    headers = None
    with open(input_csv_file, 'rb') as file_pointer:
        reader = csv.reader(file_pointer)
        first_row = reader.next()
        headers = list(first_row)

    return headers

# ___________       __
# \_   _____/ _____/  |________ ___.__.
#  |    __)_ /    \   __\_  __ <   |  |
#  |        \   |  \  |  |  | \/\___  |
# /_______  /___|  /__|  |__|   / ____|
#         \/     \/             \/
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: python csv_open.py file_name.csv query comma_seperated_headers")
        sys.exit(1)

    setup_logging()
    input_csv_file = sys.argv[1]
    raw_query = sys.argv[2]
    result_headers = sys.argv[3].split(",")

    op_timer = Timer()
    input_csv_file_full_path = os.path.abspath(input_csv_file)
    file_name_md5 = hashlib.md5(input_csv_file_full_path).hexdigest()
    file_contents_md5 = md5(input_csv_file_full_path)
    index_location = os.path.join(DROP_DIR, file_name_md5, file_contents_md5)
    log.info("Index location is: %s", index_location)

    headers = None
    index_handle = None
    if os.path.isdir(index_location) and index.exists_in(index_location):
        log.info("Reusing existing directory: %s", index_location)
        headers = get_headers_of_csv(input_csv_file)
        index_handle = index.open_dir(index_location)
    else:
        log.info("Indexing to directory: %s", index_location)
        with open(input_csv_file_full_path, 'rb') as file_pointer:
            reader = csv.reader(file_pointer)
            (headers, index_handle) = index_csv_file(reader, index_location)

    print("\n\n")

    with index_handle.searcher() as searcher:
        qp = QueryParser('default_field', schema=index_handle.schema)
        q = qp.parse(unicode(raw_query))
        results = searcher.search(q)

        for document in results:

            resp_doc_buffer = []

            for header in result_headers:
                if header in document:
                    resp_doc_buffer.append(document[header].encode("utf-8"))
                    # log.info("%s: %s", header, document[header].encode("utf-8"))
                else:
                    # log.info("%s: NA", header)
                    resp_doc_buffer.append("NA")
            # log.info("")

            print(",".join(resp_doc_buffer))

        log.info("\nTotal results: %d, Time taken: %s", len(results), op_timer.end())
