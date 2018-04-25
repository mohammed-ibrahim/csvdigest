import csv
import os
import sys
import logging
from whoosh.index import create_in
from whoosh.fields import *
from whoosh.qparser import QueryParser
import uuid
import signal
import enum


log = logging.getLogger(__name__)

DROP_DIR = "drop"

def setup_logging():
    log_formatter = logging.Formatter("")
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

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
def index_csv_file(reader):
    headers = None
    schema = Schema()
    index_doc = None
    index_writer = None
    index_directory = os.path.join(DROP_DIR, str(uuid.uuid4().hex))

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

    log.info("Documents added: %d", num_documents)
    return (headers, index_doc)


# ________                                  _____                .__
# \_____  \  __ __   ___________ ___.__.   /  _  \   ____ _____  |  | ___.__.________ ___________
#  /  / \  \|  |  \_/ __ \_  __ <   |  |  /  /_\  \ /    \\__  \ |  |<   |  |\___   // __ \_  __ \
# /   \_/.  \  |  /\  ___/|  | \/\___  | /    |    \   |  \/ __ \|  |_\___  | /    /\  ___/|  | \/
# \_____\ \_/____/  \___  >__|   / ____| \____|__  /___|  (____  /____/ ____|/_____ \\___  >__|
#        \__>           \/       \/              \/     \/     \/     \/           \/    \/

class Operation(enum.Enum):
    EXIT = 1
    SHOW_FIELDS = 2
    ALL_TERMS = 3
    TERMS = 4
    GEN_QUERY = 5
    NONE = 6
    ERROR = 7

class QueryAnalyzer():

    _successful = False
    _operation = None
    _params = []
    _q = None
    _message = None

    def __init__(self, query):
        self._q = query
        self.analyze(query)

    def param(self, index):
        return self._params[index]

    def analyze(self, original_query):
        query = original_query.lower().strip()

        if query == "exit":
            self._operation = Operation.EXIT

        elif query == "desc":
            self._operation = Operation.SHOW_FIELDS

        elif query == "all_terms":
            self._operation = Operation.ALL_TERMS

        elif query.startswith("terms"):
            self._operation = Operation.TERMS
            parts = query.split(" ")
            if len(parts) != 2:
                self._operation = Operation.ERROR
                self._message = "Usage: terms field_name"
            else:
                self._params = parts

        elif query == "":
            self._operation = Operation.NONE

        else:
            self._operation = Operation.GEN_QUERY
            self._params = query.split(" ")


#   _________                           .__         _____  .__
#  /   _____/ ____ _____ _______   ____ |  |__     /  _  \ |  |    ____   ____
#  \_____  \_/ __ \\__  \\_  __ \_/ ___\|  |  \   /  /_\  \|  |   / ___\ /  _ \
#  /        \  ___/ / __ \|  | \/\  \___|   Y  \ /    |    \  |__/ /_/  >  <_> )
# /_______  /\___  >____  /__|    \___  >___|  / \____|__  /____/\___  / \____/
#         \/     \/     \/            \/     \/          \/     /_____/
def perform_search(headers, index_doc, query):
    parts = query.split(" ", 1)
    field = parts[0]
    value = parts[1]

    with index_doc.searcher() as searcher:
        qp = QueryParser(field, schema=index_doc.schema)
        q = qp.parse(unicode(value))
        results = searcher.search(q)

        log.info("Total results: %d", len(results))

        for result in results:
            for header in headers:
                if header in result:
                    log.info("%s: %s", header, str(result[header]))
                else:
                    log.info("%s: NA", header)
    return None

# .___        __                              __  .__
# |   | _____/  |_  ________________    _____/  |_|__| ____   ____
# |   |/    \   __\/ __ \_  __ \__  \ _/ ___\   __\  |/  _ \ /    \
# |   |   |  \  | \  ___/|  | \// __ \\  \___|  | |  (  <_> )   |  \
# |___|___|  /__|  \___  >__|  (____  /\___  >__| |__|\____/|___|  /
#          \/          \/           \/     \/                    \/
def iterative_search(headers, index_doc):

    while True:
        user_input = raw_input("Enter query: ")
        parsed_query = QueryAnalyzer(user_input)
        log.info("Operation: %s", str(parsed_query._operation))
        log.info("")

        if parsed_query._operation == Operation.EXIT:
            log.info("Bye..")
            break

        if parsed_query._operation == Operation.SHOW_FIELDS:
            for header in headers:
                log.info(header)
            continue

        if parsed_query._operation == Operation.ALL_TERMS:
            with index_doc.searcher() as searcher:
                for header in headers:
                    log.info("Values for: %s", header)
                    log.info(list(searcher.lexicon(header)))
            continue

        if parsed_query._operation == Operation.NONE:
            continue

        if parsed_query._operation == Operation.ERROR:
            log.info(parsed_query._message)
            continue

        if parsed_query._operation == Operation.TERMS:
            with index_doc.searcher() as searcher:
                log.info("Values for: %s", parsed_query.param(1))
                log.info(list(searcher.lexicon(parsed_query.param(1))))
            continue

        perform_search(headers, index_doc, parsed_query._q)

    return None

def signal_handler(signal, frame):
    print 'Exiting...'
    sys.exit(0)
# ___________       __
# \_   _____/ _____/  |________ ___.__.
#  |    __)_ /    \   __\_  __ <   |  |
#  |        \   |  \  |  |  | \/\___  |
# /_______  /___|  /__|  |__|   / ____|
#         \/     \/             \/
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python csv_open.py file_name.csv")
        sys.exit(1)

    # Signal handler
    signal.signal(signal.SIGINT, signal_handler)
    setup_logging()

    input_file_name = sys.argv[1]

    headers = None
    index_doc = None
    schema = None
    with open(input_file_name, 'rb') as file_pointer:
        reader = csv.reader(file_pointer)
        (headers, index_doc) = index_csv_file(reader)

    iterative_search(headers, index_doc)

    # Open file
    # Index all contents
    # Prompt for query
    # Fire a query
