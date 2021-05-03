# Import Elasticsearch package 
from elasticsearch import Elasticsearch 
import json
import csv
import logging
from time import process_time

# Connect to the elastic cluster
es=Elasticsearch([{'host':'localhost','port':9200}])

ID = -1
def generateID():
    global ID
    ID = ID + 1
    return ID

def make_json(csvFilePath, jsonFilePath):
     
    # create a dictionary
    data = {}
     
    # Open a csv reader called DictReader
    with open(csvFilePath, encoding='utf-8') as csvf:
        csvReader = csv.DictReader(csvf)
         
        # Convert each row into a dictionary
        # and add it to data
        for rows in csvReader:
             
            # Assuming a column named 'No' to
            # be the primary key
            key = generateID()
            data[key] = rows
 
    # Open a json writer, and use the json.dumps()
    # function to dump data
    with open(jsonFilePath, 'w', encoding='utf-8') as jsonf:
        jsonf.write(json.dumps(data, indent=4))


def add_to_index(books):
    for i in range(len(books)):
        if(i%1000==0):
            logger.info(f'{i} out of {len(books)} indexed')
        #store document in elasticsearch 
        res = es.index(index='books', id=generateID(), body=books[str(i)])


def main():
    # Driver Code
    
    # Decide the two file paths according to your
    # computer system
    csv_file_path = './src/goodreads_books.csv'
    json_file_path = './src/goodreads_books.json'
    
    # Call the make_json function
    # make_json(csv_file_path, json_file_path)

    logger.info('Deleting index')
    es.indices.delete(index='books', ignore=[400, 404])          #delete index

    logger.info('Loading JSON')

    with open(json_file_path, 'r') as f:

        books = json.load(f)
    start = process_time()
    add_to_index(books)
    logger.info(f'Indexing took {process_time() - start} seconds')


if __name__ == "__main__":
    # logging setup
    es_logger = logging.getLogger('elasticsearch')
    es_logger.propagate = False
    es_logger.setLevel(logging.INFO)
    logger = logging.getLogger('mainLog')
    logger.propagate = False
    logger.setLevel(logging.INFO)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)
    
    main()
