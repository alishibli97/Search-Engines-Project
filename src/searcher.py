#!/usr/bin/env python
# Import Elasticsearch package 
from elasticsearch import Elasticsearch
import json
import argparse
import logging
from time import process_time
from collections import defaultdict
from math import sqrt


class Searcher():

    def __init__(self):
        self.es=Elasticsearch([{'host':'localhost','port':9200}])
        self.results = defaultdict(int)
        self.already_read = []
        self.search_weights = {
            'tags' : 4
            , 'name' : 1
            , 'author' : 1
            , 'abstract' : 10
        }
        self.preferred_books = {}
        self.origin_list = ''
        self.from_same_list = []

    def sort_results(self):
        # re-sort dict on calulated score
        return dict(sorted(self.results.items(), key=lambda item: item[1], reverse=True))

    def search(self, query, metric, weight, book_id=None):
        normalize_factor = 0
        if metric == 'author':
            res = self.es.indices.analyze(index='books', body={
                "analyzer" : "english",
                "text" : query
            })
            
            temp_list = []
            for token in res['tokens']:
                temp_list.append(token['token'])

            query = " ".join(temp_list)
        
            temp_result = self.es.search(index='books', size='10', body=
                { 'query':
                    { 'terms':
                        { metric: [t for t in temp_list] }
                    }
                }
            )
        else:
            temp_result = self.es.search(index='books', size='10', body=
                { 'query':
                    { 'match':
                        { metric:query }
                    }
                }
            )
        logger.info('\n'*2)
        logger.info(f'Hits for "{query}" in "{metric}"')
        logger.info('\n')
        for hit in temp_result['hits']['hits']:
            normalize_factor += hit['_score']
        for hit in temp_result['hits']['hits']:
            if hit['_source']['book_id'] != book_id and hit['_source']['name'].strip() not in self.already_read:
                if hit['_source']['bigger_list'].split('.')[-1] == self.origin_list:
                    self.from_same_list.append(hit['_source']['name'])
                score = hit['_score']/normalize_factor
                logger.info('{0:<60} {1:>8}'.format(hit['_source']['name'], score))
                if self.results[hit['_source']['name']] == 0:
                    # score calculated as: es_score * given_weight * average_rating * sqrt(number_of_ratings)
                    self.results[hit['_source']['name']] += score * float(hit['_source']['average_rating']) * int(hit['_source']['number_of_ratings'])**(1/8)

    def read_and_score_books(self, book_list_file):
        # add the books from the file to self.already_read
        with open(book_list_file) as f:
            for title in f:
                self.already_read.append(title.strip())

        # query the index wit
        with open(book_list_file) as f:
            for title in f:
                title = title.strip()
                res = self.es.search(index='books', size='1', body={
                    'query':{
                        'match':{
                            'name':title
                        }
                    }
                })
                logger.info('\n'*2)
                self.origin_list = res['hits']['hits'][0]['_source']['bigger_list'].split('.')[-1]
                if len(res['hits']['hits']) > 0:
                    logger.info(f'Found "{title}" in database')
                    for metric, weight in self.search_weights.items():
                        query = res['hits']['hits'][0]['_source'][metric].replace('\'', '').replace('{', '').replace('}', '')
                        if query != 'set()':
                            self.search(query, metric, weight, res['hits']['hits'][0]['_source']['book_id'])
                else:
                    logger.info(f'"{title}" was not found in database')


def main(args):
    searcher = Searcher()
    if args.book_list != None:
        searcher.read_and_score_books(args.book_list)
    start = process_time()
    if args.query != None:
        for metric, weight in searcher.search_weights.items():
            searcher.search(args.query, metric, weight)
        logger.info('\n'*2)
        logger.info(f'Query took {(process_time() - start):.3f} seconds to complete')
    logger.info('{:^70}'.format('FINAL RESULTS'))
    logger.info('{0:<65} {1:>8}'.format('BOOK_TITLE', 'TOTAL_SCORE'))
    logger.info('{0:<65} {1:>8}'.format('----------', '-----------'))
    for k,v in searcher.sort_results().items():
        k = k[:60]+'...' if len(k)>60 else k
        logger.info('{0:<65} {1:>8.3f}'.format(k,v))
    logger.info([book for book in searcher.from_same_list])

if __name__ == '__main__':
    # logging setup
    logger = logging.getLogger('mainLog')
    logger.propagate = False
    logger.setLevel(logging.INFO)
    consoleHandler = logging.StreamHandler()
    consoleHandler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s: - %(message)s')
    consoleHandler.setFormatter(formatter)
    logger.addHandler(consoleHandler)

    # parse arguments
    parser = argparse.ArgumentParser(description='Searcher')
    # parser.add_argument('--tag', '-t', type=str, help='tag to search by')
    # parser.add_argument('--value', '-v', type=str, help='value to search by')
    parser.add_argument('--book_list', '-b', type=str, help='list of books I enjoy')
    parser.add_argument('--query', '-q', type=str, help='search query')
    args = parser.parse_args()

    main(args)