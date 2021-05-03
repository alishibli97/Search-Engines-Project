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
        self.search_weights = {
            'tags' : 0.25
            , 'name' : 0.25
            , 'author' : 0.25
            , 'abstract' : 0.25
        }

    def search(self, query):
        result = defaultdict(int)

        for metric, weight in self.search_weights.items():
            temp_result = self.es.search(index='books', size='10', body={
                'query':{
                    'match':{
                        metric:query
                    }
                }
            })
            logger.info(f'Hits for "{query}" in "{metric}"')
            for hit in temp_result['hits']['hits']:
                # logging
                metric_value = hit['_source'][metric][:100]+'...' if len(hit['_source'][metric])>100 else hit['_source'][metric]
                logger.info('{0:<10} {1:<105} {2:>8}'.format(hit['_source']['book_id'],  metric_value, hit['_score']))
                
                # score calculated as: es_score * given_weight * average_rating * sqrt(number_of_ratings)
                result[hit['_source']['name']] += hit['_score'] * weight * float(hit['_source']['average_rating']) * sqrt(int(hit['_source']['number_of_ratings']))

        return dict(sorted(result.items(), key=lambda item: item[1], reverse=True)) # re-sort dict on calulated score


def main(args):
    searcher = Searcher()
    start = process_time()
    res = searcher.search(args.query)

    logger.info(f'Query took {(process_time() - start):.3f} seconds to complete')
    logger.info('{:^70}'.format('FINAL RESULTS'))
    logger.info('{0:<60} {1:>8}'.format('BOOK_TITLE', 'TOTAL_SCORE'))
    logger.info('{0:<60} {1:>8}'.format('----------', '-----------'))
    for k,v in res.items():
        logger.info('{0:<60} {1:>8.3f}'.format(k,v))


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
    parser.add_argument('--query', '-q', type=str, help='search query')
    args = parser.parse_args()

    main(args)