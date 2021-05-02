#!/usr/bin/env python
# Import Elasticsearch package 
from elasticsearch import Elasticsearch 
import json
import argparse

es=Elasticsearch([{'host':'localhost','port':9200}])      # Connect to the elastic cluster
"""
Basic script to create an empty python package containing one module
"""


class Searcher():
    
    def search(self, tag, value):
        result = self.es.search(index='books', body={
        'query':{
            'match':{
                tag:value
            }
        }})
        
        for hit in result['hits']['hits']:
            print(hit['_source']['title'])
            
    def __init__(self):
        self.es=Elasticsearch([{'host':'localhost','port':9200}])


def main():
    parser = argparse.ArgumentParser(description='Searcher')
    parser.add_argument('--tag', '-t', type=str, help='tag to search by')
    parser.add_argument('--value', '-v', type=str, help='value to search by')
    args = parser.parse_args()
    searcher = Searcher()
    searcher.search(args.tag, args.value)

if __name__ == '__main__':
    main()