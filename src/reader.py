# Import Elasticsearch package 
from elasticsearch import Elasticsearch 
import json
# Connect to the elastic cluster
es=Elasticsearch([{'host':'localhost','port':9200}])

ID = 0
def generateID():
    global ID
    ID = ID + 1
    return ID

es.indices.delete(index='books', ignore=[400, 404])          #delete index

with open('books.txt', 'r') as f:
    books = json.load(f)
        

for i in range(len(books)):
    #store document in elasticsearch 
    res = es.index(index='books', id=generateID(), body=books[i])
