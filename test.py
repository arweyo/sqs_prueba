import boto3
import requests
from itertools import islice, chain, groupby
from json import loads
from operator import itemgetter
import time

def batch(iterable, size):
    sourceiter = iter(iterable)
    while True:
        batchiter = islice(sourceiter, size)
        try:
            yield chain([batchiter.__next__()], batchiter)
        except StopIteration:
            return

def transform(data):
    data_sorted = sorted(data, 
                  key = itemgetter('userId'))
                  
    dict_transform={}
    for key, value in groupby(data_sorted,
                          key = itemgetter('userId')):
        records=[]
        for k in value:
            records.append(k)
        for element in records: 
            del element['userId']     
        dict_transform[key]={'records':records}
    return dict_transform    







# sqs = boto3.client(
#     'sqs',
#     aws_access_key_id='AKIAZUTZFEHDFOY5FWFC',
#     aws_secret_access_key='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'
# )
list=[]

try:
    url = "https://jsonplaceholder.typicode.com/posts"
    r = requests.get(url)
    list=loads(r.text)
except:
    print("error al procesar")


#print(list)

for batchiter in batch(list, 10):
    print( "Batch: ")
    data_transformed=transform(batchiter)
    print(data_transformed)
    
    # for element in data_transformed:
    #     response = sqs.send_message(
    #     QueueUrl='redsocial-ws-tasks-test',
    #     DelaySeconds=1,
    #     MessageAttributes=element,
    #     )

    time.sleep(10)#10 segundos de espera
  
