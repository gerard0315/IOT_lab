# *********************************************************************************************
# Program to update dynamodb with latest data from mta feed. It also cleans up stale entried from db
# Usage python dynamodata.py
# *********************************************************************************************
import json,time,sys
from collections import OrderedDict
from threading import Thread

#import boto3
#from boto3.dynamodb.conditions import Key,Attr

sys.path.append('../utils')
import tripupdate,vehicle,alert,mtaUpdates,aws

### YOUR CODE HERE ####
import boto 
#import boto.dynamodb2
import time
from time import gmtime, strftime
import threading

from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.items import Item

def connect():
    ACCOUNT_ID = '299738300456'
    IDENTITY_POOL_ID = 'us-east-1:6ad049a2-48dc-4da4-94c3-58767ee26a75'
    ROLE_ARN = 'arn:aws:iam::299738300456:role/Cognito_edisonDemoKinesisUnauth_Role'

    # Use cognito to get an identity.
    cognito = boto.connect_cognito_identity()
    cognito_id = cognito.get_id(ACCOUNT_ID, IDENTITY_POOL_ID)
    oidc = cognito.get_open_id_token(cognito_id['IdentityId'])

    # Further setup your STS using the code below
    sts = boto.connect_sts()
    assumedRoleObject = sts.assume_role_with_web_identity(ROLE_ARN, "XX", oidc['Token'])

    # Prepare DynamoDB client
    # Prepare DynamoDB client
    client_dynamo = boto.dynamodb2.connect_to_region(
        'us-east-1',
        aws_access_key_id=assumedRoleObject.credentials.access_key,
        aws_secret_access_key=assumedRoleObject.credentials.secret_key,
        security_token=assumedRoleObject.credentials.session_token)

    tables = client_dynamo.list_tables()
    #if table 'Temperature' not exist, create it,
    # otherwise call Table() function to get it
    if 'mtadata' not in tables['TableNames']:
        mtaTable = Table.create('mtadata', schema=[
            HashKey('tripId'),
        
        ], throughput={
            'read':5,
            'write':5,
        }, global_indexes=[
            GlobalAllIndex('EverythingIndex', parts=[
                    HashKey('TimeStamp',data_type=NUMBER), 
            ],
        throughput={
                    'read':1,
                    'write':1,
            })
        ],connection=client_dynamo)
        
    else:
        mtaTable = Table('mtadata', connection=client_dynamo)
    
    return mtaTable


def task1(mtaTable, mtaUpdateObj):
    try:
        while(1):
            print 'task 1'
            updates = mtaUpdateObj.getTripUpdates()
            # Check whether table is ready or not
            for update in updates:
                if hasattr(update,'vehicleData'):
                    if hasattr(update.vehicleData,'currentStopId'):
                        Id = update.vehicleData.currentStopId
                    if hasattr(update.vehicleData,'currentStopStatus'):
                        status = update.vehicleData.currentStopStatus
                    if hasattr(update.vehicleData,'timestamp'):
                        timeS = update.vehicleData.timestamp
                else:
                    Id = None
                    status = None
                    timeS = None        
                item = Item(mtaTable,
                    data={
                        'tripId': update.tripId,
                        'routeId': update.routeId,
                        'TimeStamp':time.time(),
                        'startDate': update.startDate,
                        'direction': update.direction,
                        'currentStopId': Id,
                        'currentStopStatus': status,
                        'vehicleTimeStamp': timeS,
                        'futureStopData': update.futureStops,
                        'timeStamp': update.time       
                    }
                )
                item.save(overwrite=True)
            time.sleep(30)
    except KeyboardInterrupt:
        print "Detect terminating message from keyboard. Going to shutdown task #1."
        exit
    

def task2(mtaTable):
    try:
        while(1):
            print 'task 2'
            responses = mtaTable.scan(
                TimeStamp__lte=(time.time()-120)
            )
            for resp in responses:
                res = table.delete_item(tripId=resp['tripId'])
            time.sleep(60)
    except KeyboardInterrupt:
        print "Detect terminating message from keyboard. Going to shutdown task #2."
        exit

if __name__ == "__main__":
    mtaUpdateObj = mtaUpdates.mtaUpdates('20778f9857669c6fdf7fbd4e4f07fd30')
    table = connect()
    t1 = threading.Thread(target=task1, args=(table,mtaUpdateObj))
    t1.start()
    t2 = threading.Thread(target=task2, args=(table,))
    t2.start()

