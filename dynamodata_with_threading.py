# *********************************************************************************************
# Program to update dynamodb with latest data from mta feed. It also cleans up stale entried from db
# Usage python dynamodata.py
# *********************************************************************************************
import json,time,sys
from collections import OrderedDict
from threading import Thread

import boto3
from boto3.dynamodb.conditions import Key,Attr

sys.path.append('../utils')
import tripupdate,vehicle,alert,mtaUpdates,aws,mtaUpdates_vehicle,mtaUpdates_final,backup

### threding ###
import logging
import threading
import time

#logging.basicConfig(level=logging.DEBUG,format='[%(levelname)s] (%(threadName)-10s) %(message)s',)

### YOUR CODE HERE ####

import urllib2,contextlib
from datetime import datetime
from collections import OrderedDict
import calendar
import datetime
from pytz import timezone
import gtfs_realtime_pb2
import google.protobuf

import boto
import boto.dynamodb2
import time

from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
from boto.dynamodb2.items import Item



ACCOUNT_ID = '758957437187'
IDENTITY_POOL_ID = 'us-east-1:57e76a23-719b-4e4e-b063-494ddb08f7e5'
ROLE_ARN = 'arn:aws:iam::758957437187:role/Cognito_edisonDemoKinesisUnauth_Role'

# Use cognito to get an identity.
cognito = boto.connect_cognito_identity()
cognito_id = cognito.get_id(ACCOUNT_ID, IDENTITY_POOL_ID)
oidc = cognito.get_open_id_token(cognito_id['IdentityId'])
 
# Further setup your STS using the code below
sts = boto.connect_sts()
assumedRoleObject = sts.assume_role_with_web_identity(ROLE_ARN, "XX", oidc['Token'])
DYNAMODB_TABLE_NAME = 'MTA'
# Prepare DynamoDB client
client_dynamo = boto.dynamodb2.connect_to_region(
    'us-east-1',
    aws_access_key_id=assumedRoleObject.credentials.access_key,
    aws_secret_access_key=assumedRoleObject.credentials.secret_key,
    security_token=assumedRoleObject.credentials.session_token)
 
from boto.dynamodb2.table import Table
table_dynamo = Table(DYNAMODB_TABLE_NAME, connection=client_dynamo)

KINESIS_STREAM_NAME = 'edisonDemoKinesis'

# Prepare Kinesis client
client_kinesis = boto.connect_kinesis(
    aws_access_key_id=assumedRoleObject.credentials.access_key,
    aws_secret_access_key=assumedRoleObject.credentials.secret_key,
    security_token=assumedRoleObject.credentials.session_token)

##################################################################################table

tables = client_dynamo.list_tables() 
if DYNAMODB_TABLE_NAME not in tables['TableNames']:
    print DYNAMODB_TABLE_NAME+' not found, creating table, wait 15s'
    users = Table.create(DYNAMODB_TABLE_NAME, schema=[
       HashKey('tripId'), # defaults to STRING data_type
       RangeKey('timeStamp'),],connection=client_dynamo)
    time.sleep(15)
else:
    users = Table(DYNAMODB_TABLE_NAME, schema=[
       HashKey('tripId'),
       RangeKey('timeStamp'),
       ],connection=client_dynamo)  
    print DYNAMODB_TABLE_NAME+' selected' 

def task1():
  try:  
    while True:
    #############################task1: running every 30 sec, shall look at adding data to the "mtadata" DynamoDB table continuously
      #logging.debug('Starting')  
      mta = backup.mtaUpdates('9677454425d764f551397579a52aa866')
      result = mta.getTripUpdates()
      
      print result[0].timestamp
      print 'start saving data'
      i=1
      for items in result:
      
      
          if hasattr(items, 'routeId'):
              print 'update'
                              
              searchtrains = users.query_2(tripId__eq=items.tripId, timeStamp__eq=str(result[0].timestamp))
              true=1;
              for train in searchtrains:
                  true=0;
                  #print train['tripId']
                  if train['tripId'] == items.tripId:
                      print 'tripId aleady exit:'+train['tripId']+' '+train['timeStamp']+', updating item...'
                      itemsave=users.get_item(tripId=items.tripId, timeStamp=str(result[0].timestamp))
                      itemsave['routeId']= items.routeId
                      itemsave['startDate']= items.startDate
                      itemsave['direction']= items.direction
                      #itemsave['vehicleData']= items.vehicleData
                      itemsave['futureStops']= items.futureStops
                      itemsave.save()
                      
              if(true):  
                    itemsave = Item(users, data={
                   'tripId':items.tripId,
                   'timeStamp': str(result[0].timestamp),
                   'routeId': items.routeId,
                   'startDate': items.startDate,
                   'direction': items.direction,
                   'vehicleData': items.vehicleData,
                   'futureStops': items.futureStops,
                   })
                    itemsave.save()              
                    
              #print('write tripupdate success!')
              
              
          elif hasattr(items, 'currentStopNumber'):
              print 'vehicle'
      
              searchtrains = users.query_2(tripId__eq=items.tripId, timeStamp__eq=str(result[0].timestamp))
              true=1;
              for train in searchtrains:
                  true=0;
                  #print train['tripId']
                  if train['tripId'] == items.tripId:
                      print 'tripId aleady exit:'+train['tripId']+' '+train['timeStamp']+', updating item...'
                      itemsave=users.get_item(tripId=items.tripId, timeStamp=str(result[0].timestamp))                           
                      itemsave['currentStopNumber']= items.currentStopNumber
                      itemsave['currentStopId']= items.currentStopId
                      itemsave['vehicle_timestamp']= items.timestamp
                      itemsave['currentStopStatus']= items.currentStopStatus 
                      itemsave.save()       
              if(true):
                  itemsave = Item(users, data={
                 'tripId':items.tripId,
                 'timeStamp': str(result[0].timestamp),
                 'currentStopNumber': items.currentStopNumber,
                 'currentStopId': items.currentStopId,
                 'vehicle_timestamp': items.timestamp,
                 'currentStopStatus': items.currentStopStatus,               
                 })
          
                  itemsave.save()                
              #print('write vehicle success!')        
          #else:
              #print 'no'
                    
          #print i 
          #i+=1
      print 'write success'
      time.sleep(30)    
      #logging.debug('Exiting')    
  except KeyboardInterrupt:
                   exit

def task2():
#############################task2: running every 60 sec, looks at cleaning out data from the table that is older than 2 minutes old
  try:  
    while True:
      #logging.debug('Starting')
      print 'deleting old data'
      d = datetime.datetime.utcnow()
      POSIXnow = calendar.timegm(d.timetuple())
      print POSIXnow
      
      recent = users.scan()
      
      for user in recent:
          if int(user['timeStamp'])<(POSIXnow-60):
              #print user['tripId']+' '+user['timeStamp']+' '+user['direction']+' '+user['routeId']+' '+user['startDate']+' '+str(user['currentStopId'])+' '+str(user['currentStopNumber'])+' '+str(user['currentStopStatus'])+' '+str(user['vehicle_timestamp'])
              users.delete_item(tripId=user['tripId'],timeStamp=user['timeStamp'])
      time.sleep(60)
                
      #logging.debug('Exiting')     
  except  KeyboardInterrupt:
                   exit



t_task1 = threading.Thread(name='task1', target=task1)
t_task2 = threading.Thread(name='task2', target=task2)

t_task1.start()
t_task2.start()







