import urllib2,contextlib
from datetime import datetime
from collections import OrderedDict

from pytz import timezone
import gtfs_realtime_pb2
import google.protobuf

import vehicle,alert,tripupdate

class mtaUpdates(object):

    # Do not change Timezone
    TIMEZONE = timezone('America/New_York')
    
    # feed url depends on the routes to which you want updates
    # here we are using feed 1 , which has lines 1,2,3,4,5,6,S
    # While initializing we can read the API Key and add it to the url
    feedurl = 'http://datamine.mta.info/mta_esi.php?feed_id=1&key='
    
    VCS = {1:"INCOMING_AT", 2:"STOPPED_AT", 3:"IN_TRANSIT_TO"}    
    tripUpdates = []
    alerts = []

    def __init__(self,apikey):
        self.feedurl = self.feedurl + apikey

    # Method to get trip updates from mta real time feed
    def getTripUpdates(self):
	tmp = []
        ve = []        
        
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            with contextlib.closing(urllib2.urlopen(self.feedurl)) as response:
                d = feed.ParseFromString(response.read())
        except (urllib2.URLError, google.protobuf.message.DecodeError) as e:
            print "Error while connecting to mta server " +str(e)
	
        timestamp = feed.header.timestamp
        nytime = datetime.fromtimestamp(timestamp,self.TIMEZONE)
        
        #print nytime
        global tripUpdates
        self.tripUpdates.append(feed.header)	
        
        for entity in feed.entity:
            
            #print entity.id
            
	    # Trip update represents a change in timetable
            if entity.trip_update and entity.trip_update.trip.trip_id:
                update = tripupdate.tripupdate()
                update.tripId=entity.trip_update.trip.trip_id
                update.routeId=entity.trip_update.trip.route_id
                update.startDate=entity.trip_update.trip.start_date
                update.direction=entity.trip_update.trip.trip_id[10:11]
                update.vehicleData =None
                update.futureStops = None
#                update.futureStops = str(entity.trip_update.stop_time_update)
		tmp+=[update]		
#		self.tripUpdates.append(update)                 			
            if entity.vehicle and entity.vehicle.trip.trip_id:
                v = vehicle.vehicle()
		            ##### INSERT VEHICLE CODE HERE #####
                v.tripId=entity.vehicle.trip.trip_id
                v.currentStopNumber=entity.vehicle.current_stop_sequence              
                v.currentStopId=entity.vehicle.stop_id
                v.timestamp=entity.vehicle.timestamp
                v.currentStopStatus=entity.vehicle.current_status
                ve.append(v)
	        self.tripUpdates.append(v)

            if entity.HasField('alert'):
                a = alert.alert()           
                if entity.alert.informed_entity:
                              

                    print 'has informed_entity'

                    a.alertMessage=entity.alert.header_text.translation
                    print entity.alert.informed_entity

                    print entity.alert.informed_entity[0:1]
                    print 'hello1'
                    for item in entity.alert.informed_entity:
                      print item.trip
		      a.tripId.append(item.trip.trip_id)

                    #a.routeId=str(entity.alert.informed_entity[0].route_id)
                    #print (entity.alert.informed_entity[0])[0:10]   #wrong
                                                                         
                print '\n'+'result:'     
                #print entity                
                print a.tripId
               # print a.routeId
               # print a.alertMessage
                #print a.tripId
		#self.tripUpdates.append(a)
		self.alerts.append(a)
		#### INSERT ALERT CODE HERE #####

        for item1 in tmp:
                        for item2 in ve:
                                if item1.tripId == item2.tripId:
				        item1.vehicleData = item2
                        self.tripUpdates.append(item1)

        return self.tripUpdates


    # END OF getTripUpdates method
