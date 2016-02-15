import urllib,urllib2,contextlib
import sys
import gtfs_reatime_pb2
import google.protobuf
from datetime import datetime
from collections import OrderedDict
from pytz import timezone
import vehicle,alert,tripupdate

class mtaUpdates(object):

    # Do not change Timezone
    TIMEZONE = timezone('America/New_York')

    # feed url depends on the routes to which you want updates
    # here we are using feed 1 , which has lines 1,2,3,4,5,6,S
    # While initializing we can read the API Key and add it to the url
    feedurl = 'http://datamine.mta.info/mta_esi.php?feed_id=1&key='
    #apikey = '78b884010b517f078fdaeb9345beac62'

    VCS = {1:"INCOMING_AT", 2:"STOPPED_AT", 3:"IN_TRANSIT_TO"}
    tripUpdates = []
    vehiclesInfo = []
    alerts = []

    def __init__(self,apikey):
        self.feedurl = self.feedurl + apikey

    # Method to get trip updates from mta real time feed
    def getTripUpdates(self):
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            with contextlib.closing(urllib2.urlopen(self.feedurl)) as response:
                d = feed.ParseFromString(response.read())
        except (urllib2.URLError, google.protobuf.message.DecodeError) as e:
            print "Error while connecting to mta server " +str(e)
        vehicle_ctr, alert_ctr, trip_ctr=0,0,0

        for entity in feed.entity:
                if entity.HasField('trip_update'):
                #print entity
                        trip_ctr=trip_ctr+1
                if entity.HasField('alert'):
                        #print entity
                        alert_ctr=alert_ctr+1
                if entity.HasField('vehicle'):
                        #print entity
                        vehicle_ctr=vehicle_ctr+1

        print "Trip Updates: ", trip_ctr
        print "Alerts: ",    alert_ctr
        print "Vehicle Position Updates: ",vehicle_ctr

        timestamp = feed.header.timestamp
        nytime = datetime.fromtimestamp(timestamp,self.TIMEZONE)
        print timestamp
        for entity in feed.entity:
            # Trip update represents a change in timetable
        for entity in feed.entity:
            # Trip update represents a change in timetable
            if entity.trip_update:
                #and entity.trip_update.trip.trip_id:
                updates = entity.trip_update
                #print "check1"
                trip_id_ = entity.trip_update.trip.trip_id
                tripUpdates.append([trip_id_: {
                    "tripId": trip_id,
                    "routeId": entity.trip_update.trip.route_id,
                    "startDate": entity.trip_update.trip.start_date,
                    #"stopId": entity.trip_update.stop_time_update.stop_id
                    "futureStopData":{entity.trip_update.stop_time_update.stopid:["arrivaltime" = entity.trip_update.stop_time_update.arrive.time,
                    "departuretime" = entity.trip_update.stop_time_update.departure.time]}
                }])


            if entity.vehicle and entity.vehicle.trip.trip_id:
                v = entity.vehicle
                trip_id_ = entity.vehicle.trip.trip_id
                vehiclesInfo.append(trip_id_:{
                    "tripId":trip_id,
                    "routeId":entity.vehicle.trip.route_id,
                    "startDate":entity.vehicle.trip.start_date,
                    "currentStopId":entity.vehicle.stop_id,
                    "currentStopStatus":entuty.vehicle.current_status,
                    "vehicleTimeStamp": entity.vehible.timestamp
                })

            if entity.alert:
                alert = entity.alert
                print "check3"
                #### INSERT ALERT CODE HERE ####


        return self.tripUpdates
        return self.vehicleInfo


    # END OF getTripUpdates method


if __name__ == "__main__":
    apikey = '78b884010b517f078fdaeb9345beac62'
    mtaData = mtaUpdates(apikey)
    mtaData.getTripUpdates()