-- hive -f AirportPicks.hql
create external table airportTrip1
(medallion string, 
hack_license string, 
vendor_id string, 
rate_code int,  
store_and_fwd_flag string,
pickup_datetime string,	
dropoff_datetime string, 
passenger_count string, 
trip_time_in_secs string, 
trip_distance int,  
pickup_longitude float, 
pickup_latitude float, 
dropoff_longitude float, 
dropoff_latitude float) 
row format delimited fields terminated by ',' location '/user/ap4095/bigData';

--Airport Pickups
INSERT OVERWRITE LOCAL DIRECTORY 'AirportPickups' SELECT * FROM airportTrip WHERE pickup_latitude < 40.651381   AND pickup_latitude > 40.640668  AND pickup_longitude < -73.776283   AND pickup_longitude > -73.794694 ;                                   