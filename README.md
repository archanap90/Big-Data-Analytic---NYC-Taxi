# Big-Data Analytics on NYC Taxi data


### Analytic 1:  
Counts the number of Trip from source to destination and then uses PIG Query to find the most frequent trips from one source to destination. 
Technology components: Map Reduce and PIG

Things we considered to count number of trips:
We had to know the nearby locations to group them as a single source, we achieved it by rounding off the latitude and longitude co-ordinates. Locations that are close are grouped together because most of the digits match in their coordinates.
ex : -73.776243,40.651281 and  -73.776240,40.651280 are near by locations . They only differ in the last digit

### Analytic 2: 
Suggesting restaurants based on the most taxi trips made during the mealtime. Idea is to get a sense of places visited most in these time intervals.  We have used Pig to segregate data into lunch, breakfast and dinner timings.
This Analytic uses Map Reduce to get results, Mapper takes key as the time range and dropoff location as the value . Most visited places from this analytic could be a good place to open a new restaurant.

### Analytic 3: 
Ride Sharing from Airport. As a pre-analytic we are using HIVE to get all the trips that are starting and ending at Airport location (JFK) with a buffer of 1 hour and 30 min.
Hive is used to get the trips that involves airport as destination or source. Then we ran Map Reduce on that to get trips that happened for each 30 min buffer. This gave the idea of how many trips are happening to near by locations thus helping to determine whether ride share is possible or not. These insights could be a good place to advertise Ride Share apps as well.   

### Analytic 4:  
Run MapReduce on both the taxi and the flight data. In the taxi data we are filtering all the data based on the drop-off or pickup locations as the JFK airport for each cab. In the flight data we are grouping all the data in an hourly basis i.e. counting the number of flights for each hour, for every day.
We create two external tables using the above outputs and each table has the columns date and time. Using these common columns we can perform join on two tables and get the count of flights for a cab at the time. 
This count gives the respective cab the number of flights that are going to land, thus recommending him/her to pickup a passenger or leave.
