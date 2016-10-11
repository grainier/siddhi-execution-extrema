## siddhi-extension-minbymaxby

###minByLength

####<event> minByLength(<int|long|double|float|string> parameter, <int|long|time> windowLength)

####    Extension Type: Window
####    Description: Sliding length window, that holds an event which has minimum value of given attribute within last windowLength events, and gets updated on every event arrival and expiry.
####    Parameter:parameter:  The parameter that need to be compared to find the event which has min value.
####	      windowLength: The number of events that need to should be in a sliding length window.
####    Return Type: Return current and expired events.
####    Examples:minByLength(temp) returns the event which has minimum temp value recorded for all the events based on their arrival and expiry.
####
#### maxByLength has similar behaviour but it returns an event which has minimum value of given attribute within last windowLength events, and gets updated on every event arrival and expiry.
#
###maxBylengthBatch
#
####<event> maxBylengthBatch(<int|long|double|float|string> parameter, <int> windowLength)
#
####    Extension Type: Window
####    Description: Batch (tumbling) length window, that holds an event which has maximum value of given attribute up to windowLength events, and gets updated on every windowLength event arrival.
####    Parameter:  parameter:  The parameter that need to be compared to find the event which has max value.
####		windowLength: For the number of events the window should tumble.
####    Return Type: Return current and expired events.
####   Examples: minByLengthBatch(temp) returns the event which has maximum temp value recorded for all the events based on their arrival and expiry.
#
#### minBylengthBatch has similar behaviour but it returns an event which has maximum value of given attribute up to windowLength events, and gets updated on every windowLength event arrival.
#
###minBytime
#
#
####<event> minByTime(<int|long|double|float|string> parameter, <int|long|time> windowTime)
####    Extension Type: Window
####    Description: Sliding time window, that holds an event which has minimum value of given attribute for that arrived during last windowTime period, and gets updated on every event arrival and expiry.
####    Parameter:  parameter:  The parameter that need to be compared to find the event which has min value.
####		windowTime: The sliding time period for which the window should hold events.
####    Return Type: Return current and expired events.
####    Examples: minByTime(temp) returns the event which has minimum temp value recorded for all the events based on their arrival time and expiry.
#
#### maxByTime has similar behaviour but it returns an event which has minimum value of given attribute for that arrived during last windowTime period, and gets updated on every event arrival and expiry.
#
### maxBytimeBatch
#
####<event> maxBytimeBatch(<int|long|double|float|string> parameter, <int|long|time> windowTime)
#
####   Extension Type: Window
####   Description: Batch (tumbling) time window, that holds an event which has maximum value of given attribute arrived between windowTime periods, and gets updated for every windowTime.
####    Parameter: parameter:  The parameter that need to be compared to find the event which has max value.
####	       windowTime: The batch time period for which the window should hold events.
####    Return Type: Return current and expired events.
####    Examples:  minByTimeBatch(temp) returns the event which has maximum temp value recorded for all the events based on their arrival time and expiry.
#
#### maxByTimeBatch has similar behaviour but it returns an event which has maximum value of given attribute arrived between windowTime periods, and gets updated for every windowTime.
#
#
#