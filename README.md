# siddhi-extension-minbymaxby

#minByLength

#<event> minByLength(<int|long|double|float|string> parameter, <int|long|time> windowLength)
#
#   Extension Type: Window
#    Description: Sliding length window, that holds an event which has minimum value of given attribute within last windowLength events, and gets updated on every event arrival and expiry.
#   Parameter:parameter:  The parameter that need to be compared to find the event which has min value.
#	      windowLength: The number of events that need to should be in a sliding length window.
#    Return Type: Return current and expired events.
#    Examples:minByLength(temp) returns the event which has minimum temp value recorded for all the events based on their arrival and expiry.
#
#maxByLength
#
#<event> maxByLength(<int|long|double|float|string> parameter, <int|long> windowLength)
#
#    Extension Type: Window
#    Description: Sliding length window, that holds an event which has maximum value of given attribute within last windowLength events, and gets updated on every event arrival and expiry.
#    Parameter: parameter:  The parameter that need to be compared to find the event which has max value.
#	       windowLength: The number of events that need to should be in a sliding length window.
#    Return Type: Return current and expired events.
#    Examples:minByLength(temp) returns the event which has maximum temp value recorded for all the events based on their arrival and expiry.
#
#minBylengthBatch

#<event> lengthBatch(<int|long|double|float|string> parameter, <int> windowLength)

#    Extension Type: Window
#    Description: Batch (tumbling) length window, that holds an event which has minimum value of given attribute up to windowLength events, and gets updated on every windowLength event arrival.
#    Parameter:  parameter:  The parameter that need to be compared to find the event which has min value.
#		windowLength: For the number of events the window should tumble.
#    Return Type: Return current and expired events.
#    Examples: minByLengthBatch(temp) returns the event which has minimum temp value recorded for all the events based on their arrival and expiry.
#
#maxBylengthBatch
#
#<event> lengthBatch(<int|long|double|float|string> parameter, <int> windowLength)
#
#    Extension Type: Window
#    Description: Batch (tumbling) length window, that holds an event which has maximum value of given attribute up to windowLength events, and gets updated on every windowLength event arrival.
#    Parameter:  parameter:  The parameter that need to be compared to find the event which has max value.
#		windowLength: For the number of events the window should tumble.
#    Return Type: Return current and expired events.
#    Examples: minByLengthBatch(temp) returns the event which has maximum temp value recorded for all the events based on their arrival and expiry.