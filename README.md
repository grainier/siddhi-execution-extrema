# SiddhiExtensions
This repository contains 6 extensions created for [Siddhi](https://github.com/wso2/siddhi)
The following extensions can be found in the [top-k-bottom-k module](https://github.com/nadundesilva/SiddhiExtensions/tree/master/top-k-bottom-k)

## topKLengthBatch
Keeps a length batch window of the specified length and emits the topK events per batch if there is a change in the topK values from the last topK values sent.
```
from <input stream name>[<filter condition>]#extrema:topKLengthBatch(attribute, <int> windowLength, <int> kValue)
select <attribute name>, <attribute name>, ...
insert into <output stream name>
```

## bottomKLengthBatch
Keeps a length batch window of the specified length and emits the bottomK events per batch if there is a change in the bottomK values from the last bottomK values sent.
```
from <input stream name>[<filter condition>]#extrema:bottomKLengthBatch(attribute, <int> windowLength, <int> kValue)
select <attribute name>, <attribute name>, ...
insert into <output stream name>
```

## topKTimeBatch
Keeps a time batch window of the specified size and emits the topK events per batch if there is a change in the topK values from the last topK values sent.
```
from <input stream name>[<filter condition>]#extrema:topKTimeBatch(attribute, <int|long|time> windowTime, <int> kValue, <int> [startTime])
select <attribute name>, <attribute name>, ...
insert into <output stream name>
```

## bottomKTimeBatch
Keeps a time batch window of the specified size and emits the bottomK events per batch if there is a change in the bottomK values from the last bottomK values sent.
```
from <input stream name>[<filter condition>]#extrema:bottomKTimeBatch(attribute, <int|long|time> windowTime, <int> kValue, <int> [startTime])
select <attribute name>, <attribute name>, ...
insert into <output stream name>
```

## topK
Processes the events received and emits the topK values when the values change. This can work on sliding and batch windows.
```
from <input stream name>[<filter condition>]<sliding/batch window>#extrema:topK(attribute, <int> kValue)
select <attribute name>, <attribute name>, ...
insert into <output stream name>
```

## bottomK
Processes the events received and emits the bottomK values when the values change. This can work on sliding and batch windows.
```
from <input stream name>[<filter condition>]<sliding/batch window>#extrema:bottomK(attribute, <int> kValue)
select <attribute name>, <attribute name>, ...
insert into <output stream name>
```
