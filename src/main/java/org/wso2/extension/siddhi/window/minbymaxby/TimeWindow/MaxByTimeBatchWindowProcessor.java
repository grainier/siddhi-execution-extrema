package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

public class MaxByTimeBatchWindowProcessor extends MaxByMinByTimeBatchWindowProcessor {
    public MaxByTimeBatchWindowProcessor(){
        timeBatchWindowType = Constants.MAX_BY;
    }
}
