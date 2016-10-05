package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

public class MinByTimeBatchWindowProcessor extends MaxByMinByTimeBatchWindowProcessor {
    public MinByTimeBatchWindowProcessor(){
        timeBatchWindowType = Constants.MIN_BY;
    }
}
