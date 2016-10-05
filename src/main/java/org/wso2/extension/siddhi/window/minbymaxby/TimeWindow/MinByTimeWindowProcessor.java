package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

public class MinByTimeWindowProcessor extends MaxByMinByTimeWindowProcessor {
    public MinByTimeWindowProcessor(){
        timeWindowType = Constants.MIN_BY;
    }
}
