package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

public class MaxByTimeBatchWindowProcessor extends MinByMaxByTimeBatchWindowProcessor{
    public MaxByTimeBatchWindowProcessor(){
        windowType = Constants.MaxByTimeBatchWindow;
    }
}
