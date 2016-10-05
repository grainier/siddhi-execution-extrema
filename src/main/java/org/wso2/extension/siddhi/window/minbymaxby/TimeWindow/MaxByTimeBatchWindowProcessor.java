package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

/**
 * Output the max event corresponding to a given attribute in a Time Batch Window
 */
public class MaxByTimeBatchWindowProcessor extends MaxByMinByTimeBatchWindowProcessor {
    public MaxByTimeBatchWindowProcessor() {
        sortType = Constants.MAX_BY;
        windowType = Constants.MaxByTimeBatch;
    }
}
