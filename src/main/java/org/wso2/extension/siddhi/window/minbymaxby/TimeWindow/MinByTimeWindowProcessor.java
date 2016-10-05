package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

/**
 * Output the min event corresponding to a given attribute in a Time Window
 */
public class MinByTimeWindowProcessor extends MaxByMinByTimeWindowProcessor {
    public MinByTimeWindowProcessor() {
        sortType = Constants.MIN_BY;
        windowType = Constants.MinByTime;
    }
}
