package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

/**
 * Output the min event corresponding to a given attribute in a Time Window
 */
public class MinByTimeWindowProcessor extends MaxByMinByTimeWindowProcessor {
    public MinByTimeWindowProcessor() {
        timeWindowType = Constants.MIN_BY;
    }
}
