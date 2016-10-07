package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

import org.wso2.extension.siddhi.window.minbymaxby.MaxByMinByConstants;

/**
 * Output the min event corresponding to a given attribute in a Time Window
 */
public class MinByTimeWindowProcessor extends MaxByMinByTimeWindowProcessor {
    public MinByTimeWindowProcessor() {
        maxByMinByType = MaxByMinByConstants.MIN_BY;
        windowType = MaxByMinByConstants.MinByTime;
    }
}
