package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

import org.wso2.extension.siddhi.window.minbymaxby.MaxByMinByConstants;

/**
 * Output the max event corresponding to a given attribute in a Time Window
 */

public class MaxByTimeWindowProcessor extends MaxByMinByTimeWindowProcessor {
    public MaxByTimeWindowProcessor() {
        sortType = MaxByMinByConstants.MAX_BY;
        windowType = MaxByMinByConstants.MaxByTime;
    }
}
