package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

import org.wso2.extension.siddhi.window.minbymaxby.MaxByMinByConstants;

/**
 * Output the max event corresponding to a given attribute in a Time Batch Window
 */
public class MaxByTimeBatchWindowProcessor extends MaxByMinByTimeBatchWindowProcessor {
    public MaxByTimeBatchWindowProcessor() {
        maxByMinByType = MaxByMinByConstants.MAX_BY;
        windowType = MaxByMinByConstants.MaxByTimeBatch;
    }
}
