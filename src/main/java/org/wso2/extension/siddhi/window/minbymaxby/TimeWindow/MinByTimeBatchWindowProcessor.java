package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

import org.wso2.extension.siddhi.window.minbymaxby.MaxByMinByConstants;

/**
 * Output the min event corresponding to a given attribute in a TimeBatch Window
 */
public class MinByTimeBatchWindowProcessor extends MaxByMinByTimeBatchWindowProcessor {
    public MinByTimeBatchWindowProcessor() {
        sortType = MaxByMinByConstants.MIN_BY;
        windowType = MaxByMinByConstants.MinByTimeBatch;
    }
}
