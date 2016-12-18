/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.wso2.extension.siddhi.execution.extrema;

import org.wso2.extension.siddhi.execution.extrema.util.MaxByMinByConstants;

/**
 * Sample Query:
 * from inputStream#window.minbymaxby:maxbytimebatch(attribute1,1 sec)
 * select attribute1, attribute2
 * insert into outputStream;
 * <p>
 * Description:
 * In the sample query given, 1 sec is the duration of the window and attribute1 is the maxBy attribute.
 * According to the given attribute it will give the maximum event within given time.
 */

public class MaxByTimeBatchWindowProcessor extends MaxByMinByTimeBatchWindowProcessor {
    public MaxByTimeBatchWindowProcessor() {
        maxByMinByType = MaxByMinByConstants.MAX_BY;
        windowType = MaxByMinByConstants.MaxByTimeBatch;
    }
}
