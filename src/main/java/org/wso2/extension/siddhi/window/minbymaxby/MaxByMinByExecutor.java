
/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
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
 */
package org.wso2.extension.siddhi.window.minbymaxby;

import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.query.api.expression.constant.FloatConstant;
import org.wso2.siddhi.query.compiler.SiddhiQLParser;

import java.util.Comparator;
import java.util.TreeMap;

/**
 * Created by mathuriga on 29/09/16.
 */
public class MaxByMinByExecutor {
    private String functionType;
    private TreeMap<Object, StreamEvent> treeMap = new TreeMap<Object, StreamEvent>();
    private TreeMap<Object, StreamEvent> getTreeMap() {
        return treeMap;
    }



    public String getFunctionType() {
        return functionType;
    }

    public void setFunctionType(String functionType) {
        this.functionType = functionType;
    }


    /**
     * To insert the current event into treemap .
     *
     * @param clonedStreamEvent copy of current event
     * @param parameterValue    key for the treemap(object which holds the parameter value)
     */
    public void insert(StreamEvent clonedStreamEvent, Object parameterValue) {
        treeMap.put(parameterValue, clonedStreamEvent);


    }

    /**
     * To find the event which holds minimum or maximum  value of given parameter.
     *
     * @param functionType MIN/MAX
     * @return outputEvent
     */

    public StreamEvent getResult(String functionType) {
        StreamEvent outputEvent;
        if (functionType.equals("MIN")) {
            Object minEventKey = treeMap.firstKey();
            outputEvent = treeMap.get(minEventKey);
        } else {
            Object maxEventKey = treeMap.lastKey();
            outputEvent = treeMap.get(maxEventKey);
        }
        return outputEvent;
    }
    
}
