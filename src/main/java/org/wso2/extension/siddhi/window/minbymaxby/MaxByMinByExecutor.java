
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
import org.wso2.siddhi.query.api.definition.Attribute;
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

    /**
     * Retrun the minimum event comparing two events
     * 
     * @param currentEvent  new event 
     * @param oldEvent  the previous event that is stored as the minimun event
     * @param minByAttribute  the attribute which the comparison is done.
     * @return minEvent
     */

    public static StreamEvent getMinEventBatchProcessor(StreamEvent currentEvent, StreamEvent oldEvent, ExpressionExecutor minByAttribute) {
        StreamEvent minEvent = oldEvent;
        if (minEvent != null) {
            Attribute.Type attributeType = minByAttribute.getReturnType();
            switch (attributeType) {
                case DOUBLE:
                    if ((Double) minByAttribute.execute(currentEvent) <= (Double) minByAttribute.execute(minEvent)) {
                        minEvent = currentEvent;
                    }
                    break;
                case FLOAT:
                    if ((Float) minByAttribute.execute(currentEvent) <= (Float) minByAttribute.execute(minEvent)) {
                        minEvent = currentEvent;
                    }
                    break;
                case LONG:
                    if ((Long) minByAttribute.execute(currentEvent) <= (Long) minByAttribute.execute(minEvent)) {
                        minEvent = currentEvent;
                    }
                    break;
                default:
                    if ((Integer) minByAttribute.execute(currentEvent) <= (Integer) minByAttribute.execute(minEvent)) {
                        minEvent = currentEvent;
                    }
                    break;
            }
        } else {
            minEvent = currentEvent;
        }
        return minEvent;
    }

    /**
     * Retrun the maximum event comparing two events
     *
     * @param currentEvent  new event 
     * @param oldEvent  the previous event that is stored as the maximum event
     * @param maxByAttribute  the attribute which the comparison is done.
     * @return maxEvent
     */

    public static StreamEvent getMaxEventBatchProcessor(StreamEvent currentEvent, StreamEvent oldEvent, ExpressionExecutor maxByAttribute) {
        StreamEvent maxEvent = oldEvent;
        if (maxEvent != null) {
            Attribute.Type attributeType = maxByAttribute.getReturnType();
            switch (attributeType) {
                case DOUBLE:
                    if ((Double) maxByAttribute.execute(currentEvent) >= (Double) maxByAttribute.execute(maxEvent)) {
                        maxEvent = currentEvent;
                    }
                    break;
                case FLOAT:
                    if ((Float) maxByAttribute.execute(currentEvent) >= (Float) maxByAttribute.execute(maxEvent)) {
                        maxEvent = currentEvent;
                    }
                    break;
                case LONG:
                    if ((Long) maxByAttribute.execute(currentEvent) >= (Long) maxByAttribute.execute(maxEvent)) {
                        maxEvent = currentEvent;
                    }
                    break;
                default:
                    if ((Integer) maxByAttribute.execute(currentEvent) >= (Integer) maxByAttribute.execute(maxEvent)) {
                        maxEvent = currentEvent;
                    }
                    break;
            }
        } else {
            maxEvent = currentEvent;
        }
        return maxEvent;
    }

    
}
