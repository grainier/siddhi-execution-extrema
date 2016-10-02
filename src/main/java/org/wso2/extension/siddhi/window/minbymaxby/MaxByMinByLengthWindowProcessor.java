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

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.MetaComplexEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.query.api.expression.Expression;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.parser.CollectionOperatorParser;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mathuriga on 30/09/16.
 */
public class MaxByMinByLengthWindowProcessor extends WindowProcessor implements FindableProcessor {
    private ExpressionExecutor funtionParameter;
    protected String functionType;
    private int length;
    private int count = 0;
    private ComplexEventChunk<StreamEvent> expiredEventChunk = null;
    private ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
    private ExecutionPlanContext executionPlanContext;
    private MaxByMinByExecutor maxByMinByExecutor;
    private StreamEvent outputStreamEvent;
    private List<StreamEvent> events = new ArrayList<StreamEvent>();


    public void setOutputStreamEvent(StreamEvent outputSreamEvent) {
        this.outputStreamEvent = outputSreamEvent;
    }


    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
        maxByMinByExecutor = new MaxByMinByExecutor();
        if (functionType == "MIN") {
            maxByMinByExecutor.setFunctionType(functionType);
            if (attributeExpressionExecutors.length == 2) {
                funtionParameter = attributeExpressionExecutors[0];
                length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
            }
        } else {
            maxByMinByExecutor.setFunctionType(functionType);
            if (attributeExpressionExecutors.length == 2) {
                funtionParameter = attributeExpressionExecutors[0];
                length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
            }

        }
    }


    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk, Processor processor, StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            while (complexEventChunk.hasNext()) {
                StreamEvent streamEvent = complexEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);

                if (count != 0) {
                    outputStreamEventChunk.clear();
                }

                //get the parameter value for every events
                Object parameterValue = getParameterValue(funtionParameter, streamEvent);
                maxByMinByExecutor.insert(clonedStreamEvent, parameterValue);
                //

                //clonedEvent.setType(StreamEvent.Type.EXPIRED);
                if (count < length) {
                    count++;
                    //get the output event
                    setOutputStreamEvent(maxByMinByExecutor.getResult(maxByMinByExecutor.getFunctionType()));
                    outputStreamEventChunk.add(outputStreamEvent);
                    System.out.println(outputStreamEventChunk);
                    if (outputStreamEventChunk.getFirst() != null) {
                        streamEventChunks.add(outputStreamEventChunk);
                    }
                    //
                    events.add(clonedStreamEvent);
                } else {
                    StreamEvent firstEvent = events.get(0);
                    if (firstEvent != null) {
                        firstEvent.setTimestamp(currentTime);

                        //remove the expired event from treemap
                        Object expiredEventParameterValue = getParameterValue(funtionParameter, firstEvent);
                        maxByMinByExecutor.getTreeMap().remove(expiredEventParameterValue);
                        events.remove(0);
                        //

                        //get the output event
                        setOutputStreamEvent(maxByMinByExecutor.getResult(maxByMinByExecutor.getFunctionType()));
                        outputStreamEventChunk.add(outputStreamEvent);
                        System.out.println(outputStreamEventChunk);
                        if (outputStreamEventChunk.getFirst() != null) {
                            streamEventChunks.add(outputStreamEventChunk);
                        }
                        //


                        //complexEventChunk.insertBeforeCurrent(firstEvent);
                        events.add(clonedStreamEvent);

                    }
                }
            }
        }

        for (ComplexEventChunk<StreamEvent> outputStreamEventChunk : streamEventChunks) {
            nextProcessor.process(outputStreamEventChunk);
        }

    }

    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        return new Object[]{new AbstractMap.SimpleEntry<String, Object>("ExpiredEventChunk", expiredEventChunk.getFirst()), new AbstractMap.SimpleEntry<String, Object>("Count", count)};
    }

    @Override
    public void restoreState(Object[] state) {
        expiredEventChunk.clear();
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        expiredEventChunk.add((StreamEvent) stateEntry.getValue());
        Map.Entry<String, Object> stateEntry2 = (Map.Entry<String, Object>) state[1];
        count = (Integer) stateEntry2.getValue();
    }

    /**
     * To find events from the processor event pool, that the matches the matchingEvent based on finder logic.
     *
     * @param matchingEvent the event to be matched with the events at the processor
     * @param finder        the execution element responsible for finding the corresponding events that matches
     *                      the matchingEvent based on pool of events at Processor
     * @return the matched events
     */
    @Override
    public synchronized StreamEvent find(ComplexEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, outputStreamEventChunk, streamEventCloner);
    }

    /**
     * To construct a finder having the capability of finding events at the processor that corresponds to the incoming
     * matchingEvent and the given matching expression logic.
     *
     * @param expression                  the matching expression
     * @param matchingMetaComplexEvent    the meta structure of the incoming matchingEvent
     * @param executionPlanContext        current execution plan context
     * @param variableExpressionExecutors the list of variable ExpressionExecutors already created
     * @param eventTableMap               map of event tables
     * @param matchingStreamIndex         the stream index of the incoming matchingEvent
     * @param withinTime                  the maximum time gap between the events to be matched
     * @return finder having the capability of finding events at the processor against the expression and incoming
     * matchingEvent
     */
    @Override
    public Finder constructFinder(Expression expression, MetaComplexEvent matchingMetaComplexEvent,
                                  ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors,
                                  Map<String, EventTable> eventTableMap, int matchingStreamIndex, long withinTime) {
        return CollectionOperatorParser.parse(expression, matchingMetaComplexEvent, executionPlanContext,
                variableExpressionExecutors, eventTableMap, matchingStreamIndex, inputDefinition, withinTime);
    }


    /**
     * To find the parameter value of given parameter for each event .
     *
     * @param functionParameter name of the parameter of the event data
     * @param streamEvent       event  at processor
     * @return the parameterValue
     */

    public Object getParameterValue(ExpressionExecutor functionParameter, StreamEvent streamEvent) {
        Object parameterValue;
        parameterValue = functionParameter.execute(streamEvent);
        return parameterValue;
    }


}
