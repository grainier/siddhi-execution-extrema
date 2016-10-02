
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
import org.wso2.siddhi.core.util.parser.CollectionOperatorParser;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mathuriga on 29/09/16.
 */
public class MaxByMinByLengthBatchWindowProcessor extends WindowProcessor implements FindableProcessor {
    private int length;
    private int count = 0;

    protected String functionType;
    private ExpressionExecutor functionParameter;
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private ExecutionPlanContext executionPlanContext;
    private VariableExpressionExecutor[] variableExpressionExecutors;
    MaxByMinByExecutor maxByMinByExecutor;

    ComplexEventChunk<StreamEvent> resultStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);

    public MaxByMinByLengthBatchWindowProcessor() {

    }


    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        maxByMinByExecutor = new MaxByMinByExecutor();
        this.executionPlanContext = executionPlanContext;
        if (functionType == "MIN") {
            maxByMinByExecutor.setFunctionType(functionType);
            variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length - 1];
            if (attributeExpressionExecutors.length == 2) {
                variableExpressionExecutors[0] = (VariableExpressionExecutor) attributeExpressionExecutors[0];
                functionParameter = variableExpressionExecutors[0];
                length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
            }
        } else {
            maxByMinByExecutor.setFunctionType(functionType);
            variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length - 1];
            if (attributeExpressionExecutors.length == 2) {
                variableExpressionExecutors[0] = (VariableExpressionExecutor) attributeExpressionExecutors[0];
                functionParameter = variableExpressionExecutors[0];
                length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
            }

        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();

        synchronized (this) {

            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            //clear the outputStream for every lengthBatch
            if (count == 0) {
                outputStreamEventChunk.clear();
            }

            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();

                //get the parameter value for every events
                Object parameterValue = getParameterValue(functionParameter, streamEvent);
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);

                //currentEventChunk.add(clonedStreamEvent);
                //put the value to treemap
                maxByMinByExecutor.insert(clonedStreamEvent, parameterValue);

                count++;
                if (count == length) {
//
                    count = 0;
                    //get the results

                    outputStreamEventChunk.add(maxByMinByExecutor.getResult(maxByMinByExecutor.getFunctionType()));
                    resultStreamEventChunk.add(maxByMinByExecutor.getResult(maxByMinByExecutor.getFunctionType()));

                    System.out.println(maxByMinByExecutor.getResult(maxByMinByExecutor.getFunctionType()));
                    maxByMinByExecutor.getTreeMap().clear();
                    if (outputStreamEventChunk.getFirst() != null) {
                        streamEventChunks.add(outputStreamEventChunk);
                    }
                }

            }
        }
        for (ComplexEventChunk<StreamEvent> outputStreamEventChunk : streamEventChunks) {
            nextProcessor.process(outputStreamEventChunk);
        }


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
        return finder.find(matchingEvent, resultStreamEventChunk, streamEventCloner);
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


    @Override
    public void start() {
        //do nothing
    }

    @Override
    public void stop() {
        //do nothing
    }

    @Override
    public Object[] currentState() {
        return null;
    }

    @Override
    public void restoreState(Object[] state) {
//
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
