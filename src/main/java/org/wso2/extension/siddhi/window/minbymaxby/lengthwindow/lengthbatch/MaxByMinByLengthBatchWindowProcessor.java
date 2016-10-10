
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
package org.wso2.extension.siddhi.window.minbymaxby.lengthwindow.lengthbatch;

import org.wso2.extension.siddhi.window.minbymaxby.MaxByMinByConstants;
import org.wso2.extension.siddhi.window.minbymaxby.MaxByMinByExecutor;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
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
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;


/**
 * Abstract class which gives the min/max event in a LengthBatch window
 */

public abstract class MaxByMinByLengthBatchWindowProcessor extends WindowProcessor implements FindableProcessor {
    private int length;
    private int count = 0;
    protected String minByMaxByExecutorType;
    protected String minByMaxByExtensionType;
    private ExpressionExecutor minByMaxByExecutorAttribute;
    private ComplexEventChunk<StreamEvent> expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private ExecutionPlanContext executionPlanContext;
    private VariableExpressionExecutor[] variableExpressionExecutors;
    private MaxByMinByExecutor minByMaxByExecutor;
    private StreamEvent oldEvent;
    private StreamEvent resultEvent;
    private StreamEvent expiredResultEvent;
    private StreamEvent resetEvent;

    public MaxByMinByLengthBatchWindowProcessor() {

    }


    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        this.executionPlanContext = executionPlanContext;
        minByMaxByExecutor = new MaxByMinByExecutor();
        if (minByMaxByExecutorType == "MIN") {
            minByMaxByExecutor.setMinByMaxByExecutorType(minByMaxByExecutorType);
        } else {
            minByMaxByExecutor.setMinByMaxByExecutorType(minByMaxByExecutorType);
        }

        if (attributeExpressionExecutors.length != 2) {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to minbymaxby:" + minByMaxByExecutorType + " window, " +
                    "required 2, but found " + attributeExpressionExecutors.length);
        }

        Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();
        if (!((attributeType == Attribute.Type.DOUBLE)
                || (attributeType == Attribute.Type.INT)
                || (attributeType == Attribute.Type.STRING)
                || (attributeType == Attribute.Type.FLOAT)
                || (attributeType == Attribute.Type.LONG))) {
            throw new ExecutionPlanValidationException("Invalid parameter type found for the first argument of minbymaxby:" + minByMaxByExecutorType + " window, " +
                    "required " + Attribute.Type.INT + " or " + Attribute.Type.LONG +
                    " or " + Attribute.Type.FLOAT + " or " + Attribute.Type.DOUBLE + "or" + Attribute.Type.STRING +
                    ", but found " + attributeType.toString());
        }
        attributeType = attributeExpressionExecutors[1].getReturnType();
        if (!((attributeType == Attribute.Type.LONG)
                || (attributeType == Attribute.Type.INT))) {
            throw new ExecutionPlanValidationException("Invalid parameter type found for the second argument of minbymaxby:" + minByMaxByExecutorType + " window, " +
                    "required " + Attribute.Type.INT + " or " + Attribute.Type.LONG +
                    ", but found " + attributeType.toString());
        }

        variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length - 1];
        if (attributeExpressionExecutors.length == 2) {
            variableExpressionExecutors[0] = (VariableExpressionExecutor) attributeExpressionExecutors[0];
            minByMaxByExecutorAttribute = variableExpressionExecutors[0];
            length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
        }

    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent currentEvent = streamEventCloner.copyStreamEvent(streamEvent);

                if (count == 0) {
                    outputStreamEventChunk.clear();
                    oldEvent = null;
                    //clonedResultEvent=resultEvent;
                }

                //Get the event which hold the minimum or maximum event

                if (minByMaxByExecutorType.equals(MaxByMinByConstants.MAX_BY)) {
                    resultEvent = MaxByMinByExecutor.getMaxEventBatchProcessor(currentEvent, oldEvent, minByMaxByExecutorAttribute);
                    oldEvent = resultEvent;
                } else if (minByMaxByExecutorType.equals(MaxByMinByConstants.MIN_BY)) {
                    resultEvent = MaxByMinByExecutor.getMinEventBatchProcessor(currentEvent, oldEvent, minByMaxByExecutorAttribute);
                    oldEvent = resultEvent;
                }

                count++;
                if (count == length) {

                    if (resultEvent != null) {
                        if (expiredResultEvent != null) {
                            expiredEventChunk.clear();
                            outputStreamEventChunk.add(expiredResultEvent);
                            outputStreamEventChunk.add(resetEvent);

                        }
                        outputStreamEventChunk.add(resultEvent);
                        expiredResultEvent = streamEventCloner.copyStreamEvent(resultEvent);
                        expiredResultEvent.setTimestamp(currentTime);
                        expiredResultEvent.setType(StreamEvent.Type.EXPIRED);
                        expiredEventChunk.add(expiredResultEvent);
                        resetEvent = streamEventCloner.copyStreamEvent(resultEvent);
                        resetEvent.setType(StateEvent.Type.RESET);
                        System.out.println(outputStreamEventChunk);
                    }
                    count = 0;
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
        return this.expiredEventChunk != null ? new Object[]{this.resultEvent, this.expiredEventChunk.getFirst(), Integer.valueOf(this.count), this.resetEvent} : new Object[]{this.resultEvent, Integer.valueOf(this.count), this.resetEvent};
    }

    @Override
    public void restoreState(Object[] state) {
        if (state.length > 3) {
            this.resultEvent = null;
            Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
            resultEvent = (StreamEvent) stateEntry.getValue();
            Map.Entry<String, Object> stateEntry2 = (Map.Entry<String, Object>) state[1];
            this.count = ((Integer) state[2]).intValue();
            this.resetEvent = (StreamEvent) state[3];
        } else {
            this.resultEvent = null;
            Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
            resultEvent = (StreamEvent) stateEntry.getValue();
            Map.Entry<String, Object> stateEntry2 = (Map.Entry<String, Object>) state[1];
            this.count = ((Integer) state[1]).intValue();
            this.resetEvent = (StreamEvent) state[2];
        }
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, Finder finder) {

        return finder.find(matchingEvent, expiredEventChunk, streamEventCloner);
    }

    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder, ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> list, Map<String, EventTable> map) {
        return OperatorParser.constructOperator(expiredEventChunk, expression, matchingMetaStateHolder, executionPlanContext, list, map);
    }
}
