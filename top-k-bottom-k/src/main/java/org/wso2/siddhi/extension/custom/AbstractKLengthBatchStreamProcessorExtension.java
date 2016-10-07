/*
 * Copyright (c) 2015, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.custom;

import com.clearspring.analytics.stream.Counter;
import com.clearspring.analytics.stream.StreamSummary;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.state.StateEvent;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class AbstractKLengthBatchStreamProcessorExtension extends StreamProcessor implements FindableProcessor {
    protected boolean isTopK;
    private int windowLength;
    private int querySize;

    private int count;
    private VariableExpressionExecutor attrVariableExpressionExecutor;
    private StreamSummary<Object> topKFinder;

    private StreamEvent lastStreamEvent = null;
    private StreamEvent resetEvent = null;
    private ComplexEventChunk<StreamEvent> expiredEventChunk = null;

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                   ExecutionPlanContext executionPlanContext) {
        String namePrefix = (isTopK ? "Top" : "Bottom");
        if (attributeExpressionExecutors.length == 3) {
            expiredEventChunk = new ComplexEventChunk<StreamEvent>(true);
            count = 0;
        } else {
            throw new ExecutionPlanValidationException("3 arguments should be passed to " +
                    namePrefix + "KLengthBatchStreamProcessor, but found " + attributeExpressionExecutors.length);
        }

        // Checking the topK/bottomK attribute
        if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
            attrVariableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        } else {
            throw new ExecutionPlanValidationException("Attribute for ordering in " +
                    namePrefix + "KLengthBatchStreamProcessor should be a variable. but found a constant attribute " +
                    attributeExpressionExecutors[1].getClass().getCanonicalName());
        }

        // Checking the window length parameter
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            Attribute.Type attributeType = attributeExpressionExecutors[1].getReturnType();
            if (attributeType == Attribute.Type.INT) {
                windowLength = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                topKFinder = new StreamSummary<Object>(windowLength);
            } else {
                throw new ExecutionPlanValidationException("Window length parameter for " +
                        namePrefix + "KLengthBatchStreamProcessor should be INT. but found " + attributeType);
            }
        } else {
            throw new ExecutionPlanValidationException("Window length parameter for " +
                    namePrefix + "KLengthBatchWindowProcessor should be a constant. but found a dynamic attribute " +
                    attributeExpressionExecutors[1].getClass().getCanonicalName());
        }

        // Checking the query size parameter
        if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            Attribute.Type attributeType = attributeExpressionExecutors[2].getReturnType();
            if (attributeType == Attribute.Type.INT) {
                querySize = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new ExecutionPlanValidationException("Query size parameter for " +
                        namePrefix + "KLengthBatchWindowProcessor should be INT. but found " + attributeType);
            }
        } else {
            throw new ExecutionPlanValidationException("Query size parameter for " +
                    namePrefix + "KLengthBatchWindowProcessor should be a constant. but found a dynamic attribute " +
                    attributeExpressionExecutors[2].getClass().getCanonicalName());
        }

        // Generating the list of attributes
        List<Attribute> newAttributes = new ArrayList<Attribute>();
        for (int i = 0; i < querySize; i++) {
            newAttributes.add(new Attribute(namePrefix + (i + 1) + "Element", attrVariableExpressionExecutor.getReturnType()));
            newAttributes.add(new Attribute(namePrefix + (i + 1) + "Frequency", Attribute.Type.LONG));
        }
        return newAttributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
        synchronized (this) {
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            int frequencyCountMultiplier = (isTopK ? 1 : -1);
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);

                // Current event arrival tasks
                if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                    count++;
                    lastStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    topKFinder.offer(
                            attrVariableExpressionExecutor.execute(clonedStreamEvent),
                            frequencyCountMultiplier
                    );
                }

                // Window end tasks
                if (count == windowLength) {
                    // Adding expired events
                    if (outputExpectsExpiredEvents && expiredEventChunk.getFirst() != null) {
                        outputStreamEventChunk.add(expiredEventChunk.getFirst());
                        expiredEventChunk.clear();
                    }

                    // Adding the reset event
                    outputStreamEventChunk.add(resetEvent);
                    resetEvent = null;

                    // Adding the last event with the topK frequencies for the window
                    List<Counter<Object>> topKCounters = topKFinder.topK(querySize);
                    Object[] outputStreamEventData = new Object[2 * querySize];
                    int i = 0;
                    while (i < topKCounters.size()) {
                        Counter<Object> topKCounter = topKCounters.get(i);
                        outputStreamEventData[2 * i] = topKCounter.getItem();
                        outputStreamEventData[2 * i + 1] = topKCounter.getCount() * frequencyCountMultiplier;
                        i++;
                    }
                    complexEventPopulater.populateComplexEvent(lastStreamEvent, outputStreamEventData);
                    outputStreamEventChunk.add(lastStreamEvent);

                    // Setting the event expired in this window
                    StreamEvent expiredStreamEvent = streamEventCloner.copyStreamEvent(lastStreamEvent);
                    expiredStreamEvent.setTimestamp(currentTime);
                    expiredStreamEvent.setType(ComplexEvent.Type.EXPIRED);
                    expiredEventChunk.add(expiredStreamEvent);
                    lastStreamEvent = null;

                    // Resetting window
                    topKFinder = new StreamSummary<Object>(windowLength);
                    count = 0;

                    // Setting the reset event to be used in the end of the window
                    if (resetEvent == null) {
                        resetEvent = streamEventCloner.copyStreamEvent(streamEventChunk.getFirst());
                        resetEvent.setType(ComplexEvent.Type.RESET);
                    }
                }
            }
        }

        if (outputStreamEventChunk.getFirst() != null) {
            nextProcessor.process(outputStreamEventChunk);
        }
    }

    @Override
    public void start() {
        // Do Nothing
    }

    @Override
    public void stop() {
        // Do Nothing
    }

    @Override
    public Object[] currentState() {
        synchronized (this) {
            if (outputExpectsExpiredEvents) {
                return new Object[]{topKFinder, windowLength, querySize, count, lastStreamEvent, resetEvent, expiredEventChunk};
            } else {
                return new Object[]{topKFinder, windowLength, querySize, count, lastStreamEvent, resetEvent};
            }
        }
    }

    @Override
    public void restoreState(Object[] state) {
        synchronized (this) {
            topKFinder = (StreamSummary<Object>) state[0];
            windowLength = (Integer) state[1];
            querySize = (Integer) state[2];
            count = (Integer) state[3];

            lastStreamEvent = (StreamEvent) state[4];
            resetEvent = (StreamEvent) state[5];
            if (state.length == 7) {
                expiredEventChunk = (ComplexEventChunk<StreamEvent>) state[6];
            }
        }
    }

    @Override
    public StreamEvent find(StateEvent matchingEvent, Finder finder) {
        synchronized (this) {
            return finder.find(matchingEvent, expiredEventChunk, streamEventCloner);
        }
    }

    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder,
                                  ExecutionPlanContext executionPlanContext,
                                  List<VariableExpressionExecutor> variableExpressionExecutors,
                                  Map<String, EventTable> eventTableMap) {
        synchronized (this) {
            if (expiredEventChunk == null) {
                expiredEventChunk = new ComplexEventChunk<StreamEvent>(true);
            }
            return OperatorParser.constructOperator(
                    expiredEventChunk, expression, matchingMetaStateHolder, executionPlanContext,
                    variableExpressionExecutors, eventTableMap
            );
        }
    }
}