/*
 *  Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 *  WSO2 Inc. licenses this file to you under the Apache License,
 *  Version 2.0 (the "License"); you may not use this file except
 *  in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an
 *   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *   KIND, either express or implied. See the License for the
 *   specific language governing permissions and limitations
 *   under the License.
 */

package org.wso2.extension.siddhi.execution.extrema;

import org.wso2.extension.siddhi.execution.extrema.util.Constants;
import org.wso2.extension.siddhi.execution.extrema.util.MaxByMinByExecutor;
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
 * It extends WindowProcessor class
 *
 * @see WindowProcessor
 * @see FindableProcessor
 */

public abstract class MaxByMinByLengthBatchWindowProcessor extends WindowProcessor implements FindableProcessor {
    private int length;
    private int count = 0;
    /*
     * minByMaxByExecutorType holds the value to indicate MIN or MAX
     */
    protected String minByMaxByExecutorType;
    /*
    Attribute which is used to find Extrema event
     */
    private ExpressionExecutor minByMaxByExecutorAttribute;
    private ComplexEventChunk<StreamEvent> expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private ExecutionPlanContext executionPlanContext;

    /*
    minByMaxByExecutor used to get extrema event
     */
    private MaxByMinByExecutor minByMaxByExecutor;
    /*
     *Represents previous extrema event
     */
    private StreamEvent oldEvent;
    /*
    Represents current extrema event
     */
    private StreamEvent currentevent;
    private StreamEvent expiredResultEvent;
    private StreamEvent resetEvent;

    /**
     * The init method of the WindowProcessor, this method will be called before other methods
     *
     * @param expressionExecutors  the executors of each function parameters
     * @param executionPlanContext the context of the execution plan
     */
    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        this.executionPlanContext = executionPlanContext;
        minByMaxByExecutor = new MaxByMinByExecutor();

        minByMaxByExecutor.setMinByMaxByExecutorType(minByMaxByExecutorType);


        if (attributeExpressionExecutors.length != 2) {
            throw new ExecutionPlanValidationException(
                    "Invalid no of arguments passed to minbymaxby:" + minByMaxByExecutorType + " window, "
                            + "required 2, but found " + attributeExpressionExecutors.length);
        }

        Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();

        if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
            if (!((attributeType == Attribute.Type.DOUBLE) || (attributeType == Attribute.Type.INT) || (attributeType
                    == Attribute.Type.FLOAT) || (attributeType == Attribute.Type.LONG) || (attributeType
                    == Attribute.Type.STRING))) {
                throw new ExecutionPlanValidationException(
                        "Invalid parameter type found for the first argument of minbymaxby:" + minByMaxByExecutorType
                                + " window, " + "required " + Attribute.Type.INT + " or " + Attribute.Type.LONG + " or "
                                + Attribute.Type.FLOAT + " or " + Attribute.Type.DOUBLE + "or" + Attribute.Type.STRING
                                + ", but found " + attributeType.toString());
            }
        } else {
            throw new ExecutionPlanValidationException(
                    "LengthBatch window should have variable parameter attribute but found a constant attribute "
                            + attributeExpressionExecutors[0].getClass().getCanonicalName());
        }

        attributeType = attributeExpressionExecutors[1].getReturnType();
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            if (!(((attributeType == Attribute.Type.INT)))) {
                throw new ExecutionPlanValidationException(
                        "Invalid parameter type found for the second argument of minbymaxby:" + minByMaxByExecutorType
                                + " window, " + "required " + Attribute.Type.INT +
                                ", but found " + attributeType.toString() + " or second argument is not a constant");
            }
        } else {
            throw new ExecutionPlanValidationException(
                    "LengthBatch window should have constant parameter attribute but found a dynamic attribute "
                            + attributeExpressionExecutors[1].getClass().getCanonicalName());
        }
        minByMaxByExecutorAttribute = attributeExpressionExecutors[0];
        length = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();

    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk  the stream event chunk that need to be processed
     * @param nextProcessor     the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner) {
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
                }

                //Get the event which hold the minimum or maximum event

                if (minByMaxByExecutorType.equals(Constants.MAX_BY)) {
                    resultEvent = MaxByMinByExecutor
                            .getMaxEventBatchProcessor(currentEvent, oldEvent, minByMaxByExecutorAttribute);
                    oldEvent = resultEvent;
                } else if (minByMaxByExecutorType.equals(Constants.MIN_BY)) {
                    resultEvent = MaxByMinByExecutor
                            .getMinEventBatchProcessor(currentEvent, oldEvent, minByMaxByExecutorAttribute);
                    oldEvent = currentevent;
                }

                count++;
                if (count == length) {
                    if (expiredResultEvent != null) {
                        expiredEventChunk.clear();
                        expiredResultEvent.setTimestamp(currentTime);
                        outputStreamEventChunk.add(expiredResultEvent);
                        outputStreamEventChunk.add(resetEvent);
                    }
                    outputStreamEventChunk.add(currentevent);
                    expiredResultEvent = streamEventCloner.copyStreamEvent(currentevent);
                    expiredResultEvent.setType(StreamEvent.Type.EXPIRED);
                    // TODO: 15/12/16 create the chunk within find method 
                    expiredEventChunk.add(expiredResultEvent);
                    resetEvent = streamEventCloner.copyStreamEvent(currentevent);
                    resetEvent.setType(StateEvent.Type.RESET);
                    System.out.println(outputStreamEventChunk);

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

    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override
    public void start() {
        //do nothing
    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {
        //do nothing
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as an array
     */
    @Override
    public Object[] currentState() {
        if (this.expiredResultEvent != null)
            return new Object[]{this.currentevent, this.expiredResultEvent, Integer.valueOf(this.count),
                    this.resetEvent};
        else {
            return new Object[]{this.currentevent, Integer.valueOf(this.count), this.resetEvent};
        }
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as an array on
     *              the same order provided by currentState().
     */
    @Override
    public void restoreState(Object[] state) {
        if (state.length > 3) {
            this.currentevent = (StreamEvent) state[0];
            this.expiredResultEvent = (StreamEvent) state[1];
            this.count = (Integer) state[2];
            this.resetEvent = (StreamEvent) state[3];
        } else {
            this.currentevent = (StreamEvent) state[0];
            this.count = (Integer) state[2];
            this.resetEvent = (StreamEvent) state[3];
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
    public StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, expiredEventChunk, streamEventCloner);
    }

    /**
     * To construct a finder having the capability of finding events at the processor that corresponds to the incoming
     * matchingEvent and the given matching expression logic.
     *
     * @param expression              the matching expression
     * @param matchingMetaStateHolder the meta structure of the incoming matchingEvent
     * @param executionPlanContext    current execution plan context
     * @param list                    the list of variable ExpressionExecutors already created
     * @param map                     map of event tables
     * @return finder having the capability of finding events at the processor against the expression and incoming
     * matchingEvent
     */
    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder,
                                  ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> list,
                                  Map<String, EventTable> map) {
        return OperatorParser
                .constructOperator(expiredEventChunk, expression, matchingMetaStateHolder, executionPlanContext, list,
                        map);
    }
}
