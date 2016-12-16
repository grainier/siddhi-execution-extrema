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
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.WindowProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.*;

/**
 * Abstract class which gives the min/max event in a Time Window
 * according to given attribute as events arrive and expire
 */

public abstract class MaxByMinByTimeWindowProcessor extends WindowProcessor
        implements SchedulingProcessor, FindableProcessor {

    protected String maxByMinByType;
    protected String windowType;
    private long timeInMilliSeconds;
    private Scheduler scheduler;
    private ExecutionPlanContext executionPlanContext;
    private volatile long lastTimestamp = Long.MIN_VALUE;
    private ExpressionExecutor sortByAttribute;
    private StreamEvent currentEvent;
    private MaxByMinByExecutor minByMaxByExecutor;
    private ComplexEventChunk<StreamEvent> expiredEventChunk;

    /**
     * The getScheduler method of the TimeWindowProcessor.
     * Since scheduler is a private variable, setter method is for public access.
     */
    @Override public Scheduler getScheduler() {
        return scheduler;
    }

    /**
     * The setScheduler method of the TimeWindowProcessor.
     * Since scheduler is a private variable, setter method is for public access.
     *
     * @param scheduler the value of scheduler.
     */
    @Override public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    /**
     * The init method of the WindowProcessor, this method will be called before other methods
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     */
    @Override protected void init(ExpressionExecutor[] attributeExpressionExecutors,
            ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
        this.expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
        minByMaxByExecutor = new MaxByMinByExecutor();
        if (attributeExpressionExecutors.length == 2) {
            Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();
            sortByAttribute = attributeExpressionExecutors[0];
            if (!((attributeType == Attribute.Type.DOUBLE) || (attributeType == Attribute.Type.INT) || (attributeType
                    == Attribute.Type.FLOAT) || (attributeType == Attribute.Type.LONG) || (attributeType
                    == Attribute.Type.STRING))) {
                throw new ExecutionPlanValidationException(
                        "Invalid parameter type found for the first argument of " + windowType + " required "
                                + Attribute.Type.INT + " or " + Attribute.Type.LONG + " or " + Attribute.Type.FLOAT
                                + " or " + Attribute.Type.DOUBLE + " or " + Attribute.Type.STRING + ", but found "
                                + attributeType.toString());
            }

            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();

                } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1])
                            .getValue();
                } else {
                    throw new ExecutionPlanValidationException(
                            "Time parameter should be either int or long, but found " + attributeExpressionExecutors[1]
                                    .getReturnType());
                }
            } else {
                throw new ExecutionPlanValidationException(
                        "Time parameter should have constant parameter attribute but found a dynamic attribute "
                                + attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else {
            throw new ExecutionPlanValidationException(
                    "Invalid no of arguments passed to " + windowType + ", " + "required 2, but found "
                            + attributeExpressionExecutors.length + " input attributes");
        }
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk  the stream event chunk that need to be processed
     * @param nextProcessor     the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     */
    @Override protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
            StreamEventCloner streamEventCloner) {
        synchronized (this) {
            StreamEvent streamEvent = null;
            while (streamEventChunk.hasNext()) {
                streamEvent = streamEventChunk.next();
                long currentTime = executionPlanContext.getTimestampGenerator().currentTime();

                // Iterate through the sortedEventMap and remove the expired events
                Set set = minByMaxByExecutor.getSortedEventMap().entrySet();
                Iterator iterator = set.iterator();
                while (iterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) iterator.next();
                    StreamEvent expiredEvent = (StreamEvent) entry.getValue();
                    long timeDiff = expiredEvent.getTimestamp() - currentTime + timeInMilliSeconds;
                    if (timeDiff <= 0) {
                        iterator.remove();
                    }
                }
                //remove expired events from the expiredEventChunk
                expiredEventChunk.reset();
                while (expiredEventChunk.hasNext()) {
                    StreamEvent toExpiredEvent = expiredEventChunk.next();
                    long timeDiff = toExpiredEvent.getTimestamp() - currentTime + timeInMilliSeconds;
                    if (timeDiff <= 0) {
                        expiredEventChunk.remove();
                        toExpiredEvent.setType(StreamEvent.Type.EXPIRED);
                        toExpiredEvent.setTimestamp(currentTime);
                        streamEventChunk.insertBeforeCurrent(toExpiredEvent);
                    }
                }

                //Add the current event to sortedEventMap
                if (streamEvent.getType() == StreamEvent.Type.CURRENT) {
                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    minByMaxByExecutor.insert(clonedEvent, sortByAttribute.execute(clonedEvent));
                    if (lastTimestamp < clonedEvent.getTimestamp()) {
                        scheduler.notifyAt(clonedEvent.getTimestamp() + timeInMilliSeconds);
                        lastTimestamp = clonedEvent.getTimestamp();
                    }
                }
                streamEventChunk.remove();
            }
            expiredEventChunk.reset();
            //retrieve the min/max event and add to streamEventChunk
            if (streamEvent != null && streamEvent.getType() == StreamEvent.Type.CURRENT) {
                StreamEvent tempEvent;
                if (maxByMinByType.equals(MaxByMinByConstants.MIN_BY)) {
                    tempEvent = minByMaxByExecutor.getResult(MaxByMinByConstants.MIN_BY);
                } else {
                    tempEvent = minByMaxByExecutor.getResult(MaxByMinByConstants.MAX_BY);
                }
                // TODO: 12/15/16  use .equals() to compare two objects  
                if (tempEvent != currentEvent) {
                    StreamEvent event = streamEventCloner.copyStreamEvent(tempEvent);
                    expiredEventChunk.add(event);
                    currentEvent = tempEvent;
                    streamEventChunk.add(currentEvent);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    /**
     * To find events from the processor event pool, that the matches the matchingEvent based on finder logic.
     *
     * @param matchingEvent the event to be matched with the events at the processor
     * @param finder        the execution element responsible for finding the corresponding events that matches
     *                      the matchingEvent based on pool of events at Processor
     * @return the matched events
     */
    @Override public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, expiredEventChunk, streamEventCloner);
    }

    /**
     * To construct a finder having the capability of finding events at the processor that corresponds to the incoming
     * matchingEvent and the given matching expression logic.
     *
     * @param expression                  the matching expression
     * @param matchingMetaStateHolder     the meta structure of the incoming matchingEvent
     * @param executionPlanContext        current execution plan context
     * @param variableExpressionExecutors the list of variable ExpressionExecutors already created
     * @param eventTableMap               map of event tables
     * @return finder having the capability of finding events at the processor against the expression and incoming
     * matchingEvent
     */
    @Override public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder,
            ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors,
            Map<String, EventTable> eventTableMap) {
        return OperatorParser
                .constructOperator(expiredEventChunk, expression, matchingMetaStateHolder, executionPlanContext,
                        variableExpressionExecutors, eventTableMap);
    }

    /**
     * This will be called only once and this can be used to acquire
     * required resources for the processing element.
     * This will be called after initializing the system and before
     * starting to process the events.
     */
    @Override public void start() {
        //Do nothing
    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override public void stop() {
        //Do nothing
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as an array
     */
    @Override public Object[] currentState() {
        return new Object[] { minByMaxByExecutor.getSortedEventMap() };
    }

    /**
     * Used to restore serialized state of the processing element, for reconstructing
     * the element to the same state as if was on a previous point of time.
     *
     * @param state the stateful objects of the element as an array on
     *              the same order provided by currentState().
     */
    @Override public void restoreState(Object[] state) {
        minByMaxByExecutor.setSortedEventMap((TreeMap) state[0]);
    }
}

