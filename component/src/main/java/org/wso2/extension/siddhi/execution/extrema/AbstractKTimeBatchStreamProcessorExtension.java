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

package org.wso2.extension.siddhi.execution.extrema;

import org.wso2.extension.siddhi.execution.extrema.util.AbstractTopKBottomKFinder;
import org.wso2.extension.siddhi.execution.extrema.util.Counter;
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
import org.wso2.siddhi.core.query.processor.SchedulingProcessor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.query.processor.stream.window.FindableProcessor;
import org.wso2.siddhi.core.table.EventTable;
import org.wso2.siddhi.core.util.Scheduler;
import org.wso2.siddhi.core.util.collection.operator.Finder;
import org.wso2.siddhi.core.util.collection.operator.MatchingMetaStateHolder;
import org.wso2.siddhi.core.util.parser.OperatorParser;
import org.wso2.extension.siddhi.execution.extrema.util.BottomKFinder;
import org.wso2.extension.siddhi.execution.extrema.util.TopKFinder;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Sample Query (For topKLengthBatch implementation):
 * from inputStream#extrema:topKTimeBatch(attribute1, 1 sec, 3)
 * select attribute1, attribute2
 * insert into outputStream;
 *
 * Sample Query (For bottomKLengthBatch implementation):
 * from inputStream#extrema:bottomKTimeBatch(attribute1, 1 sec, 3)
 * select attribute1, attribute2
 * insert into outputStream;
 *
 * Description:
 * In the example query given, 1 sec is the duration of the window, 3 is the k-value and attribute1 is the attribute of which the frequency is counted.
 * The frequencies of the values received for the attribute given will be counted by this and the topK/bottomK values will be emitted per batch.
 * Events will not emit if there is no change from the last send topK/bottomK results
 */
public abstract class AbstractKTimeBatchStreamProcessorExtension extends StreamProcessor implements SchedulingProcessor, FindableProcessor {
    private long windowTime;
    private int querySize;
    private long startTime = 0L;

    private VariableExpressionExecutor attrVariableExpressionExecutor;
    private Scheduler scheduler;
    private AbstractTopKBottomKFinder<Object> topKBottomKFinder;

    private Object[] lastOutputData;
    private StreamEvent lastStreamEvent = null;
    private StreamEvent resetEvent = null;
    private ComplexEventChunk<StreamEvent> expiredEventChunk = null;

    /**
     * The init method of the AbstractKTimeBatchStreamProcessor, this method will be called before other methods
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     */
    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                   ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length == 3 ||
                attributeExpressionExecutors.length == 4) {
            expiredEventChunk = new ComplexEventChunk<StreamEvent>(true);
        } else {
            throw new ExecutionPlanValidationException("3 arguments (4 arguments if start time is also specified) should be " +
                    "passed to " + getExtensionNamePrefix() + "KTimeBatchStreamProcessor, but found " + attributeExpressionExecutors.length);
        }

        // Checking the topK/bottomK attribute
        if (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
            attrVariableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        } else {
            throw new ExecutionPlanValidationException("Attribute for ordering in " +
                    getExtensionNamePrefix() + "KTimeBatchStreamProcessor should be a variable. but found a constant attribute " +
                    attributeExpressionExecutors[1].getClass().getCanonicalName());
        }

        // Checking the window time parameter
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            Attribute.Type attributeType = attributeExpressionExecutors[1].getReturnType();
            if (attributeType == Attribute.Type.LONG) {
                windowTime = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else if (attributeType == Attribute.Type.INT) {
                windowTime = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
            } else {
                throw new ExecutionPlanValidationException("Window time parameter for " +
                        getExtensionNamePrefix() + "KTimeBatchStreamProcessor should be INT or LONG. but found " + attributeType);
            }
        } else {
            throw new ExecutionPlanValidationException("Window time parameter for " +
                    getExtensionNamePrefix() + "KTimeBatchStreamProcessor should be a constant. but found a dynamic attribute " +
                    attributeExpressionExecutors[1].getClass().getCanonicalName());
        }

        // Checking the query size parameter
        if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            Attribute.Type attributeType = attributeExpressionExecutors[2].getReturnType();
            if (attributeType == Attribute.Type.INT) {
                querySize = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new ExecutionPlanValidationException("Query size parameter for " +
                        getExtensionNamePrefix() + "KTimeBatchStreamProcessor should be INT. but found " + attributeType);
            }
        } else {
            throw new ExecutionPlanValidationException("Query size parameter for " +
                    getExtensionNamePrefix() + "KTimeBatchStreamProcessor should be a constant. but found a dynamic attribute " +
                    attributeExpressionExecutors[2].getClass().getCanonicalName());
        }

        if (attributeExpressionExecutors.length == 4) {
            if (attributeExpressionExecutors[3] instanceof ConstantExpressionExecutor) {
                Attribute.Type attributeType = attributeExpressionExecutors[3].getReturnType();
                if (attributeType == Attribute.Type.INT) {
                    startTime = executionPlanContext.getTimestampGenerator().currentTime() +
                            (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[3]).getValue();
                } else {
                    throw new ExecutionPlanValidationException("Start time parameter for " +
                            getExtensionNamePrefix() + "KTimeBatchStreamProcessor should be INT. but found " + attributeType);
                }
            }
        }

        // Generating the list of attributes
        List<Attribute> newAttributes = new ArrayList<Attribute>();
        for (int i = 0; i < querySize; i++) {
            newAttributes.add(new Attribute(getExtensionNamePrefix() + (i + 1) + "Element", attrVariableExpressionExecutor.getReturnType()));
            newAttributes.add(new Attribute(getExtensionNamePrefix() + (i + 1) + "Frequency", Attribute.Type.LONG));
        }
        return newAttributes;
    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param streamEventChunk  the stream event chunk that need to be processed
     * @param nextProcessor     the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     * @param complexEventPopulater helps to add attributes to the events before sending the chunk to the next processor
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
        synchronized (this) {
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            if (currentTime > startTime) {
                while (streamEventChunk.hasNext()) {
                    StreamEvent streamEvent = streamEventChunk.next();
                    StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);

                    // Starting the time batch windows
                    if (topKBottomKFinder == null) {
                        topKBottomKFinder = createNewTopKBottomKFinder();
                        scheduler.notifyAt(currentTime + windowTime);
                    }

                    // New current event tasks
                    if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                        lastStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                        topKBottomKFinder.offer(attrVariableExpressionExecutor.execute(clonedStreamEvent));
                    }

                    // End of window tasks
                    if (streamEvent.getType() == ComplexEvent.Type.TIMER) {
                        if (expiredEventChunk.getFirst() != null) {
                            // Adding the expired events
                            if (outputExpectsExpiredEvents) {
                                outputStreamEventChunk.add(expiredEventChunk.getFirst());
                                expiredEventChunk.clear();
                            }

                            // Adding the reset event
                            outputStreamEventChunk.add(resetEvent);
                            resetEvent = null;
                        }

                        // Adding the last event with the topK frequencies for the window
                        if (lastStreamEvent != null) {
                            Object[] outputStreamEventData = new Object[2 * querySize];
                            List<Counter<Object>> topKCounters = topKBottomKFinder.get(querySize);
                            boolean sendEvents = false;
                            int i = 0;
                            while (i < topKCounters.size()) {
                                Counter<Object> topKCounter = topKCounters.get(i);
                                outputStreamEventData[2 * i] = topKCounter.getItem();
                                outputStreamEventData[2 * i + 1] = topKCounter.getCount();
                                if (lastOutputData == null ||
                                        lastOutputData[2 * i] != outputStreamEventData[2 * i] ||
                                        lastOutputData[2 * i + 1] != outputStreamEventData[2 * i + 1]) {
                                    sendEvents = true;
                                }
                                i++;
                            }
                            if (sendEvents) {
                                lastOutputData = outputStreamEventData;
                                complexEventPopulater.populateComplexEvent(lastStreamEvent, outputStreamEventData);
                                outputStreamEventChunk.add(lastStreamEvent);

                                // Setting the event to be expired in the next window
                                StreamEvent expiredStreamEvent = streamEventCloner.copyStreamEvent(lastStreamEvent);
                                expiredStreamEvent.setTimestamp(currentTime);
                                expiredStreamEvent.setType(ComplexEvent.Type.EXPIRED);
                                expiredEventChunk.add(expiredStreamEvent);
                                lastStreamEvent = null;
                            }
                        }

                        // Resetting window
                        topKBottomKFinder = createNewTopKBottomKFinder();
                        scheduler.notifyAt(currentTime + windowTime);

                        // Setting the reset event to be used in the end of the window
                        if (resetEvent == null) {
                            resetEvent = streamEventCloner.copyStreamEvent(streamEventChunk.getFirst());
                            resetEvent.setType(ComplexEvent.Type.RESET);
                        }
                    }
                }
            }
        }

        if (outputStreamEventChunk.getFirst() != null) {
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
        // Do Nothing
    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {
        // Do Nothing
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as an array
     */
    @Override
    public Object[] currentState() {
        synchronized (this) {
            if (outputExpectsExpiredEvents) {
                return new Object[]{topKBottomKFinder, windowTime, querySize, startTime, lastStreamEvent, resetEvent, expiredEventChunk};
            } else {
                return new Object[]{topKBottomKFinder, windowTime, querySize, startTime, lastStreamEvent, resetEvent};
            }
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
        synchronized (this) {
            topKBottomKFinder = (AbstractTopKBottomKFinder<Object>) state[0];
            windowTime = (Long) state[1];
            querySize = (Integer) state[2];
            startTime = (Long) state[3];

            lastStreamEvent = (StreamEvent) state[4];
            resetEvent = (StreamEvent) state[5];
            if (state.length == 7) {
                expiredEventChunk = (ComplexEventChunk<StreamEvent>) state[6];
            }
        }
    }

    /**
     * The getScheduler method of the AbstractKTimeBatchStreamProcessor, As scheduler is private variable, to access publicly we
     * use this getter method.
     */
    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    /**
     * The setScheduler method of the AbstractKTimeBatchStreamProcessor, As scheduler is private variable, to access publicly we
     * use this setter method.
     *
     * @param scheduler the value of scheduler.
     */
    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
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
        synchronized (this) {
            return finder.find(matchingEvent, expiredEventChunk, streamEventCloner);
        }
    }

    /**
     * To construct a finder having the capability of finding events at the processor that corresponds to the incoming
     * matchingEvent and the given matching expression logic.
     *
     * @param expression                  the matching expression
     * @param matchingMetaStateHolder    the meta structure of the incoming matchingEvent
     * @param executionPlanContext        current execution plan context
     * @param variableExpressionExecutors the list of variable ExpressionExecutors already created
     * @param eventTableMap               map of event tables
     * @return finder having the capability of finding events at the processor against the expression and incoming
     * matchingEvent
     */
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

    protected abstract AbstractTopKBottomKFinder<Object> createNewTopKBottomKFinder();

    protected abstract String getExtensionNamePrefix();
}
