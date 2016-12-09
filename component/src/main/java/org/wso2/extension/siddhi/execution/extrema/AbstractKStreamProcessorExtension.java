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
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.extension.siddhi.execution.extrema.util.BottomKFinder;
import org.wso2.extension.siddhi.execution.extrema.util.TopKFinder;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.List;

/**
 * Sample Query (For topKLengthBatch implementation):
 * from inputStream#window.length(6)#extrema:topKTimeBatch(attribute1, 3)
 * select attribute1, attribute2
 * insert into outputStream;
 *
 * Sample Query (For bottomKLengthBatch implementation):
 * from inputStream#window.timeBatch(6)#extrema:bottomKTimeBatch(attribute1, 3)
 * select attribute1, attribute2
 * insert into outputStream;
 *
 * Description:
 * In the example query given, 3 is the k-value and attribute1 is the attribute of which the frequency is counted.
 * The frequencies of the values received for the attribute given will be counted by this and the topK/bottomK values will be emitted
 * Events will be only emitted if there is a change in the topK/bottomK results for each received chunk of events
 */
public abstract class AbstractKStreamProcessorExtension extends StreamProcessor {
    private int querySize;

    private VariableExpressionExecutor attrVariableExpressionExecutor;
    private AbstractTopKBottomKFinder<Object> topKBottomKFinder;

    private Object[] lastOutputData;
    private StreamEvent lastStreamEvent = null;
    private ComplexEventChunk<StreamEvent> expiredEventChunk = null;

    /**
     * The init method of the AbstractKStreamProcessor, this method will be called before other methods
     *
     * @param attributeExpressionExecutors the executors of each function parameters
     * @param executionPlanContext         the context of the execution plan
     */
    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                   ExecutionPlanContext executionPlanContext) {
        if (this.attributeExpressionExecutors.length == 2) {
            expiredEventChunk = new ComplexEventChunk<StreamEvent>(true);
            topKBottomKFinder = createNewTopKBottomKFinder();
        } else {
            throw new ExecutionPlanValidationException("2 arguments should be " +
                    "passed to " + getExtensionNamePrefix() + "KStreamProcessor, but found " + this.attributeExpressionExecutors.length);
        }

        // Checking the topK/bottomK attribute
        if (this.attributeExpressionExecutors[0] instanceof VariableExpressionExecutor) {
            attrVariableExpressionExecutor = (VariableExpressionExecutor) this.attributeExpressionExecutors[0];
        } else {
            throw new ExecutionPlanValidationException("Attribute for ordering in " +
                    getExtensionNamePrefix() + "KStreamProcessor should be a variable. but found a constant attribute " +
                    this.attributeExpressionExecutors[1].getClass().getCanonicalName());
        }

        // Checking the query size parameter
        if (this.attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            Attribute.Type attributeType = this.attributeExpressionExecutors[1].getReturnType();
            if (attributeType == Attribute.Type.INT) {
                querySize = (Integer) ((ConstantExpressionExecutor) this.attributeExpressionExecutors[1]).getValue();
            } else {
                throw new ExecutionPlanValidationException("Query size parameter for " +
                        getExtensionNamePrefix() + "KStreamProcessor should be INT. but found " + attributeType);
            }
        } else {
            throw new ExecutionPlanValidationException("Query size parameter for " +
                    getExtensionNamePrefix() + "KStreamProcessor should be a constant. but found a dynamic attribute " +
                    this.attributeExpressionExecutors[2].getClass().getCanonicalName());
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
            StreamEvent resetEvent = null;
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);

                // Current event arrival tasks
                if (streamEvent.getType() == ComplexEvent.Type.CURRENT) {
                    lastStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    topKBottomKFinder.offer(attrVariableExpressionExecutor.execute(clonedStreamEvent));
                } else if (streamEvent.getType() == ComplexEvent.Type.EXPIRED) {
                    topKBottomKFinder.offer(attrVariableExpressionExecutor.execute(clonedStreamEvent), -1);
                } else if (streamEvent.getType() == ComplexEvent.Type.RESET) {
                    // Setting the reset event to be used in the end of the window
                    resetEvent = streamEventCloner.copyStreamEvent(clonedStreamEvent);

                    // Resetting topK/bottomK finder for batch windows using RESET event
                    topKBottomKFinder = createNewTopKBottomKFinder();
                }
            }

            if (expiredEventChunk.getFirst() != null) {
                // Adding expired events
                while (expiredEventChunk.hasNext()) {
                    outputStreamEventChunk.add(expiredEventChunk.next());
                }
                expiredEventChunk.clear();
            }

            // Adding the reset event
            if (resetEvent != null) {
                outputStreamEventChunk.add(resetEvent);
            }

            // Adding the last event with the topK frequencies for the window
            List<Counter<Object>> topKCounters = topKBottomKFinder.get(querySize);
            Object[] outputStreamEventData = new Object[2 * querySize];
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

                // Setting the event expired in this window
                StreamEvent expiredStreamEvent = streamEventCloner.copyStreamEvent(lastStreamEvent);
                expiredStreamEvent.setTimestamp(currentTime);
                expiredStreamEvent.setType(ComplexEvent.Type.EXPIRED);
                expiredEventChunk.add(expiredStreamEvent);
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
        // Do nothing
    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {
        // Do nothing
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
                return new Object[]{topKBottomKFinder, querySize, lastStreamEvent, lastOutputData, expiredEventChunk};
            } else {
                return new Object[]{topKBottomKFinder, querySize, lastStreamEvent, lastOutputData};
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
            querySize = (Integer) state[1];

            lastStreamEvent = (StreamEvent) state[2];
            lastOutputData = (Object[]) state[3];
            if (state.length == 5) {
                expiredEventChunk = (ComplexEventChunk<StreamEvent>) state[4];
            }
        }
    }

    protected abstract AbstractTopKBottomKFinder<Object> createNewTopKBottomKFinder();

    protected abstract String getExtensionNamePrefix();
}
