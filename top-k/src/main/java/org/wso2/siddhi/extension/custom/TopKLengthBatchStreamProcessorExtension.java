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
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.ExecutionPlanValidationException;

import java.util.ArrayList;
import java.util.List;

public class TopKLengthBatchStreamProcessorExtension extends StreamProcessor {
    private int windowLength;
    private int querySize;
    private int count;
    private VariableExpressionExecutor attrVariableExpressionExecutor;
    private StreamSummary<Object> topKFinder;

    @Override
    protected List<Attribute> init(AbstractDefinition abstractDefinition, ExpressionExecutor[] attributeExpressionExecutors,
                                   ExecutionPlanContext executionPlanContext) {
        if (attributeExpressionExecutors.length == 3) {
            this.executionPlanContext = executionPlanContext;
            count = 0;
            attrVariableExpressionExecutor = (VariableExpressionExecutor) attributeExpressionExecutors[0];
        } else {
            throw new ExecutionPlanValidationException("3 arguments should be passed to TopKLengthBatchWindowProcessor, " +
                    "but found " + attributeExpressionExecutors.length);
        }

        // Checking the window length parameter
        if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
            Attribute.Type attributeType = attributeExpressionExecutors[1].getReturnType();
            if (attributeType == Attribute.Type.INT) {
                windowLength = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                topKFinder = new StreamSummary<Object>(windowLength);
            } else {
                throw new ExecutionPlanValidationException("Window length parameter for TopKLengthBatchWindowProcessor " +
                        "should be int. but found " + attributeType);
            }
        } else {
            throw new ExecutionPlanValidationException("Window length parameter for TopKLengthBatchWindowProcessor " +
                    "should be a constant. but found a dynamic attribute " +
                    attributeExpressionExecutors[1].getClass().getCanonicalName());
        }

        // Checking the query size parameter
        if (attributeExpressionExecutors[2] instanceof ConstantExpressionExecutor) {
            Attribute.Type attributeType = attributeExpressionExecutors[2].getReturnType();
            if (attributeType == Attribute.Type.INT) {
                querySize = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue();
            } else {
                throw new ExecutionPlanValidationException("Query size parameter for TopKLengthBatchWindowProcessor " +
                        "should be int. but found " + attributeType);
            }
        } else {
            throw new ExecutionPlanValidationException("Query size parameter for TopKLengthBatchWindowProcessor " +
                    "should be a constant. but found a dynamic attribute " +
                    attributeExpressionExecutors[2].getClass().getCanonicalName());
        }

        // Generating the list of attributes
        List<Attribute> newAttributes = new ArrayList<Attribute>();
        for (int i = 0; i < querySize; i++) {
            newAttributes.add(new Attribute("top" + i + "Element", attrVariableExpressionExecutor.getReturnType()));
            newAttributes.add(new Attribute("top" + i + "Frequency", Attribute.Type.LONG));
        }
        return newAttributes;
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {
        synchronized (this) {
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);

                if (count == windowLength) {
                    topKFinder = new StreamSummary<Object>(windowLength);
                    count = 0;
                }

                topKFinder.offer(attrVariableExpressionExecutor.execute(clonedStreamEvent));
                count++;

                List<Counter<Object>> topKCounters = topKFinder.topK(querySize);
                Object[] outputStreamEventData = new Object[2 * querySize];
                int i = 0;
                while (i < topKCounters.size()) {
                    Counter<Object> topKCounter = topKCounters.get(i);
                    outputStreamEventData[2 * i] = topKCounter.getItem();
                    outputStreamEventData[2 * i + 1] = topKCounter.getCount();
                    i++;
                }

                complexEventPopulater.populateComplexEvent(streamEvent, outputStreamEventData);
            }
        }

        nextProcessor.process(streamEventChunk);
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
            return new Object[]{topKFinder, windowLength, querySize};
        }
    }

    @Override
    public void restoreState(Object[] state) {
        synchronized (this) {
            topKFinder = (StreamSummary<Object>) state[0];
            windowLength = (Integer) state[1];
            querySize = (Integer) state[2];
        }
    }
}
