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
import java.util.TreeMap;

/**
 * Abstract class which gives the event which holds minimum or maximum value corresponding to given attribute in a Length window.
 */
public abstract class MaxByMinByLengthWindowProcessor extends WindowProcessor implements FindableProcessor {
    /*
      Attribute which is used to find Extrema event
   */
    private ExpressionExecutor minByMaxByExecutorAttribute;
    /*
     * minByMaxByExecutorType holds the value to indicate MIN or MAX
     */
    String minByMaxByExecutorType;

    /*
    minByMaxByExecutor used to get extrema event
     */
    private MaxByMinByExecutor maxByMinByExecutor;
    private int length;
    private int count = 0;
    private ComplexEventChunk<StreamEvent> expiredEventChunk = null;
    private ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
    private ExecutionPlanContext executionPlanContext;
    private StreamEvent outputStreamEvent;
    /*
    to holds the list of events that should be in a sliding length window.
     */
    private List<StreamEvent> events = new ArrayList<StreamEvent>();
    private StreamEvent expiredEvent = null;


    /**
     * The init method of the WindowProcessor, this method will be called before other methods
     *
     * @param expressionExecutors  the executors of each function parameters
     * @param executionPlanContext the context of the execution plan
     */
    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {

        this.executionPlanContext = executionPlanContext;
        this.expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
        maxByMinByExecutor = new MaxByMinByExecutor();

        if (minByMaxByExecutorType.equals(MaxByMinByConstants.MIN_BY)) {
            maxByMinByExecutor.setMinByMaxByExecutorType(minByMaxByExecutorType);
        } else {
            maxByMinByExecutor.setMinByMaxByExecutorType(minByMaxByExecutorType);
        }

        if (attributeExpressionExecutors.length != 2) {
            throw new ExecutionPlanValidationException(
                    "Invalid no of arguments passed to minbymaxby:" + minByMaxByExecutorType + " window, "
                            + "required 2, but found " + attributeExpressionExecutors.length);
        }

        Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();

        if (!((attributeType == Attribute.Type.DOUBLE) || (attributeType == Attribute.Type.INT) || (attributeType
                == Attribute.Type.STRING) || (attributeType == Attribute.Type.FLOAT) || (attributeType
                == Attribute.Type.LONG) && (attributeExpressionExecutors[0] instanceof VariableExpressionExecutor))) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the first argument of minbymaxby:" + minByMaxByExecutorType
                            + " window, " + "required " + Attribute.Type.INT + " or " + Attribute.Type.LONG + " or "
                            + Attribute.Type.FLOAT + " or " + Attribute.Type.DOUBLE + "or" + Attribute.Type.STRING
                            + ", but found " + attributeType.toString() + " or first argument is not a Variable ");
        }
        attributeType = attributeExpressionExecutors[1].getReturnType();
        if (!(((attributeType == Attribute.Type.INT))
                && attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
            throw new ExecutionPlanValidationException(
                    "Invalid parameter type found for the second argument of minbymaxby:" + minByMaxByExecutorType
                            + " window, " + "required " + Attribute.Type.INT +
                            ", but found " + attributeType.toString() + " or second argument is not a constant");
        }


        minByMaxByExecutorAttribute = attributeExpressionExecutors[0];
        length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());


    }

    /**
     * The main processing method that will be called upon event arrival
     *
     * @param complexEventChunk the stream event chunk that need to be processed
     * @param processor         the next processor to which the success events need to be passed
     * @param streamEventCloner helps to clone the incoming event for local storage or modification
     */
    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk, Processor processor,
                           StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            while (complexEventChunk.hasNext()) {
                StreamEvent streamEvent = complexEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner
                        .copyStreamEvent(streamEvent); //this cloned event used to
                // insert the event into treemap

                if (count != 0) {
                    outputStreamEventChunk.clear();
                    expiredEventChunk.clear();
                }

                //get the parameter value for every events
                Object attributeValue = minByMaxByExecutorAttribute.execute(streamEvent);

                //insert the cloned event into tree map
                maxByMinByExecutor.insert(clonedStreamEvent, attributeValue);

                if (count < length) {
                    count++;

                    //get the output event
                    this.outputStreamEvent = maxByMinByExecutor.getResult(maxByMinByExecutor.getMinByMaxByExecutorType());

                    if (expiredEvent != null) {
                        if (outputStreamEvent.equals(expiredEvent)) {
                            expiredEvent.setTimestamp(currentTime);
                            expiredEvent.setType(StateEvent.Type.EXPIRED);
                            outputStreamEventChunk.add(expiredEvent);
                        }
                    }

                    outputStreamEventChunk.add(outputStreamEvent);
                    //add the event which is to be expired
                    // TODO: 15/12/16 change ex chunk to find mthod 
                    expiredEventChunk.add(streamEventCloner.copyStreamEvent(outputStreamEvent));
                    expiredEvent = outputStreamEvent;

                    //System.out.println(outputStreamEventChunk);
                    if (outputStreamEventChunk.getFirst() != null) {
                        streamEventChunks.add(outputStreamEventChunk);
                    }

                    events.add(clonedStreamEvent);
                } else {
                    StreamEvent firstEvent = events.get(0);
                    if (firstEvent != null) {
                        firstEvent.setTimestamp(currentTime);

                        //remove the expired event from treemap
                        Object expiredEventAttributeValue = minByMaxByExecutorAttribute.execute(streamEvent);

                        maxByMinByExecutor.getSortedEventMap().remove(expiredEventAttributeValue);
                        events.remove(0);

                        //get the output event
                        this.outputStreamEvent = maxByMinByExecutor.getResult(maxByMinByExecutor.getMinByMaxByExecutorType());
                        if (expiredEvent != null) {
                            if (outputStreamEvent != expiredEvent) {
                                expiredEvent.setTimestamp(currentTime);
                                expiredEvent.setType(StateEvent.Type.EXPIRED);
                                outputStreamEventChunk.add(expiredEvent);
                            }

                        }
                        outputStreamEventChunk.add(outputStreamEvent);
                        expiredEventChunk.add(streamEventCloner.copyStreamEvent(outputStreamEvent));
                        expiredEvent = outputStreamEvent;

                        //                        System.out.println(outputStreamEventChunk);
                        if (outputStreamEventChunk.getFirst() != null) {
                            streamEventChunks.add(outputStreamEventChunk);
                        }
                        events.add(clonedStreamEvent);

                    }
                }
            }
        }

        System.out.println(outputStreamEventChunk);
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
        //Do nothing
    }

    /**
     * This will be called only once and this can be used to release
     * the acquired resources for processing.
     * This will be called before shutting down the system.
     */
    @Override
    public void stop() {
        //Do nothing
    }

    /**
     * Used to collect the serializable state of the processing element, that need to be
     * persisted for the reconstructing the element to the same state on a different point of time
     *
     * @return stateful objects of the processing element as an array
     */
    @Override
    public Object[] currentState() {
        return new Object[]{maxByMinByExecutor.getSortedEventMap()};
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
        maxByMinByExecutor.setSortedEventMap((TreeMap<Object, StreamEvent>) state[0]);
    }

    /**
     * To find events from the processor event pool, that the matches the matchingEvent based on finder logic.
     *
     * @param matchingEvent the event to be matched with the events at the processor
     * @param finder        the execution element responsible for finding the corresponding events that matches
     *                      the matchingEvent based on pool of events at Processor
     * @return the matched events
     */
    public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, this.expiredEventChunk, this.streamEventCloner);
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
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder,
                                  ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> variableExpressionExecutors,
                                  Map<String, EventTable> eventTableMap) {
        return OperatorParser
                .constructOperator(this.expiredEventChunk, expression, matchingMetaStateHolder, executionPlanContext,
                        variableExpressionExecutors, eventTableMap);
    }
}
