package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

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
 */

public abstract class MaxByMinByTimeWindowProcessor extends WindowProcessor implements SchedulingProcessor, FindableProcessor {

    protected String sortType;
    protected String windowType;
    private long timeInMilliSeconds;
    private Scheduler scheduler;
    private ExecutionPlanContext executionPlanContext;
    private volatile long lastTimestamp = Long.MIN_VALUE;
    private ExpressionExecutor sortByAttribute;
    private StreamEvent currentEvent;
    private StreamEvent expiredEvent;
    protected String timeWindowType;
    private MaxByMinByExecutor minByMaxByExecutor;


    public void setTimeInMilliSeconds(long timeInMilliSeconds) {
        this.timeInMilliSeconds = timeInMilliSeconds;
    }

    @Override
    public Scheduler getScheduler() {
        return scheduler;
    }

    @Override
    public void setScheduler(Scheduler scheduler) {
        this.scheduler = scheduler;
    }

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
        minByMaxByExecutor=new MaxByMinByExecutor();
        if (attributeExpressionExecutors.length == 2) {
            Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();
            sortByAttribute = attributeExpressionExecutors[0];
            if (!((attributeType == Attribute.Type.DOUBLE)
                    || (attributeType == Attribute.Type.INT)
                    || (attributeType == Attribute.Type.FLOAT)
                    || (attributeType == Attribute.Type.LONG)
                    || (attributeType == Attribute.Type.STRING))) {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the first argument of " +
                        windowType +
                        " required " + Attribute.Type.INT + " or " + Attribute.Type.LONG +
                        " or " + Attribute.Type.FLOAT + " or " + Attribute.Type.DOUBLE +
                        " or " + Attribute.Type.STRING +
                        ", but found " + attributeType.toString());
            }

            if (attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor) {
                if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.INT) {
                    timeInMilliSeconds = (Integer) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();

                } else if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.LONG) {
                    timeInMilliSeconds = (Long) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();
                } else {
                    throw new ExecutionPlanValidationException("Time parameter should be either int or long, but found " +
                            attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new ExecutionPlanValidationException("Time parameter should have constant parameter attribute but found a dynamic attribute " +
                        attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else {
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to " +
                    windowType + ", " +
                    "required 2, but found " + attributeExpressionExecutors.length + " input attributes");
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        synchronized (this) {
            StreamEvent streamEvent = null;
            while (streamEventChunk.hasNext()) {
                streamEvent = streamEventChunk.next();
                long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
                //Get the set of entries
                Set set = minByMaxByExecutor.getSortedEventMap().entrySet();
                // Get an iterator
                Iterator iterator = set.iterator();
                // Iterate through the sortedEventMap and remove the expired events
                while (iterator.hasNext()) {
                    Map.Entry entry = (Map.Entry) iterator.next();
                    StreamEvent expiredEvent = (StreamEvent) entry.getValue();
                    long timeDiff = expiredEvent.getTimestamp() - currentTime + timeInMilliSeconds;
                    if (timeDiff <= 0) {
                        expiredEvent.setType(StreamEvent.Type.EXPIRED);
                        expiredEvent.setTimestamp(currentTime);
                        iterator.remove();
                    } else {
                        break;
                    }
                }
                if (streamEvent.getType() == StreamEvent.Type.CURRENT) {
                    //clone the current stream event
                    //add the event to the sorted event map
                    StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                    minByMaxByExecutor.insert(clonedEvent , sortByAttribute.execute(clonedEvent));
                    if (lastTimestamp < clonedEvent.getTimestamp()) {
                        scheduler.notifyAt(clonedEvent.getTimestamp() + timeInMilliSeconds);
                        lastTimestamp = clonedEvent.getTimestamp();
                    }
                } else {
                    streamEventChunk.remove();
                }
            }
            streamEventChunk.clear();
            if (streamEvent != null && streamEvent.getType() == StreamEvent.Type.CURRENT) {
                StreamEvent tempEvent;
                if (timeWindowType.equals(Constants.MIN_BY)){
                    tempEvent = minByMaxByExecutor.getResult("MIN");
                } else {
                    tempEvent = minByMaxByExecutor.getResult("MAX");
                }
                if (tempEvent != currentEvent) {
                    expiredEvent = currentEvent;
                    currentEvent = tempEvent;
                    if (currentEvent != null) {
                        streamEventChunk.add(currentEvent);
                    }
                }
            }
            if (outputExpectsExpiredEvents){
                if (expiredEvent != null && expiredEvent.getType() == StreamEvent.Type.EXPIRED){
                    streamEventChunk.add(expiredEvent);
                }
            }
            if (expiredEvent != null){
                expiredEventChunk.clear();
                expiredEventChunk.add(expiredEvent);
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, minByMaxByExecutor.getSortedEventMap(), streamEventCloner);
    }

    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder, ExecutionPlanContext executionPlanContext,
                                  List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap) {
        return OperatorParser.constructOperator(minByMaxByExecutor.getSortedEventMap(), expression, matchingMetaStateHolder,
                executionPlanContext, variableExpressionExecutors, eventTableMap);
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
        return new Object[]{minByMaxByExecutor.getSortedEventMap()};
    }

    @Override
    public void restoreState(Object[] state) {

        //minByMaxByExecutor.setSortedEventMap((TreeMap) state[0]);
    }
}

