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

public abstract class MinByMaxByTimeWindowProcessor extends WindowProcessor implements SchedulingProcessor, FindableProcessor {

    private long timeInMilliSeconds;
    private Scheduler scheduler;
    private ExecutionPlanContext executionPlanContext;
    private volatile long lastTimestamp = Long.MIN_VALUE;
    private ExpressionExecutor sortByAttribute;
    private TreeMap<Object, StreamEvent> sortedEventMap = new TreeMap();
    private StreamEvent currentEvent;
    private StreamEvent expiredEvent;
    protected String timeWindowType;

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
        if (attributeExpressionExecutors.length == 2) {
            Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();
            sortByAttribute = attributeExpressionExecutors[0];
            if (!((attributeType == Attribute.Type.DOUBLE)
                    || (attributeType == Attribute.Type.INT)
                    || (attributeType == Attribute.Type.FLOAT)
                    || (attributeType == Attribute.Type.LONG))) {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the first argument of " +
                        timeWindowType +
                        " required " + Attribute.Type.INT + " or " + Attribute.Type.LONG +
                        " or " + Attribute.Type.FLOAT + " or " + Attribute.Type.DOUBLE +
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
            throw new ExecutionPlanValidationException("Invalid no of arguments passed to "+
                    timeWindowType+ ", " +
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
                Set set = sortedEventMap.entrySet();
                // Get an iterator
                Iterator iterator = set.iterator();
                // Iterate through the sortedEventMap and remove the expired events
                while(iterator.hasNext()) {
                    Map.Entry entry = (Map.Entry)iterator.next();
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
                    MaxByMinByExecutor.insert(clonedEvent , sortByAttribute.execute(clonedEvent));
                    if (lastTimestamp < clonedEvent.getTimestamp()) {
                        scheduler.notifyAt(clonedEvent.getTimestamp() + timeInMilliSeconds);
                        lastTimestamp = clonedEvent.getTimestamp();
                    }
                }
                else {
                    streamEventChunk.remove();
                }
            }
            streamEventChunk.clear();
            if (streamEvent != null && streamEvent.getType() == StreamEvent.Type.CURRENT) {
                StreamEvent tempEvent;
                if (timeWindowType.equals(Constants.MIN_BY)){
                    tempEvent = MaxByMinByExecutor.getResult("MIN");
                } else {
                    tempEvent = MaxByMinByExecutor.getResult("MAX");
                }
                if(tempEvent != currentEvent){
                    currentEvent = tempEvent;
                    expiredEvent = currentEvent;
                    if(currentEvent != null){
                        streamEventChunk.add(currentEvent);
                    }
                }
            }
            if (outputExpectsExpiredEvents){
                if (expiredEvent != null && expiredEvent.getType() == StreamEvent.Type.EXPIRED){
                    streamEventChunk.add(expiredEvent);
                }
            }
        }
        nextProcessor.process(streamEventChunk);
    }

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, sortedEventMap, streamEventCloner);
    }

    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder, ExecutionPlanContext executionPlanContext,
                                  List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap) {
        return OperatorParser.constructOperator(sortedEventMap, expression, matchingMetaStateHolder,
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
        return new Object[]{sortedEventMap};
    }

    @Override
    public void restoreState(Object[] state) {
        this.sortedEventMap = ((TreeMap) state[0]);
    }
}

