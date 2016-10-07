package org.wso2.extension.siddhi.window.minbymaxby.TimeWindow;

import org.wso2.extension.siddhi.window.minbymaxby.MaxByMinByConstants;
import org.wso2.extension.siddhi.window.minbymaxby.MaxByMinByExecutor;
import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
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

import java.util.List;
import java.util.Map;

/**
 * Abstract class which gives the min/max event in a Time Batch Window
 */

public abstract class MaxByMinByTimeBatchWindowProcessor extends WindowProcessor implements SchedulingProcessor, FindableProcessor {
    protected String maxByMinByType;
    protected String windowType;
    private long timeInMilliSeconds;
    private long nextEmitTime = -1;
    private StreamEvent resetEvent = null;
    private Scheduler scheduler;
    private ExecutionPlanContext executionPlanContext;
    private boolean isStartTimeEnabled = false;
    private long startTime = 0;
    private ExpressionExecutor sortByAttribute;
    private StreamEvent currentEvent = null;
    private ComplexEventChunk<StreamEvent> expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);

    @Override
    protected void init(ExpressionExecutor[] attributeExpressionExecutors, ExecutionPlanContext executionPlanContext) {
        this.executionPlanContext = executionPlanContext;
        if (attributeExpressionExecutors.length == 2) {
            Attribute.Type attributeType = attributeExpressionExecutors[0].getReturnType();
            sortByAttribute = attributeExpressionExecutors[0];
            if (!((attributeType == Attribute.Type.DOUBLE)
                    || (attributeType == Attribute.Type.INT)
                    || (attributeType == Attribute.Type.FLOAT)
                    || (attributeType == Attribute.Type.LONG)
                    || (attributeType == Attribute.Type.STRING))) {
                throw new ExecutionPlanValidationException("Invalid parameter type found for the first argument of " +
                        windowType + " " +
                        "required " + Attribute.Type.INT + " or " + Attribute.Type.LONG +
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
                    throw new ExecutionPlanValidationException("Time Batch window's parameter attribute should be either int or long, but found " +
                            attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new ExecutionPlanValidationException("Time Batch window should have constant parameter attribute but found a dynamic attribute " +
                        attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
        } else if (attributeExpressionExecutors.length == 3) {
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
                    throw new ExecutionPlanValidationException("Time window's parameter attribute should be either int or long, but found " +
                            attributeExpressionExecutors[1].getReturnType());
                }
            } else {
                throw new ExecutionPlanValidationException("Time window should have constant parameter attribute but found a dynamic attribute " +
                        attributeExpressionExecutors[1].getClass().getCanonicalName());
            }
            // start time
            isStartTimeEnabled = true;
            if (attributeExpressionExecutors[2].getReturnType() == Attribute.Type.INT) {
                startTime = Integer.parseInt(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
            } else {
                startTime = Long.parseLong(String.valueOf(((ConstantExpressionExecutor) attributeExpressionExecutors[2]).getValue()));
            }
        } else {
            throw new ExecutionPlanValidationException(windowType + " should only have two or three parameters. but found " +
                    attributeExpressionExecutors.length + " input attributes");
        }
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        synchronized (this) {
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            if (nextEmitTime == -1) {
                if (isStartTimeEnabled) {
                    nextEmitTime = getNextEmitTime(currentTime);
                } else {
                    nextEmitTime = executionPlanContext.getTimestampGenerator().currentTime() + timeInMilliSeconds;
                }
                scheduler.notifyAt(nextEmitTime);
            }

            boolean sendEvents;
            if (currentTime >= nextEmitTime) {
                nextEmitTime += timeInMilliSeconds;
                scheduler.notifyAt(nextEmitTime);
                sendEvents = true;
            } else {
                sendEvents = false;
            }

            //for each event, set the min/max event as current event
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
                if (streamEvent.getType() != ComplexEvent.Type.CURRENT) {
                    continue;
                }
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                if (maxByMinByType.equals(MaxByMinByConstants.MIN_BY)) {
                    currentEvent = MaxByMinByExecutor.getMinEventBatchProcessor(clonedStreamEvent, currentEvent, sortByAttribute);
                } else if (maxByMinByType.equals(MaxByMinByConstants.MAX_BY)) {
                    currentEvent = MaxByMinByExecutor.getMaxEventBatchProcessor(clonedStreamEvent, currentEvent, sortByAttribute);
                }
            }
            streamEventChunk.clear();
            if (sendEvents) {
                if (outputExpectsExpiredEvents) {
                    if (expiredEventChunk.getFirst() != null) {
                        while (expiredEventChunk.hasNext()) {
                            StreamEvent expiredEvent = expiredEventChunk.next();
                            expiredEvent.setTimestamp(currentTime);
                        }
                        streamEventChunk.add(expiredEventChunk.getFirst());
                    }
                }
                if (expiredEventChunk != null) {
                    expiredEventChunk.clear();
                }
                if (currentEvent != null) {

                    // add reset event in front of current events
                    streamEventChunk.add(resetEvent);
                    resetEvent = null;
                    StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                    toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                    expiredEventChunk.add(toExpireEvent);
                    resetEvent = streamEventCloner.copyStreamEvent(currentEvent);
                    resetEvent.setType(ComplexEvent.Type.RESET);
                    streamEventChunk.add(currentEvent);
                }
                currentEvent = null;
            }
        }
        if (streamEventChunk.getFirst() != null) {
            streamEventChunk.setBatch(true);
            nextProcessor.process(streamEventChunk);
            streamEventChunk.setBatch(false);
        }
    }

    private long getNextEmitTime(long currentTime) {
        // returns the next emission time based on system clock round time values.
        long elapsedTimeSinceLastEmit = (currentTime - startTime) % timeInMilliSeconds;
        long emitTime = currentTime + (timeInMilliSeconds - elapsedTimeSinceLastEmit);
        return emitTime;
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
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        //Do nothing
    }

    @Override
    public Object[] currentState() {
        if (expiredEventChunk != null) {
            return new Object[]{currentEvent, expiredEventChunk.getFirst(), resetEvent};
        } else {
            return new Object[]{currentEvent, resetEvent};
        }
    }

    @Override
    public void restoreState(Object[] state) {
        if (state.length > 2) {
            currentEvent = (StreamEvent)state[0];
            expiredEventChunk.clear();
            expiredEventChunk.add((StreamEvent) state[1]);
            resetEvent = (StreamEvent) state[2];
        } else {
            currentEvent = (StreamEvent) state[0];
            resetEvent = (StreamEvent) state[1];
        }
    }

    @Override
    public synchronized StreamEvent find(StateEvent matchingEvent, Finder finder) {
        return finder.find(matchingEvent, expiredEventChunk, streamEventCloner);
    }

    @Override
    public Finder constructFinder(Expression expression, MatchingMetaStateHolder matchingMetaStateHolder, ExecutionPlanContext executionPlanContext,
                                  List<VariableExpressionExecutor> variableExpressionExecutors, Map<String, EventTable> eventTableMap) {
        if (expiredEventChunk == null) {
            expiredEventChunk = new ComplexEventChunk<StreamEvent>(false);
        }
        return OperatorParser.constructOperator(expiredEventChunk, expression, matchingMetaStateHolder, executionPlanContext, variableExpressionExecutors, eventTableMap);
    }

}
