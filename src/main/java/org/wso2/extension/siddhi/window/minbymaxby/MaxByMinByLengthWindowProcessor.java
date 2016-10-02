package org.wso2.extension.siddhi.window.minbymaxby;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEvent;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.MetaComplexEvent;
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
import org.wso2.siddhi.query.api.expression.Expression;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mathuriga on 30/09/16.
 */
public class MaxByMinByLengthWindowProcessor extends WindowProcessor implements FindableProcessor {
    private ExpressionExecutor funtionParameter;
    private int length;
    private int count = 0;
    private ComplexEventChunk<StreamEvent> expiredEventChunk = null;
    private ExecutionPlanContext executionPlanContext;
    private MaxByMinByExecutor maxByMinByExecutor;
    private StreamEvent outputStreamEvent;
    private List<StreamEvent> events = new ArrayList<StreamEvent>();


    public void setOutputStreamEvent(StreamEvent outputSreamEvent) {
        this.outputStreamEvent = outputSreamEvent;
    }

    @Override
    public StreamEvent find(ComplexEvent complexEvent, Finder finder) {
        return null;
    }

    @Override
    public Finder constructFinder(Expression expression, MetaComplexEvent metaComplexEvent, ExecutionPlanContext executionPlanContext, List<VariableExpressionExecutor> list, Map<String, EventTable> map, int i, long l) {
        return null;
    }

    @Override
    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
    }

    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext, String functionType) {
        this.executionPlanContext = executionPlanContext;
        maxByMinByExecutor = new MaxByMinByExecutor();
        if (functionType == "MIN") {
            maxByMinByExecutor.setFunctionType(functionType);
            if (attributeExpressionExecutors.length == 2) {
                funtionParameter = attributeExpressionExecutors[0];
                length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
            }
        } else {
            maxByMinByExecutor.setFunctionType(functionType);
            if (attributeExpressionExecutors.length == 2) {
                funtionParameter = attributeExpressionExecutors[0];
                length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
            }

        }
    }


    @Override
    protected void process(ComplexEventChunk<StreamEvent> complexEventChunk, Processor processor, StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);
            while (complexEventChunk.hasNext()) {
                StreamEvent streamEvent = complexEventChunk.next();
                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);

                if (count != 0) {
                    outputStreamEventChunk.clear();
                }

                //get the parameter value for every events
                Object parameterValue = getParameterValue(funtionParameter, streamEvent);
                maxByMinByExecutor.insert(clonedStreamEvent, parameterValue);
                //

                //clonedEvent.setType(StreamEvent.Type.EXPIRED);
                if (count < length) {
                    count++;
                    //get the output event
                    setOutputStreamEvent(maxByMinByExecutor.getResult(maxByMinByExecutor.getFunctionType()));
                    outputStreamEventChunk.add(outputStreamEvent);
                    System.out.println(outputStreamEventChunk);
                    if (outputStreamEventChunk.getFirst() != null) {
                        streamEventChunks.add(outputStreamEventChunk);
                    }
                    //
                    events.add(clonedStreamEvent);
                } else {
                    StreamEvent firstEvent = events.get(0);
                    if (firstEvent != null) {
                        firstEvent.setTimestamp(currentTime);

                        //remove the expired event from treemap
                        Object expiredEventParameterValue = getParameterValue(funtionParameter, firstEvent);
                        maxByMinByExecutor.getTreeMap().remove(expiredEventParameterValue);
                        events.remove(0);
                        //

                        //get the output event
                        setOutputStreamEvent(maxByMinByExecutor.getResult(maxByMinByExecutor.getFunctionType()));
                        outputStreamEventChunk.add(outputStreamEvent);
                        System.out.println(outputStreamEventChunk);
                        if (outputStreamEventChunk.getFirst() != null) {
                            streamEventChunks.add(outputStreamEventChunk);
                        }
                        //


                        //complexEventChunk.insertBeforeCurrent(firstEvent);
                        events.add(clonedStreamEvent);

                    }
                }
            }
        }

        for (ComplexEventChunk<StreamEvent> outputStreamEventChunk : streamEventChunks) {
            nextProcessor.process(outputStreamEventChunk);
        }

    }

    @Override
    public void start() {

    }

    @Override
    public void stop() {

    }

    @Override
    public Object[] currentState() {
        return new Object[]{new AbstractMap.SimpleEntry<String, Object>("ExpiredEventChunk", expiredEventChunk.getFirst()), new AbstractMap.SimpleEntry<String, Object>("Count", count)};
    }

    @Override
    public void restoreState(Object[] state) {
        expiredEventChunk.clear();
        Map.Entry<String, Object> stateEntry = (Map.Entry<String, Object>) state[0];
        expiredEventChunk.add((StreamEvent) stateEntry.getValue());
        Map.Entry<String, Object> stateEntry2 = (Map.Entry<String, Object>) state[1];
        count = (Integer) stateEntry2.getValue();
    }

    public Object getParameterValue(ExpressionExecutor funtionParameter, StreamEvent streamEvent) {
        Object parameterValue;

        parameterValue = funtionParameter.execute(streamEvent);

        return parameterValue;
    }


}
