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
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private ComplexEventChunk<StreamEvent> expiredEventChunk = null;
    private ExecutionPlanContext executionPlanContext;
    private StreamEvent resetEvent = null;
    private VariableExpressionExecutor[] variableExpressionExecutors;
    MaxByMinByExecutor maxByMinByExecutor = new MaxByMinByExecutor();
    private StreamEvent outputSreamEvent;
    private List<StreamEvent> events=new ArrayList<StreamEvent>();


    public void setOutputSreamEvent(StreamEvent outputSreamEvent) {
        this.outputSreamEvent = outputSreamEvent;
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

        //this.expiredEventChunk = new ComplexEventChunk(false);
        if (functionType == "MIN") {
            maxByMinByExecutor.setFunctionType(functionType);
            variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length - 1];
            if (attributeExpressionExecutors.length == 2) {
                variableExpressionExecutors[0] = (VariableExpressionExecutor) attributeExpressionExecutors[0];
                funtionParameter = variableExpressionExecutors[0];
                length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
            }
        } else {
            maxByMinByExecutor.setFunctionType(functionType);
            variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length - 1];
            if (attributeExpressionExecutors.length == 2) {
                variableExpressionExecutors[0] = (VariableExpressionExecutor) attributeExpressionExecutors[0];
                funtionParameter = variableExpressionExecutors[0];
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
               // StreamEvent clonedEvent = streamEventCloner.copyStreamEvent(streamEvent);
                StreamEvent clonedStreamEventTreeMap = streamEventCloner.copyStreamEvent(streamEvent);

                if(count==length){
                    outputStreamEventChunk.clear();
                }

                //get the parameter value for every events
                Object parameterValue = getParameterValue(funtionParameter, streamEvent);
                maxByMinByExecutor.insert(clonedStreamEventTreeMap,parameterValue);
                //

                //clonedEvent.setType(StreamEvent.Type.EXPIRED);
                if (count < length) {
                    count++;
                    if(count==length)
                    {
                        //get the output event
                        setOutputSreamEvent(maxByMinByExecutor.getResult(maxByMinByExecutor.getFunctionType()));
                        outputStreamEventChunk.add(outputSreamEvent);
                        System.out.println(outputStreamEventChunk);
                        if (outputStreamEventChunk.getFirst() != null) {
                            streamEventChunks.add(outputStreamEventChunk);
                        }
                        //
                    }
                    events.add(clonedStreamEventTreeMap);
                    //this.expiredEventChunk.add(clonedEvent);
                }
               else {
                    StreamEvent firstEvent = events.get(0);
                    if (firstEvent != null) {
                        firstEvent.setTimestamp(currentTime);

                        //get the output event
                        setOutputSreamEvent(maxByMinByExecutor.getResult(maxByMinByExecutor.getFunctionType()));
                        outputStreamEventChunk.add(outputSreamEvent);
                        System.out.println(outputStreamEventChunk);
                        if (outputStreamEventChunk.getFirst() != null) {
                            streamEventChunks.add(outputStreamEventChunk);
                        }
                        //

                        //remove the expired event from treemap
                        Object expiredEventParameterValue = getParameterValue(funtionParameter, firstEvent);
                        maxByMinByExecutor.getTreeMap().remove(expiredEventParameterValue);
                        events.remove(0);
                        //
                        complexEventChunk.insertBeforeCurrent(firstEvent);
                        events.add(clonedStreamEventTreeMap);
                        //this.expiredEventChunk.add(clonedEvent);
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
