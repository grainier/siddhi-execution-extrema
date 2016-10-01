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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by mathuriga on 29/09/16.
 */
public class MaxByMinByLengthBatchWindowProcessor extends WindowProcessor implements FindableProcessor {
    private ExpressionExecutor funtionParameter;
    private int length;
    private int count = 0;
    private ComplexEventChunk<StreamEvent> currentEventChunk = new ComplexEventChunk<StreamEvent>(false);
    private ComplexEventChunk<StreamEvent> expiredEventChunk = null;
    private ExecutionPlanContext executionPlanContext;
    private StreamEvent resetEvent = null;
    private VariableExpressionExecutor[] variableExpressionExecutors;
    MaxByMinByExecutor maxByMinByExecutor = new MaxByMinByExecutor();


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
//        this.executionPlanContext = executionPlanContext;
//        variableExpressionExecutors = new VariableExpressionExecutor[attributeExpressionExecutors.length - 1];
//        if (attributeExpressionExecutors.length == 2) {
//            variableExpressionExecutors[0] = (VariableExpressionExecutor) attributeExpressionExecutors[0];
//            funtionParameter = variableExpressionExecutors[0];
//            maxByMinByExecutor.setFunctionParameter(funtionParameter);
//            length = (Integer) (((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue());
//        }

    }

    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext, String functionType) {
        this.executionPlanContext = executionPlanContext;
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
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        List<ComplexEventChunk<StreamEvent>> streamEventChunks = new ArrayList<ComplexEventChunk<StreamEvent>>();
        synchronized (this) {
            ComplexEventChunk<StreamEvent> outputStreamEventChunk = new ComplexEventChunk<StreamEvent>(true);

            //clear the outputStream for every lengthBatch
            if (count == 0) {
                outputStreamEventChunk.clear();
            }

            long currentTime = executionPlanContext.getTimestampGenerator().currentTime();
            while (streamEventChunk.hasNext()) {
                StreamEvent streamEvent = streamEventChunk.next();
               // System.out.println(streamEvent);

                //get the parameter value for every events
                Object parameterValue = getParameterValue(funtionParameter, streamEvent);

                StreamEvent clonedStreamEvent = streamEventCloner.copyStreamEvent(streamEvent);
                StreamEvent clonedStreamEventTreeMap = streamEventCloner.copyStreamEvent(streamEvent);
                currentEventChunk.add(clonedStreamEvent);

                //put the value to treemap
                maxByMinByExecutor.insert(clonedStreamEventTreeMap, parameterValue);


                count++;
                if (count == length) {
                    if (outputExpectsExpiredEvents) {
                        if (expiredEventChunk.getFirst() != null) {
                            while (expiredEventChunk.hasNext()) {
                                StreamEvent expiredEvent = expiredEventChunk.next();
                                expiredEvent.setTimestamp(currentTime);
                            }

                        }
                    }
                    if (expiredEventChunk != null) {
                        expiredEventChunk.clear();
                    }

                    if (currentEventChunk.getFirst() != null) {
                        resetEvent = null;

                        if (expiredEventChunk != null) {
                            currentEventChunk.reset();
                            while (currentEventChunk.hasNext()) {
                                StreamEvent currentEvent = currentEventChunk.next();
                                StreamEvent toExpireEvent = streamEventCloner.copyStreamEvent(currentEvent);
                                toExpireEvent.setType(StreamEvent.Type.EXPIRED);
                                expiredEventChunk.add(toExpireEvent);
                            }
                        }

                        resetEvent = streamEventCloner.copyStreamEvent(currentEventChunk.getFirst());
                        resetEvent.setType(ComplexEvent.Type.RESET);

                    }
                    currentEventChunk.clear();
                    count = 0;
                    //get the results

                    outputStreamEventChunk.add(maxByMinByExecutor.getResult(maxByMinByExecutor.getFunctionType()));
                    maxByMinByExecutor.getTreeMap().clear();
                    if (outputStreamEventChunk.getFirst() != null) {
                        streamEventChunks.add(outputStreamEventChunk);
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

        if (expiredEventChunk != null) {
            return new Object[]{currentEventChunk.getFirst(), expiredEventChunk.getFirst(), count, resetEvent};
        } else {
            return new Object[]{currentEventChunk.getFirst(), count, resetEvent};
        }
    }

    @Override
    public void restoreState(Object[] state) {
        if (state.length > 3) {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) state[0]);
            expiredEventChunk.clear();
            expiredEventChunk.add((StreamEvent) state[1]);
            count = (Integer) state[2];
            resetEvent = (StreamEvent) state[3];

        } else {
            currentEventChunk.clear();
            currentEventChunk.add((StreamEvent) state[0]);
            count = (Integer) state[1];
            resetEvent = (StreamEvent) state[2];
        }
    }

    public Object getParameterValue(ExpressionExecutor funtionParameter, StreamEvent streamEvent) {
        Object parameterValue;

            parameterValue = funtionParameter.execute(streamEvent);

        return parameterValue;
    }

}
