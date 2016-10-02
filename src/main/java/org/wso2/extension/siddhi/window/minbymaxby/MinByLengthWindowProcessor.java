package org.wso2.extension.siddhi.window.minbymaxby;

import org.wso2.siddhi.core.config.ExecutionPlanContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;

/**
 * Created by mathuriga on 30/09/16.
 */
public class MinByLengthWindowProcessor extends MaxByMinByLengthWindowProcessor {
    String functionType;


    public MinByLengthWindowProcessor() {
        this.functionType = "MIN";
    }

    protected void init(ExpressionExecutor[] expressionExecutors, ExecutionPlanContext executionPlanContext) {
        super.init(expressionExecutors, executionPlanContext, functionType);
    }

    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor, StreamEventCloner streamEventCloner) {
        super.process(streamEventChunk, nextProcessor, streamEventCloner);
    }
}
