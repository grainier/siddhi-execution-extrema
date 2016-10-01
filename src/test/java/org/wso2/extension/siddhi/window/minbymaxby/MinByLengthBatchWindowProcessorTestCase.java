package org.wso2.extension.siddhi.window.minbymaxby;

import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by mathuriga on 29/09/16.
 */
public class MinByLengthBatchWindowProcessorTestCase {

    private static final Logger log = Logger.getLogger(MinByLengthBatchWindowProcessorTestCase.class);
    private int inEventCount;
    private int removeEventCount;
    private int count;
    private boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        inEventCount = 0;
        removeEventCount = 0;
        eventArrived = false;
    }


    @Test
    public void testMinByWindowForLengthBatch() throws InterruptedException {
        log.info("Testing minBy length batch window with no of events greater than window size");

        SiddhiManager siddhiManager = new SiddhiManager();

        siddhiManager.setExtension("unique:minByLengthBatch", MinByLengthBatchWindowProcessor.class);
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.minbymaxby:minByLengthBatch(price, 4) select symbol,price," +
                "volume insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        try {
            executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    EventPrinter.print(events);
                    Object[] results = new Object[]{"IBM", 50.5f, 2};
                    assertArrayEquals(results, events[0].getData());

                }
            });
            InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            executionPlanRuntime.start();
            inputHandler.send(new Object[]{"IBM", 700f, 14});
            inputHandler.send(new Object[]{"IBM", 50.5f, 2});
            inputHandler.send(new Object[]{"et", 700f, 1});
            inputHandler.send(new Object[]{"dg", 60.5f, 24});

            Thread.sleep(1000);


        } finally {
            executionPlanRuntime.shutdown();
        }
    }


    @Test
    public void testMinByWindowForLengthBatch2() throws InterruptedException {
        log.info("Testing minBy length batch window with no of events less than window size");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.minbymaxby:minByLengthBatch(volume, 4) select symbol,price," +
                "volume insert into outputStream ;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        try {
            executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    EventPrinter.print(events);
                    Object[] results1 = new Object[]{"IBM", 60.5f, 2};
                    assertArrayEquals(results1, events[0].getData());

                }
            });
            InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            executionPlanRuntime.start();
            inputHandler.send(new Object[]{"IBM", 700f, 14});
            inputHandler.send(new Object[]{"IBM", 60.5f, 2});
            inputHandler.send(new Object[]{"IBM", 700f, 142});
            inputHandler.send(new Object[]{"IBM", 60.5f, 21});
            inputHandler.send(new Object[]{"et", 700f, 10});
            inputHandler.send(new Object[]{"dg", 60.5f, 24});



            Thread.sleep(1000);


        } finally {
            executionPlanRuntime.shutdown();
        }
    }
}
