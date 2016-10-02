package org.wso2.extension.siddhi.window.minbymaxby;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.stream.output.StreamCallback;
import org.wso2.siddhi.core.util.EventPrinter;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * Created by mathuriga on 30/09/16.
 */
public class MinByLengthWindowProcessorTestCase {

    private static final Logger log = Logger.getLogger(MinByLengthWindowProcessorTestCase.class);
    int count;
    List<Object> results = new ArrayList<Object>();

    @Before
    public void init() {
        count = 0;
    }


    @Test
    public void testMinByLengthWindowProcessor1() throws InterruptedException {
        log.info("Testing minByLengthWindowProcessor with no of events less than window size for float type parameter");
        SiddhiManager siddhiManager = new SiddhiManager();

        siddhiManager.setExtension("unique:minByLengthBatch", MinByLengthBatchWindowProcessor.class);
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.minbymaxby:minByLength(price, 4) select symbol,price," +
                "volume insert into outputStream ;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        results.add(new Object[]{"IBM", 700f, 14});
        results.add(new Object[]{"IBM", 20.5f, 2});
        results.add(new Object[]{"IBM", 20.5f, 2});
        try {
            executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    System.out.print("output event: ");
                    EventPrinter.print(events);

                    for (Event event : events) {
                        assertArrayEquals((Object[]) results.get(count), event.getData());
                        count++;
                    }
                }
            });
            InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            executionPlanRuntime.start();
            inputHandler.send(new Object[]{"IBM", 700f, 14});
            inputHandler.send(new Object[]{"IBM", 20.5f, 2});
            inputHandler.send(new Object[]{"WSO2", 700f, 1});
            Thread.sleep(1000);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }


    @Test
    public void testMinByLengthWindowProcessor2() throws InterruptedException {
        log.info("Testing minByLengthWindowProcessor with no of events equal to window size for integer type parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.minbymaxby:minByLength(volume, 4) select symbol,price," +
                "volume insert into outputStream ;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            results.add(new Object[]{"IBM", 700f, 14});
            results.add(new Object[]{"IBM", 60.5f, 12});
            results.add(new Object[]{"IBM", 700f, 2});
            results.add(new Object[]{"IBM", 700f, 2});
            executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    System.out.print("output event: ");
                    EventPrinter.print(events);
                    for (Event event : events) {
                        assertArrayEquals((Object[]) results.get(count), event.getData());
                        count++;
                    }
                }
            });
            InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            executionPlanRuntime.start();
            inputHandler.send(new Object[]{"IBM", 700f, 14});
            inputHandler.send(new Object[]{"IBM", 60.5f, 12});
            inputHandler.send(new Object[]{"IBM", 700f, 2});
            inputHandler.send(new Object[]{"xoo", 60.5f, 82});
            Thread.sleep(1000);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }


    @Test
    public void testMinByLengthWindowProcessor3() throws InterruptedException {
        log.info("Testing minByLengthWindowProcessor with no of events greater than window size for String type parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.minbymaxby:minByLength(symbol, 4) select symbol,price," +
                "volume insert into outputStream ;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            results.add(new Object[]{"bbc", 700f, 14});
            results.add(new Object[]{"bbb", 60.5f, 12});
            results.add(new Object[]{"bbb", 60.5f, 12});
            results.add(new Object[]{"bbb", 60.5f, 12});
            results.add(new Object[]{"abc", 700f, 84});
            results.add(new Object[]{"abc", 700f, 84});
            results.add(new Object[]{"aac", 700f, 2});
            results.add(new Object[]{"aac", 700f, 2});
            results.add(new Object[]{"aac", 700f, 2});
            results.add(new Object[]{"aaa", 60.5f, 82});

            executionPlanRuntime.addCallback("outputStream", new StreamCallback() {
                @Override
                public void receive(Event[] events) {
                    System.out.print("output event: ");
                    EventPrinter.print(events);

                    for (Event event : events) {
                        assertArrayEquals((Object[]) results.get(count), event.getData());
                        count++;
                    }
                }
            });
            InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            executionPlanRuntime.start();
            inputHandler.send(new Object[]{"bbc", 700f, 14});
            inputHandler.send(new Object[]{"bbb", 60.5f, 12});
            inputHandler.send(new Object[]{"xxx", 700f, 2});
            inputHandler.send(new Object[]{"ddd", 60.5f, 82});
            inputHandler.send(new Object[]{"abc", 700f, 84});
            inputHandler.send(new Object[]{"ghj", 60.5f, 12});
            inputHandler.send(new Object[]{"aac", 700f, 2});
            inputHandler.send(new Object[]{"dhh", 60.5f, 82});
            inputHandler.send(new Object[]{"drg", 700f, 2});
            inputHandler.send(new Object[]{"aaa", 60.5f, 82});
            Thread.sleep(1000);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

}
