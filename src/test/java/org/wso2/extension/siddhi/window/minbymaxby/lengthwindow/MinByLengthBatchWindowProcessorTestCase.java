
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
 */
package org.wso2.extension.siddhi.window.minbymaxby.lengthwindow;

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
 * Created by mathuriga on 29/09/16.
 */
public class MinByLengthBatchWindowProcessorTestCase {

    private static final Logger log = Logger.getLogger(MinByLengthBatchWindowProcessorTestCase.class);
    private int count;

    @Before
    public void init() {
        count = 0;
    }

    @Test
    public void testMinByWindowForLengthBatch1() throws InterruptedException {
        log.info("Testing minByLengthBatchWindowProcessor with no of events greater than window size for float type parameter");

        SiddhiManager siddhiManager = new SiddhiManager();

        //siddhiManager.setExtension("unique:minByLengthBatch", MinByLengthBatchWindowProcessor.class);
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.minbymaxby:minByLengthBatch(price, 4) select symbol,price," +
                "volume insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        try {
            executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    System.out.print("output event: ");
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
        log.info("Testing minByLengthBatchWindowProcessor with no of events greater than window size for integer type parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.minbymaxby:minByLengthBatch(volume, 4) select symbol,price," +
                "volume insert into outputStream ;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            results.add(new Object[]{"IBM", 60.5f, 2});
            results.add(new Object[]{"et", 700f, 1});
            executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    System.out.print("output event: ");
                    EventPrinter.print(events);
                    Object[] results1 = new Object[]{"IBM", 60.5f, 2};
                    for (Event event : events) {
                        assertArrayEquals((Object[]) results.get(count), event.getData());
                        count++;
                    }

                }
            });
            InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            executionPlanRuntime.start();
            inputHandler.send(new Object[]{"IBM", 700f, 14});
            inputHandler.send(new Object[]{"IBM", 60.5f, 2});
            inputHandler.send(new Object[]{"IBM", 700f, 142});
            inputHandler.send(new Object[]{"IBM", 60.5f, 21});
            inputHandler.send(new Object[]{"et", 700f, 12});
            inputHandler.send(new Object[]{"dg", 60.5f, 24});
            inputHandler.send(new Object[]{"IBM", 60.5f, 21});
            inputHandler.send(new Object[]{"et", 700f, 1});
            inputHandler.send(new Object[]{"dg", 60.5f, 24});
            Thread.sleep(1000);

        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void testMinByWindowForLengthBatch3() throws InterruptedException {
        log.info("Testing minByLengthBatchWindowProcessor with no of events greater than window size for integer type parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.minbymaxby:minByLengthBatch(symbol, 4) select symbol,price," +
                "volume insert into outputStream ;";
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);
        try {
            final List<Object> results = new ArrayList<Object>();
            results.add(new Object[]{"AAA", 700f, 142});
            results.add(new Object[]{"DGF", 60.5f, 21});
            executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    System.out.print("output event: ");
                    EventPrinter.print(events);
                    Object[] results1 = new Object[]{"IBM", 60.5f, 2};
                    for (Event event : events) {
                        assertArrayEquals((Object[]) results.get(count), event.getData());
                        count++;
                    }
                }
            });
            InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            executionPlanRuntime.start();
            inputHandler.send(new Object[]{"XXX", 700f, 14});
            inputHandler.send(new Object[]{"ABC", 60.5f, 2});
            inputHandler.send(new Object[]{"AAA", 700f, 142});
            inputHandler.send(new Object[]{"ACD", 60.5f, 21});
            inputHandler.send(new Object[]{"RTE", 700f, 1});
            inputHandler.send(new Object[]{"YTX", 60.5f, 24});
            inputHandler.send(new Object[]{"DGF", 60.5f, 21});
            inputHandler.send(new Object[]{"ETR", 700f, 1});
            inputHandler.send(new Object[]{"DXD", 60.5f, 24});
            Thread.sleep(1000);
        } finally {
            executionPlanRuntime.shutdown();
        }
    }

    @Test
    public void testMinByWindowForLengthBatch4() throws InterruptedException {
        log.info("Testing minByLengthBatchWindowProcessor with no of events greater than window size for float type parameter");

        SiddhiManager siddhiManager = new SiddhiManager();
        String cseEventStream = "define stream cseEventStream (symbol string, price float, volume int);";
        String query = "@info(name = 'query1') from cseEventStream#window.minbymaxby:minByLengthBatch(price, 4) select symbol,price," +
                "volume insert into outputStream ;";

        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(cseEventStream + query);

        try {
            executionPlanRuntime.addCallback("outputStream", new StreamCallback() {

                @Override
                public void receive(Event[] events) {
                    System.out.print("output event: ");
                    EventPrinter.print(events);
                    Object[] results = null;
                    assertArrayEquals(results, events[0].getData());

                }
            });
            InputHandler inputHandler = executionPlanRuntime.getInputHandler("cseEventStream");
            executionPlanRuntime.start();
            inputHandler.send(new Object[]{"IBM", 700f, 14});
            inputHandler.send(new Object[]{"IBM", 50.5f, 2});


            Thread.sleep(1000);


        } finally {
            executionPlanRuntime.shutdown();
        }
    }


}
