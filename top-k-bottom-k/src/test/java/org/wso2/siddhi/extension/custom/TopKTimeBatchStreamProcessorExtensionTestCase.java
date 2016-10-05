/*
 * Copyright (c) 2016, WSO2 Inc. (http://www.wso2.org)
 * All Rights Reserved.
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
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.siddhi.extension.custom;

import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;
import org.wso2.siddhi.core.ExecutionPlanRuntime;
import org.wso2.siddhi.core.SiddhiManager;
import org.wso2.siddhi.core.event.Event;
import org.wso2.siddhi.core.query.output.callback.QueryCallback;
import org.wso2.siddhi.core.stream.input.InputHandler;
import org.wso2.siddhi.core.util.EventPrinter;

public class TopKTimeBatchStreamProcessorExtensionTestCase {
    private static final Logger log = Logger.getLogger(TopKTimeBatchStreamProcessorExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testTopKTimeBatchStreamProcessorExtensionWithoutStartTime() throws InterruptedException {
        log.info("TopKTimeBatchStreamProcessor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item int, price double);";
        String query = ("@info(name = 'query1') from inputStream#custom:topKTimeBatch(item, 1 sec, 3)  " +
                "insert all events into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        addQueryCallback(executionPlanRuntime);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"item1", 10});
        inputHandler.send(new Object[]{"item1", 13});
        inputHandler.send(new Object[]{"item2", 65});
        inputHandler.send(new Object[]{"item1", 74});
        inputHandler.send(new Object[]{"item2", 25});
        inputHandler.send(new Object[]{"item3", 64});
        // Time Window reset
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"item4", 14});
        inputHandler.send(new Object[]{"item4", 73});
        // To get all the expired events
        Thread.sleep(1100);

        Thread.sleep(1000);
        Assert.assertEquals(3, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testTopKTimeBatchStreamProcessorExtensionWithStartTime() throws InterruptedException {
        log.info("TopKTimeBatchStreamProcessor TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item int, price double);";
        String query = ("@info(name = 'query1') from inputStream#custom:topKTimeBatch(item, 1 sec, 3, 1000)  " +
                "insert all events into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        addQueryCallback(executionPlanRuntime);

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"item3", 43});
        inputHandler.send(new Object[]{"item3", 61});
        inputHandler.send(new Object[]{"item3", 44});
        // Start time
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"item1", 10});
        inputHandler.send(new Object[]{"item1", 13});
        inputHandler.send(new Object[]{"item2", 65});
        inputHandler.send(new Object[]{"item1", 74});
        inputHandler.send(new Object[]{"item2", 25});
        inputHandler.send(new Object[]{"item3", 64});
        // Time Window reset
        Thread.sleep(1100);
        inputHandler.send(new Object[]{"item4", 75});
        inputHandler.send(new Object[]{"item4", 34});
        // To get all the expired events
        Thread.sleep(1100);

        Thread.sleep(1000);
        Assert.assertEquals(3, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    private void addQueryCallback(ExecutionPlanRuntime executionPlanRuntime) {
        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (count == 0) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals("item1", event.getData(2));
                        Assert.assertEquals(3L, event.getData(3));
                        Assert.assertEquals("item2", event.getData(4));
                        Assert.assertEquals(2L, event.getData(5));
                        Assert.assertEquals("item3", event.getData(6));
                        Assert.assertEquals(1L, event.getData(7));
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNull(removeEvents);
                } else if (count == 1) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals("item4", event.getData(2));
                        Assert.assertEquals(2L, event.getData(3));
                        Assert.assertNull(event.getData(4));
                        Assert.assertNull(event.getData(5));
                        Assert.assertNull(event.getData(6));
                        Assert.assertNull(event.getData(7));
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        Assert.assertEquals("item1", event.getData(2));
                        Assert.assertEquals(3L, event.getData(3));
                        Assert.assertEquals("item2", event.getData(4));
                        Assert.assertEquals(2L, event.getData(5));
                        Assert.assertEquals("item3", event.getData(6));
                        Assert.assertEquals(1L, event.getData(7));
                        Assert.assertTrue(event.isExpired());
                    }
                } else if (count == 2) {
                    Assert.assertNull(inEvents);
                    Assert.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        Assert.assertEquals("item4", event.getData(2));
                        Assert.assertEquals(2L, event.getData(3));
                        Assert.assertNull(event.getData(4));
                        Assert.assertNull(event.getData(5));
                        Assert.assertNull(event.getData(6));
                        Assert.assertNull(event.getData(7));
                        Assert.assertTrue(event.isExpired());
                    }
                } else {
                    Assert.fail();
                }
                count++;
            }
        });
    }
}
