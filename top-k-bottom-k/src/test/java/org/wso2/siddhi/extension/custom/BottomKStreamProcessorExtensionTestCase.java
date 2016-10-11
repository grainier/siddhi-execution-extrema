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

public class BottomKStreamProcessorExtensionTestCase {
    private static final Logger log = Logger.getLogger(BottomKStreamProcessorExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testBottomKStreamProcessorExtensionWithLengthBatchWindow() throws InterruptedException {
        log.info("BottomKStreamProcessor TestCase 1");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item string, price long);";
        String query = ("@info(name = 'query1') from inputStream#window.lengthBatch(6)#custom:bottomK(item, 3) " +
                "insert all events into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (count == 0) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals("item3", event.getData(2));
                        Assert.assertEquals(1L, event.getData(3));
                        Assert.assertEquals("item2", event.getData(4));
                        Assert.assertEquals(2L, event.getData(5));
                        Assert.assertEquals("item1", event.getData(6));
                        Assert.assertEquals(3L, event.getData(7));
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNull(removeEvents);
                } else if (count == 1) {
                    Assert.assertNull(inEvents);
                    Assert.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        Assert.assertEquals("item3", event.getData(2));
                        Assert.assertEquals(1L, event.getData(3));
                        Assert.assertEquals("item2", event.getData(4));
                        Assert.assertEquals(2L, event.getData(5));
                        Assert.assertEquals("item1", event.getData(6));
                        Assert.assertEquals(3L, event.getData(7));
                        Assert.assertTrue(event.isExpired());
                    }
                } else if (count == 2) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals("item4", event.getData(2));
                        Assert.assertEquals(2L, event.getData(3));
                        Assert.assertEquals("item5", event.getData(4));
                        Assert.assertEquals(2L, event.getData(5));
                        Assert.assertEquals("item6", event.getData(6));
                        Assert.assertEquals(2L, event.getData(7));
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNull(removeEvents);
                }
                count++;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"item1", 10L});
        inputHandler.send(new Object[]{"item1", 13L});
        inputHandler.send(new Object[]{"item2", 65L});
        inputHandler.send(new Object[]{"item1", 74L});
        inputHandler.send(new Object[]{"item2", 25L});
        inputHandler.send(new Object[]{"item3", 64L});
        // Length Window reset
        inputHandler.send(new Object[]{"item1", 10L});
        inputHandler.send(new Object[]{"item1", 13L});
        inputHandler.send(new Object[]{"item2", 65L});
        inputHandler.send(new Object[]{"item1", 74L});
        inputHandler.send(new Object[]{"item2", 25L});
        inputHandler.send(new Object[]{"item3", 64L});
        // Length Window reset
        inputHandler.send(new Object[]{"item4", 65L});
        inputHandler.send(new Object[]{"item5", 45L});
        inputHandler.send(new Object[]{"item6", 34L});
        inputHandler.send(new Object[]{"item4", 76L});
        inputHandler.send(new Object[]{"item5", 93L});
        inputHandler.send(new Object[]{"item6", 23L});

        Thread.sleep(1100);
        Assert.assertEquals(3, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }

    @Test
    public void testBottomKStreamProcessorExtensionWithTimeWindow() throws InterruptedException {
        log.info("BottomKStreamProcessor TestCase 2");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item string, price long);";
        String query = ("@info(name = 'query1') from inputStream#window.time(1 sec)#custom:bottomK(item, 3) " +
                "insert all events into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                if (count ==2) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals("item2", event.getData(2));
                        Assert.assertEquals(1L, event.getData(3));
                        Assert.assertEquals("item1", event.getData(4));
                        Assert.assertEquals(2L, event.getData(5));
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        Assert.assertEquals("item1", event.getData(2));
                        Assert.assertEquals(1L, event.getData(3));
                        Assert.assertEquals("item2", event.getData(4));
                        Assert.assertEquals(1L, event.getData(5));
                        Assert.assertTrue(event.isExpired());
                    }
                } else if (count == 8) {
                    Assert.assertNotNull(inEvents);
                    for (Event event : inEvents) {
                        Assert.assertEquals("item4", event.getData(2));
                        Assert.assertEquals(1L, event.getData(3));
                        Assert.assertEquals("item5", event.getData(4));
                        Assert.assertEquals(1L, event.getData(5));
                        Assert.assertEquals("item6", event.getData(6));
                        Assert.assertEquals(1L, event.getData(7));
                        Assert.assertFalse(event.isExpired());
                    }
                    Assert.assertNotNull(removeEvents);
                    for (Event event : removeEvents) {
                        Assert.assertEquals("item4", event.getData(2));
                        Assert.assertEquals(1L, event.getData(3));
                        Assert.assertEquals("item5", event.getData(4));
                        Assert.assertEquals(1L, event.getData(5));
                        Assert.assertTrue(event.isExpired());
                    }
                }
                count++;
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"item1", 10L});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"item2", 65L});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"item1", 10L});
        // Time Window reset
        Thread.sleep(1000);
        inputHandler.send(new Object[]{"item4", 65L});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"item5", 45L});
        Thread.sleep(100);
        inputHandler.send(new Object[]{"item6", 34L});

        Thread.sleep(1100);
        Assert.assertEquals(12, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}
