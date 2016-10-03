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

public class TopKLengthBatchStreamProcessorExtensionTestCase {
    static final Logger log = Logger.getLogger(TopKLengthBatchStreamProcessorExtensionTestCase.class);
    private volatile int count;
    private volatile boolean eventArrived;

    @Before
    public void init() {
        count = 0;
        eventArrived = false;
    }

    @Test
    public void testTopKLengthBatchStreamProcessorExtension() throws InterruptedException {
        log.info("TopKLengthBatchStreamProcessor TestCase");
        SiddhiManager siddhiManager = new SiddhiManager();

        String inStreamDefinition = "define stream inputStream (item int, price double);";
        String query = ("@info(name = 'query1') from inputStream#custom:topKLengthBatch(item, 6, 3)  " +
                "insert into outputStream;");
        ExecutionPlanRuntime executionPlanRuntime = siddhiManager.createExecutionPlanRuntime(inStreamDefinition + query);

        executionPlanRuntime.addCallback("query1", new QueryCallback() {
            @Override
            public void receive(long timeStamp, Event[] inEvents, Event[] removeEvents) {
                EventPrinter.print(timeStamp, inEvents, removeEvents);
                eventArrived = true;
                for (Event event : inEvents) {
                    if (count == 0) {
                        // Checking the if the topK elements are considered
                        Assert.assertEquals("item1", event.getData(2));
                        Assert.assertEquals(3L, event.getData(3));
                        Assert.assertEquals("item2", event.getData(4));
                        Assert.assertEquals(2L, event.getData(5));
                        Assert.assertEquals("item3", event.getData(6));
                        Assert.assertEquals(1L, event.getData(7));
                    } else if (count == 1) {
                        // Checking if the window had been reset
                        Assert.assertEquals("item4", event.getData(2));
                        Assert.assertEquals(2L, event.getData(3));
                        Assert.assertEquals("item5", event.getData(4));
                        Assert.assertEquals(2L, event.getData(5));
                        Assert.assertEquals("item6", event.getData(6));
                        Assert.assertEquals(2L, event.getData(7));
                    }
                    count++;
                }
            }
        });

        InputHandler inputHandler = executionPlanRuntime.getInputHandler("inputStream");
        executionPlanRuntime.start();

        inputHandler.send(new Object[]{"item1", 10});
        inputHandler.send(new Object[]{"item1", 13});
        inputHandler.send(new Object[]{"item2", 65});
        inputHandler.send(new Object[]{"item1", 74});
        inputHandler.send(new Object[]{"item2", 25});
        inputHandler.send(new Object[]{"item3", 64});
        // Length Window reset
        inputHandler.send(new Object[]{"item4", 65});
        inputHandler.send(new Object[]{"item5", 45});
        inputHandler.send(new Object[]{"item6", 34});
        inputHandler.send(new Object[]{"item4", 76});
        inputHandler.send(new Object[]{"item5", 93});
        inputHandler.send(new Object[]{"item6", 23});

        Thread.sleep(1000);
        Assert.assertEquals(2, count);
        Assert.assertTrue(eventArrived);
        executionPlanRuntime.shutdown();
    }
}
