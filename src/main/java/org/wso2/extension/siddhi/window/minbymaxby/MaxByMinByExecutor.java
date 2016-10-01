package org.wso2.extension.siddhi.window.minbymaxby;

import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.executor.ExpressionExecutor;

import java.util.TreeMap;

/**
 * Created by mathuriga on 29/09/16.
 */
public class MaxByMinByExecutor {
    private String functionType;
    private ExpressionExecutor functionParameter;
    private TreeMap<Object, StreamEvent> treeMap = new TreeMap<Object, StreamEvent>();

    public TreeMap<Object, StreamEvent> getTreeMap() {
        return treeMap;
    }


   public void setFunctionParameter(ExpressionExecutor functionParameter) {
        this.functionParameter = functionParameter;
    }

    public String getFunctionType() {
        return functionType;
    }

    public void setFunctionType(String functionType) {
        this.functionType = functionType;
    }


    public void insert(StreamEvent clonedStreamEvent, Object parameterValue) {
        treeMap.put(parameterValue, clonedStreamEvent);
    }

    public StreamEvent getResult(String functionType) {
        StreamEvent outputEvent;

        if (functionType.equals("MIN")) {
            Object minEventKey = treeMap.firstKey();
            outputEvent = treeMap.get(minEventKey);
        } else {
            Object maxEventKey = treeMap.lastKey();
            outputEvent = treeMap.get(maxEventKey);
        }
        return outputEvent;
    }
}
