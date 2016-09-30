package org.wso2.siddhi.extension.custom;

public class TopKTimeBatchStreamProcessorExtension extends AbstractKTimeBatchStreamProcessorExtension {
    public TopKTimeBatchStreamProcessorExtension() {
        isTopK = true;
    }
}
