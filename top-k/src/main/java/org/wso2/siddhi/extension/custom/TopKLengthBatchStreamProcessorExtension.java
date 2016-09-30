package org.wso2.siddhi.extension.custom;

public class TopKLengthBatchStreamProcessorExtension extends AbstractKLengthBatchStreamProcessorExtension {
    public TopKLengthBatchStreamProcessorExtension() {
        isTopK = true;
    }
}
