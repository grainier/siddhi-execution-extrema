package org.wso2.siddhi.extension.custom;

public class BottomKLengthBatchStreamProcessorExtension extends AbstractKLengthBatchStreamProcessorExtension {
    public BottomKLengthBatchStreamProcessorExtension() {
        isTopK = false;
    }
}
