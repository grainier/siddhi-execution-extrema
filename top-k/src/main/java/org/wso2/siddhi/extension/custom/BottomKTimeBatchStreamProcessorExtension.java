package org.wso2.siddhi.extension.custom;

public class BottomKTimeBatchStreamProcessorExtension extends AbstractKTimeBatchStreamProcessorExtension {
    public BottomKTimeBatchStreamProcessorExtension() {
        isTopK = false;
    }
}
