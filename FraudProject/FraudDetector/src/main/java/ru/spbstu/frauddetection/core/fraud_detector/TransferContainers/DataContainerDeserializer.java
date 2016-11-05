package ru.spbstu.frauddetection.core.fraud_detector.TransferContainers;

import org.apache.commons.lang.SerializationUtils;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class DataContainerDeserializer implements Deserializer<DetectableDataContainer> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public DetectableDataContainer deserialize(String s, byte[] bytes) {
        return (DetectableDataContainer) SerializationUtils.deserialize(bytes);
    }

    @Override
    public void close() {

    }
}
