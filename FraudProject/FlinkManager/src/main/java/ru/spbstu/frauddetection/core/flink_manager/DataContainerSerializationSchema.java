package ru.spbstu.frauddetection.core.flink_manager;

import org.apache.commons.lang.SerializationUtils;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import ru.spbstu.frauddetection.core.fraud_detector.TransferContainers.DetectableDataContainer;

public class DataContainerSerializationSchema implements
        SerializationSchema<DetectableDataContainer> {
        @Override
        public byte[] serialize(DetectableDataContainer detectableDataContainer) {
                return SerializationUtils.serialize(detectableDataContainer);
        }
}
