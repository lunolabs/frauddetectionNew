package ru.spbstu.frauddetection.core.fraud_detector.TransferContainers;

import backtype.storm.utils.Utils;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class VerdictSerializer implements Serializer<VerdictContainer> {
    @Override
    public void configure(Map<String, ?> map, boolean b) {

    }

    @Override
    public byte[] serialize(String s, VerdictContainer verdictContainer) {
        return Utils.serialize(verdictContainer);
    }

    @Override
    public void close() {

    }
}
