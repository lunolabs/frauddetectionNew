package ru.spbstu.frauddetection.core.fraud_detector.KafkaTransfers;

import ru.spbstu.frauddetection.core.fraud_detector.TransferContainers.VerdictContainer;

public interface DetectResponseListener {
    void onResponse(VerdictContainer container);
}
