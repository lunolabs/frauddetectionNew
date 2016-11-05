package ru.spbstu.frauddetection.core.fraud_detector.KafkaTransfers;

import ru.spbstu.frauddetection.core.fraud_detector.TransferContainers.DetectableDataContainer;

public interface DetectRequestListener {
    void onDetectRequest(DetectableDataContainer data);
}
