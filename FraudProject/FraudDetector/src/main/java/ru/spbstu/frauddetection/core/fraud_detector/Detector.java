package ru.spbstu.frauddetection.core.fraud_detector;

import org.apache.kafka.common.utils.Utils;
import org.apache.spark.api.java.JavaSparkContext;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;
import ru.spbstu.frauddetection.core.fraud_detector.KafkaTransfers.DetectRequestListener;
import ru.spbstu.frauddetection.core.fraud_detector.KafkaTransfers.DetectResponseListener;
import ru.spbstu.frauddetection.core.fraud_detector.KafkaTransfers.DetectableDataConsumer;
import ru.spbstu.frauddetection.core.fraud_detector.TransferContainers.DetectableDataContainer;
import ru.spbstu.frauddetection.core.fraud_detector.TransferContainers.VerdictContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

public class Detector implements Runnable {
    private static final Integer QUEUE_CAPACITY = 1000;

    private boolean terminated = false;
    private LinkedBlockingQueue<DetectableDataContainer> queue;
    private List<DetectResponseListener> responseListeners;

//    private FraudDetectionManager detectionManager;

    public Detector(DetectableDataConsumer consumer, JavaSparkContext sparkContext, Configuration xmlConfig) {
        queue = new LinkedBlockingQueue<>(QUEUE_CAPACITY);

//        detectionManager = new FraudDetectionManager(sparkContext, xmlConfig);

        DetectRequestListener requestListener = new DetectRequestListener() {
            @Override
            public void onDetectRequest(DetectableDataContainer data) {
                queue.offer(data);
            }
        };

        consumer.addDetectRequestListener(requestListener);

        responseListeners = new ArrayList<>();
    }

    public void addResponseListener(DetectResponseListener listener) {
        responseListeners.add(listener);
    }

    @Override
    public void run() {
        while (!terminated) {
            DetectableDataContainer detectableData = queue.poll();

            if (detectableData == null) {
//                TODO: optimize check
                Utils.sleep(50);
            } else {
//                boolean fraudDetected = detectionManager.fraudDetected(detectableData.getParsedData(), detectableData.getDB());
                boolean fraudDetected = false;

//                TODO: change to logs
                System.out.print("For \n" + detectableData.getInputXML() + "\nfraud ");
                if (fraudDetected) {
                    System.out.println("detected");
                } else {
                    System.out.println("not detected");
                }

                VerdictContainer verdict = new VerdictContainer(fraudDetected, detectableData.getInputXML());
                for (DetectResponseListener listener : responseListeners) {
                    listener.onResponse(verdict);
                }
            }
        }
    }

    public void shutdown() {
        System.out.println("Shutting down detector");
        terminated  = true;
    }
}
