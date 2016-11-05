package ru.spbstu.frauddetection.core.fraud_detector.KafkaTransfers;

import com.google.common.io.Resources;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import ru.spbstu.frauddetection.core.fraud_detector.TransferContainers.DetectableDataContainer;

import java.io.IOException;
import java.io.InputStream;
import java.lang.Runnable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class DetectableDataConsumer implements Runnable {
    private static final String TESTABLE_DATA_REQUEST = "spark_test_data_request";

    private static final Integer TIMEOUT = 10000;

    private final KafkaConsumer<String, DetectableDataContainer> consumer;
    private final String[] topics = {TESTABLE_DATA_REQUEST};

    private List<DetectRequestListener> listeners;

    public DetectableDataConsumer() throws IOException {
        Properties consumerProps = new Properties();

        try (InputStream props = Resources.getResource("consumer.properties").openStream()) {
            consumerProps.load(props);
        }

        this.consumer = new KafkaConsumer<>(consumerProps);

        listeners = new ArrayList<>();
    }

    public void addDetectRequestListener(DetectRequestListener listener) {
        listeners.add(listener);
    }

    public void run() {
        consumer.subscribe(Arrays.asList(topics));

        try {
            while (true) {
                ConsumerRecords<String, DetectableDataContainer> records = consumer.poll(TIMEOUT);
                for (ConsumerRecord<String, DetectableDataContainer> record : records) {
                    for (DetectRequestListener listener : listeners) {
                        listener.onDetectRequest(record.value());
                    }
                }
            }
        } catch (WakeupException e) {

        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        System.out.println("Shutting down consumer");
        consumer.wakeup();
    }
}
