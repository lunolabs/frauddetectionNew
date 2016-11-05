package ru.spbstu.frauddetection.core.fraud_detector.KafkaTransfers;

import com.google.common.io.Resources;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.utils.Utils;
import ru.spbstu.frauddetection.core.fraud_detector.Detector;
import ru.spbstu.frauddetection.core.fraud_detector.TransferContainers.VerdictContainer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class VerdictProducer implements Runnable {
    private static final Integer QUEUE_CAPACITY = 1000;
    private static final String RESPONSE_TOPIC = "spark_verdict_response";

    private boolean terminated = false;
    private BlockingQueue<VerdictContainer> queue;

    private final KafkaProducer<String, VerdictContainer> producer;

    public VerdictProducer(Detector detector) throws IOException {
        queue = new LinkedBlockingDeque<>(QUEUE_CAPACITY);

        Properties producerProps = new Properties();

        try (InputStream props = Resources.getResource("verdict_producer.properties").openStream()) {
            producerProps.load(props);
        }

        this.producer = new KafkaProducer<>(producerProps);

        DetectResponseListener listener = new DetectResponseListener() {
            @Override
            public void onResponse(VerdictContainer data) {
                queue.offer(data);
            }
        };

        detector.addResponseListener(listener);
    }

    @Override
    public void run() {
        while (!terminated) {
            VerdictContainer verdict = queue.poll();

            if (verdict == null) {
//                TODO: optimize check
                Utils.sleep(50);
            } else {
                producer.send(new ProducerRecord<String, VerdictContainer>(RESPONSE_TOPIC, verdict));
            }
        }

        producer.close();
    }

    public void shutdown() {
        System.out.println("Shutting down producer");
        terminated  = true;
    }
}
