package ru.spbstu.frauddetection.core.fraud_detector;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.xml.sax.SAXException;
import ru.spbstu.frauddetection.FraudConfig.ConfigurationParser.ConfigurationParser;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;
import ru.spbstu.frauddetection.core.fraud_detector.KafkaTransfers.DetectableDataConsumer;
import ru.spbstu.frauddetection.core.fraud_detector.KafkaTransfers.VerdictProducer;

import javax.xml.bind.JAXBException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class DetectionDataflowManager {
    private static final Integer CONSUMER_NUM_THREADS = 1;
    private static final Integer DETECTOR_NUM_THREADS = 1;
    private static final Integer PRODUCER_NUM_THREADS = 1;

    private static final String CONFIGURATION_FILENAME = "/medicine_config.xml";

    private final ExecutorService executor;

    public DetectionDataflowManager() {
        executor = Executors.newFixedThreadPool(CONSUMER_NUM_THREADS + DETECTOR_NUM_THREADS +
                                                PRODUCER_NUM_THREADS);
    }

    public void start() throws IOException, JAXBException, SAXException {
        DetectableDataConsumer consumer = submitDataConsumer();

        JavaSparkContext sparkContext = setupSparkCluster();
        Configuration xmlConfiguration = constructDetectionConfiguration();
        Detector detector = submitDetector(consumer, sparkContext, xmlConfiguration);

        VerdictProducer producer = submitVerdictProducer(detector);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                consumer.shutdown();
                producer.shutdown();

                detector.shutdown();

                executor.shutdown();

                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });
    }

    private JavaSparkContext setupSparkCluster() {
        SparkConf conf = new SparkConf()
                .setAppName("Detection").setMaster("local[*]");

        return new JavaSparkContext(conf);
    }

    private Configuration constructDetectionConfiguration() throws JAXBException, SAXException {
        InputStream is = getClass().getResourceAsStream(CONFIGURATION_FILENAME);
        String xmlConfigString = new Scanner(is).useDelimiter("\\Z").next();

        return new ConfigurationParser().parseConfiguration(xmlConfigString);
    }

    private DetectableDataConsumer submitDataConsumer() throws IOException {
        DetectableDataConsumer consumer = new DetectableDataConsumer();
        executor.submit(consumer);
        return consumer;
    }

    private VerdictProducer submitVerdictProducer(Detector detector) throws IOException {
        VerdictProducer producer = new VerdictProducer(detector);
        executor.submit(producer);
        return producer;
    }

    private Detector submitDetector(DetectableDataConsumer consumer, JavaSparkContext sparkContext,
                                    Configuration detectionConfig) {
        Detector detector = new Detector(consumer, sparkContext, detectionConfig);
        executor.submit(detector);
        return detector;
    }
}
