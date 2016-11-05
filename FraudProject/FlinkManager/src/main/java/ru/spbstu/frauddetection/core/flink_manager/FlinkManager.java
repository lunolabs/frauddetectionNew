package ru.spbstu.frauddetection.core.flink_manager;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer08;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.log4j.Logger;
import ru.spbstu.frauddetection.core.flink_manager.DataflowOperators.DBReader;
import ru.spbstu.frauddetection.core.flink_manager.DataflowOperators.DetectionConnector;
import ru.spbstu.frauddetection.core.flink_manager.DataflowOperators.XMLParser;
import ru.spbstu.frauddetection.core.fraud_detector.TransferContainers.DetectableDataContainer;

import java.io.InputStream;
import java.util.*;

public class FlinkManager {
    private final static Logger logger = Logger.getLogger(FlinkManager.class);

    private static final String CONFIGURATION_FILENAME = "/medicine_config.xml";
    private static final String KAFKA_PORT = "localhost:9092";
    private static final String ZK_PORT = "localhost:2181";

//    TODO: extracts topics to general system configuration
    private static final String INPUT_XML_CONSUMER_TOPIC = "input_xml_consumer";
    private static final String TEST_DATA_REQUEST_TOPIC = "spark_test_data_request";

    public void run() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        setFraudDetectionConfiguration(env);

        MockData mockDB = new MockData();

        DataStream<String> sourceStream = setupSource(env);

        DataStream<DataflowCustomTypes.ParsedXMLTuple> parsedXMLDataStream = sourceStream
                .map(new XMLParser());

        DataStream<DataflowCustomTypes.ParsedXMLDBTuple> dataBaseReaderStream = parsedXMLDataStream
                .map(new DBReader(mockDB));

        DataStream<DetectableDataContainer> detectionConnector = dataBaseReaderStream
                .map(new DetectionConnector());
        detectionConnector.addSink(new FlinkKafkaProducer08<>(
                KAFKA_PORT, TEST_DATA_REQUEST_TOPIC, new DataContainerSerializationSchema()));

//        Kafka receiver operator here

//        SplitStream<DataflowCustomTypes.VerdictTuple> splitStream = splitStreamsByVerdicts(fraudDetectorStream);
//
//        DataStream<DataflowCustomTypes.VerdictTuple> dbWriterStream = splitStream
//                .select(DataflowConfigurationConstants.VERDICT_PASSED_CHECK);
//
//        dbWriterStream.map(new DBWriter());
//        dbWriterStream.print();
//
//        DataStream<DataflowCustomTypes.VerdictTuple> verdictSinkStream = splitStream
//                .select(DataflowConfigurationConstants.VERDICT_FAILED_CHECK);
//
//        verdictSinkStream.print();

//        Debug tool
//        prints operations graph with JSON -> http://flink.apache.org/visualizer/
//        System.out.println(env.getExecutionPlan());

        env.execute();
    }

    private DataStream<String> setupSource(StreamExecutionEnvironment env) {
        FlinkKafkaConsumer08<String> kafkaConsumer = setupKafkaConsumer();

        // adding kafka consumer source
        DataStream<String> sourceStream = env
                .addSource(kafkaConsumer);

        return sourceStream;
    }

    private void setFraudDetectionConfiguration(StreamExecutionEnvironment env) {
        InputStream is = getClass().getResourceAsStream(CONFIGURATION_FILENAME);
        String xmlConfiguration = new Scanner(is).useDelimiter("\\Z").next();

        Map<String, String> config = new HashMap<>();
        config.put(DataflowConfigurationConstants.XML_CONFIGURATION_KEY, xmlConfiguration);
        ParameterTool parameters = ParameterTool.fromMap(config);

        env.getConfig().setGlobalJobParameters(parameters);
    }

    private FlinkKafkaConsumer08<String> setupKafkaConsumer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KAFKA_PORT);
        properties.setProperty("zookeeper.connect", ZK_PORT);
        properties.setProperty("group.id", INPUT_XML_CONSUMER_TOPIC);

//        TODO: extract kafka topic to system properties
        return new FlinkKafkaConsumer08<String>("xml_data", new SimpleStringSchema(), properties);
    }

    private SplitStream<DataflowCustomTypes.VerdictTuple>
    splitStreamsByVerdicts(DataStream<DataflowCustomTypes.VerdictTuple> inputStream) {
        SplitStream<DataflowCustomTypes.VerdictTuple> splitStream = inputStream
                .split(input -> {
                    List<String> output = new ArrayList<String>();
                    if (input.f0) {
                        output.add(DataflowConfigurationConstants.VERDICT_PASSED_CHECK);
                    } else {
                        output.add(DataflowConfigurationConstants.VERDICT_FAILED_CHECK);
                    }

                    return output;
                });

        return splitStream;
    }
}
