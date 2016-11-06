package ru.spbstu.frauddetection.sparkmanager;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SparkManager {

    private static final String TOPIC_NAME = "xml_data";
    private static final String ZK_HOST = "localhost:2181";

    JavaSparkContext sc;
    JavaStreamingContext ssc;

    SparkManager() {
        SparkConf conf = new SparkConf()
                .setAppName("FraudProject").setMaster("local[*]");
        sc = new JavaSparkContext(conf);
        ssc = new JavaStreamingContext(sc, Durations.seconds(10));
    }

    public void run() {
        //JavaDStream<String> xmlData = ssc.textFileStream("file:///home/lvs/other/tmp/sparkdir");
        //JavaDStream<String> xmlData = ssc.textFileStream("hdfs://localhost:54310/test");
        //JavaDStream<String> xmlData = ssc.textFileStream("hdfs://localhost:54310/test");

        Map<String, Integer> topicMap = new HashMap<>();
        //topic and number threads
        topicMap.put(TOPIC_NAME, 1);
        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(ssc, ZK_HOST, UUID.randomUUID().toString(), topicMap);
        //get massage
        JavaDStream<String> lines = kafkaStream.map((Tuple2<String, String> tuple2) -> tuple2._2());
        lines.foreachRDD(rdd ->
                rdd.foreach(str -> System.out.print("\n\n\n\n\n\n" + str + "\n\n\n\n\n\n\n")));

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
/*
import org.apache.kafka.common.utils.Utils;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import ru.spbstu.frauddetection.FraudConfig.ConfigurationParser.ConfigurationParser;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;

import java.io.InputStream;
import java.util.Scanner;
import java.util.UUID;

public class StormManager {
    private final static Logger logger = Logger.getLogger(StormManager.class);

    public enum StormRunModeEnum {LOCAL, PRODUCTION};

    private static final String SPOUT_NAME = "xml-data-spout";
    private static final String PARSE_XML_BOLT_NAME = "parse-xml-data-bolt";
    private static final String DATA_BASE_READER_NAME = "data-base-reader-bolt";
    private static final String FRAUD_DETECT_BOLT_NAME = "fraud-detect-bolt";
    private static final String DATA_BASE_WRITER_BOLT_NAME = "data-base-writer-bolt";
    private static final String USER_NOTIFICATION_BOLT_NAME = "user-notification-bolt";

    private static final String TOPIC_NAME = "xml_data";
    private static final String ZK_HOST = "localhost:2181";

    private static final String CONFIGURATION_FILENAME = "/medicine_config.xml";

    public void run(StormRunModeEnum runMode) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        InputStream is = getClass().getResourceAsStream(CONFIGURATION_FILENAME);
        String xmlConfiguration = new Scanner(is).useDelimiter("\\Z").next();

        MockData db = new MockData();

//        FraudDetectionBolt.initSparkContext();

        KafkaSpout kafkaSpout = createKafkaSpout();
        builder.setSpout(SPOUT_NAME, kafkaSpout, 1);
        builder.setBolt(PARSE_XML_BOLT_NAME, new ParseXMLDataBolt(), 1).shuffleGrouping(SPOUT_NAME);
        builder.setBolt(DATA_BASE_READER_NAME, new DataReaderBolt(db), 1).shuffleGrouping(PARSE_XML_BOLT_NAME);
        builder.setBolt(FRAUD_DETECT_BOLT_NAME, new FraudDetectionBolt(), 1).shuffleGrouping(DATA_BASE_READER_NAME);
        builder.setBolt(DATA_BASE_WRITER_BOLT_NAME, new DataWriterBolt(db), 1).shuffleGrouping(FRAUD_DETECT_BOLT_NAME);
        builder.setBolt(USER_NOTIFICATION_BOLT_NAME, new UserNotificationBolt(), 1).shuffleGrouping(FRAUD_DETECT_BOLT_NAME);

        Config config = new Config();
        config.setDebug(false);

        config.put(TupleConstants.XML_CONFIGURATION_KEY, xmlConfiguration);

        switch (runMode) {
            case LOCAL:
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("fraud_detection_topology", config,
                        builder.createTopology());

                while (true) {}
//                Utils.sleep(1000);
//                break;

            case PRODUCTION:
                config.setNumWorkers(3);
                StormSubmitter.submitTopology("fraud_detection_topology", config, builder.createTopology());
        }
    }

    private KafkaSpout createKafkaSpout() {
        BrokerHosts hosts = new ZkHosts(ZK_HOST);
        SpoutConfig spoutConfig = new SpoutConfig(hosts, TOPIC_NAME, "/" + TOPIC_NAME, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        return new KafkaSpout(spoutConfig);
    }
}
*/
