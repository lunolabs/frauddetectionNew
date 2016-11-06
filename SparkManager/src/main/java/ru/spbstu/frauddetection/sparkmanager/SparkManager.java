package ru.spbstu.frauddetection.sparkmanager;

import ru.spbstu.frauddetection.FraudConfig.ConfigurationParser.ConfigurationParser;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import ru.spbstu.frauddetection.InputDataCalculator.InputCalculator;
import ru.spbstu.frauddetection.InputDataCalculator.InputGroup;
import ru.spbstu.frauddetection.datastorage.AbstractData;
import ru.spbstu.frauddetection.datastorage.MockData;
import scala.Tuple2;

import java.io.FileInputStream;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class SparkManager implements Serializable {

    private static final String TOPIC_NAME = "xml_data";
    private static final String ZK_HOST = "localhost:2181";

    private static Configuration configuration;

    public static final Properties properties = new Properties();
    private static String PROPERTIESFILE = System.getProperty("user.dir") + "/etc/SparkManager.properties";
    private static String XML_CONGIG_PATH_KEY = "xml_config_path";
    static {
        try {
            properties.load(new FileInputStream(PROPERTIESFILE));
            String xmlConfig = new String(Files.readAllBytes(Paths.get((String)properties.get(XML_CONGIG_PATH_KEY))));
            configuration = new ConfigurationParser().parseConfiguration(xmlConfig);
        } catch (Exception e) {
            new Exception(e);
        }
    }

    private static JavaStreamingContext ssc;
    private static JavaSparkContext sc;
    static  {
        SparkConf conf = new SparkConf()
                .setAppName("FraudProject").setMaster("local[*]");
        sc = new JavaSparkContext(conf);
        ssc = new JavaStreamingContext(conf, Durations.seconds(10));
    }

    public void run() throws Exception {
        Map<String, Integer> topicMap = new HashMap<>();
        //topic and number threads
        topicMap.put(TOPIC_NAME, 1);
        JavaPairReceiverInputDStream<String, String> kafkaStream =
                KafkaUtils.createStream(ssc, ZK_HOST, UUID.randomUUID().toString(), topicMap);
        //get massage
        JavaDStream<String> xmls = kafkaStream.map((Tuple2<String, String> tuple2) -> tuple2._2());
        JavaDStream<List<InputGroup>> groupList = xmls.map(xml -> InputCalculator.calculate(configuration, xml));

        JavaDStream<Map<InputGroup, List<InputGroup>>> verifiableList= groupList.map(list -> {
            Map<InputGroup, List<InputGroup>> res = new HashMap<>();
            AbstractData database = new MockData();
            List<InputGroup> data = database.getValues(configuration.getUniqueFields());
            for(InputGroup inputGroup : list) {
                res.put(inputGroup, data);
            }
            return res;
        });

        //JavaDStream<InputGroup> groups = groupList.map(list -> sc.parallelize(list));
        groupList.foreachRDD(rdd ->
                rdd.foreach(obj -> System.out.print("\n\n\n\n\n\n" + obj.toString() + "\n\n\n\n\n\n\n")));

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
