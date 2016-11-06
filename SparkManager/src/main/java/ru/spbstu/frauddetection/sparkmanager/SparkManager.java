package ru.spbstu.frauddetection.sparkmanager;

import org.apache.avro.generic.GenericData;
import org.apache.spark.api.java.function.FlatMapFunction;
import ru.spbstu.frauddetection.FraudConfig.ConfigurationParser.ConfigurationParser;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;

import org.apache.spark.SparkConf;
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
            configuration = ConfigurationParser.parse(xmlConfig);
        } catch (Exception e) {
            new Exception(e);
        }
    }

    private static JavaStreamingContext ssc;
    static  {
        SparkConf conf = new SparkConf()
                .setAppName("FraudProject").setMaster("local[*]");
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
        JavaDStream<Tuple2<Integer, List<InputGroup>>> groupList = xmls.map(xml ->
                new Tuple2<>(xml.hashCode(), InputCalculator.calculate(configuration, xml)));

        AbstractData database = new MockData();
        List<InputGroup> data = database.getValues(configuration.getUniqueFields());

        JavaDStream<Tuple2<Integer, Tuple2<InputGroup, List<InputGroup>>>> setStream =
                groupList.flatMap(tupl -> {
                    List<Tuple2<Integer, Tuple2<InputGroup, List<InputGroup>>>> sets = new ArrayList<>();
                    for(InputGroup inputGroup : tupl._2) {
                        sets.add(new Tuple2<>(tupl._1, new Tuple2<>(inputGroup, data)));
                    }
                    return sets.listIterator();
                });

        setStream.foreachRDD(rdd ->
                rdd.foreach(obj -> System.out.print("\n\n\n\n\n\n" + obj.toString() + "\n\n\n\n\n\n\n")));

        JavaDStream<Tuple2<String, Boolean>> verdicts;


        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
