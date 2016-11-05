package ru.spbstu.frauddetection.core.storm_manager;

import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.xml.sax.SAXException;
import ru.spbstu.frauddetection.FraudConfig.ConfigurationParser.ConfigurationParser;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;

import javax.xml.bind.JAXBException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FraudDetectionBolt implements BoltInterface {
    private final static Logger logger = Logger.getLogger(FraudDetectionBolt.class);
    private static final JavaSparkContext sparkContext = new JavaSparkContext(new SparkConf()
            .setAppName("Detectio").setMaster("local[*]"));
    private OutputCollector collector;
    private Configuration config;

    private FraudDetectionManager detectors;

//    public static void initSparkContext() {
//        SparkConf sparkConf = new SparkConf()
//                .setAppName("Detectio").setMaster("local[*]");
//
//        if (sparkContext == null) {
//            FraudDetectionBolt.sparkContext = new JavaSparkContext(sparkConf);
//        }
//    }

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;

        String xmlConfig = (String) map.get(TupleConstants.XML_CONFIGURATION_KEY);
        try {
            config = new ConfigurationParser().parseConfiguration(xmlConfig);
        } catch (JAXBException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        }

        if (config != null) {
            detectors = new FraudDetectionManager(config, sparkContext);
        }
    }

    @Override
    public void execute(Tuple input) {
        String xmlString = input.getStringByField(TupleConstants.XML_STRING_KEY);
        Map<Method, ValueGroup> parsedXMLMap =
                (HashMap<Method, ValueGroup>) input.getValueByField(TupleConstants.PARSED_XML_KEY);
        List<ValueGroup> dbList =
                (List<ValueGroup>) input.getValueByField(TupleConstants.DATA_BASE_KEY);

        logger.info("received xml");
        logger.info(xmlString);
        logger.info("got parsed xml map");
        for (Method key : parsedXMLMap.keySet()) {
            logger.info(key + " "  + " " + parsedXMLMap.get(key));
        }

        logger.info("got data base");
        logger.info(dbList);

        boolean checkPassed = !(detectors.fraudDetected(parsedXMLMap, dbList));

        logger.info("Total check passed: " + checkPassed);

        if (checkPassed) {
            collector.emit(new Values(xmlString, null, checkPassed));
        } else {
            collector.emit(new Values(xmlString, detectors.getLastFraudMessage(), checkPassed));
        }

        collector.ack(input);
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
           declarer.declare(new Fields(TupleConstants.XML_STRING_KEY,
                   TupleConstants.FRAUD_MESSAGE_KEY, TupleConstants.VERDICT_SUCCESS_KEY));
    }
}
