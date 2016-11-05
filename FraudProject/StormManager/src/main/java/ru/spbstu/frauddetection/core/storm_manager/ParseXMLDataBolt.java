package ru.spbstu.frauddetection.core.storm_manager;

import org.apache.log4j.Logger;
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
import ru.spbstu.frauddetection.InputDataCalculator.InputCalculator;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;

import javax.xml.bind.JAXBException;
import javax.xml.xpath.XPathExpressionException;
import java.util.HashMap;
import java.util.Map;

public class ParseXMLDataBolt implements BoltInterface {
    private final static Logger logger = Logger.getLogger(ParseXMLDataBolt.class);

    private OutputCollector collector;
    private Configuration config;

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
    }

    @Override
    public void execute(Tuple input) {
        String xml = input.getString(0);

        logger.info("got data:");
        logger.info(xml);

        Map<Method, ValueGroup> outputData = new HashMap<Method, ValueGroup>();
        try {
            outputData = InputCalculator.calculate(config, xml);

            for (Method key : outputData.keySet()) {
                logger.info(key + " "  + " " + outputData.get(key));
            }

            collector.emit(new Values(xml, outputData));
            collector.ack(input);

        } catch (XPathExpressionException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(TupleConstants.XML_STRING_KEY, TupleConstants.PARSED_XML_KEY));
    }
}
