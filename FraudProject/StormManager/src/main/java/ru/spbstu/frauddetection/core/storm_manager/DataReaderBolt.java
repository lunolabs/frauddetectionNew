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
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;

import javax.xml.bind.JAXBException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DataReaderBolt implements BoltInterface {
    private final static Logger logger = Logger.getLogger(DataReaderBolt.class);
    
    private MockData db;
    private OutputCollector collector;
    private Configuration config;
    
    DataReaderBolt(MockData db)
    {
        this.db = db;
    }
    
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
        
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
    public void execute(Tuple tuple) {
        if (config == null) {
            logger.fatal("Error on parsing configuration");
        }

        String xml = tuple.getStringByField(TupleConstants.XML_STRING_KEY);
        Map<Method, ValueGroup> parsedXML = (HashMap<Method, ValueGroup>) tuple.getValueByField(TupleConstants.PARSED_XML_KEY);

        logger.info("received xml:");
        logger.info(xml);
        logger.info("got map:");

        for (Method key : parsedXML.keySet()) {
            logger.info(key + " "  + " " + parsedXML.get(key));
        }

        List<ValueGroup> dbList = db.getValues(config.getUniqueFields());

        logger.info("sending data from data base:");
        logger.info(dbList);
        
        collector.emit(new Values(xml, parsedXML, dbList));
        collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(TupleConstants.XML_STRING_KEY,
                                                TupleConstants.PARSED_XML_KEY, TupleConstants.DATA_BASE_KEY));
    }
}
