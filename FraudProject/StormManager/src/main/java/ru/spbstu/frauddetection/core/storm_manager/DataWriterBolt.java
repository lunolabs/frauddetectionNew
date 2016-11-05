package ru.spbstu.frauddetection.core.storm_manager;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;

import java.util.HashMap;
import java.util.Map;

public class DataWriterBolt implements BoltInterface {
    private final static Logger logger = Logger.getLogger(DataWriterBolt.class);

    private OutputCollector collector;
    private MockData db;

    public DataWriterBolt(MockData db) {
        this.db = db;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    public void execute(Tuple tuple) {
        
        Boolean check = (Boolean) tuple.getValueByField(TupleConstants.VERDICT_SUCCESS_KEY);
        logger.info("got check passed: " + check);
        
        if(check) {
            String xmlInput = (String) tuple.getValueByField(TupleConstants.XML_STRING_KEY);

            logger.info("got xml: ");
            logger.info(xmlInput);

//            db.addValue(xmlInput);
            
//            Map <Method, ValueGroup> values = (HashMap<Method, ValueGroup>)tuple.getValueByField(TupleConstants.PARSED_XML_KEY);
//            logger.info("got map");
//            for (Method key : values.keySet()) {
//                logger.info(key + " "  + " " + values.get(key));
//            }
//
//            logger.info("added value to data base");
        }

        collector.ack(tuple);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}
}
