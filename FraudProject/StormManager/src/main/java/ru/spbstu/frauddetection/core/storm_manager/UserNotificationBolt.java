package ru.spbstu.frauddetection.core.storm_manager;

import org.apache.log4j.Logger;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;
import ru.spbstu.frauddetection.core.web_service.ReportGenerator;

import java.util.HashMap;
import java.util.Map;

public class UserNotificationBolt implements BoltInterface {
    private final static Logger logger = Logger.getLogger(UserNotificationBolt.class);

    private OutputCollector collector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
        String xml = tuple.getStringByField(TupleConstants.XML_STRING_KEY);
        String fraudMessage = tuple.getStringByField(TupleConstants.FRAUD_MESSAGE_KEY);
        Boolean check = (Boolean) tuple.getValueByField(TupleConstants.VERDICT_SUCCESS_KEY);

        logger.info("check passed: " + check);
        logger.info("got xml: ");
        logger.info(xml);

        if (! check) {
            if (fraudMessage == null) {
                logger.error("Fraud detected but no fraud message found");
            } else {
                logger.info(fraudMessage);
            }
        }

        try {
            new ReportGenerator().generate(xml, fraudMessage, check);
        } catch (Exception e) {
            e.printStackTrace();
        }

        collector.ack(tuple);
    }


    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {}

}
