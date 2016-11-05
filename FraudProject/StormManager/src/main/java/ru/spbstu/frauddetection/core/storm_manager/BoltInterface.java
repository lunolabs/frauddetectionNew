package ru.spbstu.frauddetection.core.storm_manager;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.io.Serializable;
import java.util.Map;

public interface BoltInterface extends IRichBolt, Serializable{
    @Override
    void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector);

    @Override
    void execute(Tuple tuple);

    @Override
    default void cleanup() {}

    @Override
    default Map<String, Object> getComponentConfiguration() {
        return null;
    }

    @Override
    void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer);
}
