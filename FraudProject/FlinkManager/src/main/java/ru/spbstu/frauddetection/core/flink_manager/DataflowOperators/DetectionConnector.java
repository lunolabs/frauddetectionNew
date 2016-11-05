package ru.spbstu.frauddetection.core.flink_manager.DataflowOperators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.log4j.Logger;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;
import ru.spbstu.frauddetection.core.flink_manager.DataflowCustomTypes;
import ru.spbstu.frauddetection.core.fraud_detector.TransferContainers.DetectableDataContainer;

import java.util.List;
import java.util.Map;

public class DetectionConnector implements
        MapFunction<DataflowCustomTypes.ParsedXMLDBTuple, DetectableDataContainer> {
    private final static Logger logger = Logger.getLogger(DetectionConnector.class);

    @Override
    public DetectableDataContainer map(DataflowCustomTypes.ParsedXMLDBTuple input) throws Exception {
        String inputXML = input.f0;
        Map<Method, ValueGroup> parsedXML = input.f1;
        List<ValueGroup> db = input.f2;

        logger.debug("Creating container");

        return new DetectableDataContainer(inputXML, parsedXML, db);
    }
}
