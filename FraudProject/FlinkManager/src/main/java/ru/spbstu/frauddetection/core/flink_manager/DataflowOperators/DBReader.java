package ru.spbstu.frauddetection.core.flink_manager.DataflowOperators;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.log4j.Logger;
import ru.spbstu.frauddetection.FraudConfig.ConfigurationParser.ConfigurationParser;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;
import ru.spbstu.frauddetection.core.flink_manager.DataflowConfigurationConstants;
import ru.spbstu.frauddetection.core.flink_manager.DataflowCustomTypes;
import ru.spbstu.frauddetection.core.flink_manager.MockData;

import java.util.*;

public class DBReader extends
        RichMapFunction<DataflowCustomTypes.ParsedXMLTuple, DataflowCustomTypes.ParsedXMLDBTuple> {
    private final static Logger logger = Logger.getLogger(DBReader.class);

    private Configuration xmlConfig;
    private MockData db;

    public DBReader(MockData db) {
        this.db = db;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        ParameterTool globalParameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String xmlConfigString = globalParameters.getRequired(DataflowConfigurationConstants.XML_CONFIGURATION_KEY);

        xmlConfig = new ConfigurationParser().parseConfiguration(xmlConfigString);

        if (xmlConfig == null) {
            logger.fatal("Error on parsing configuration");
        }
    }

    public DataflowCustomTypes.ParsedXMLDBTuple
    map(DataflowCustomTypes.ParsedXMLTuple input) throws Exception {
        String inputXML = input.f0;
        Map<Method, ValueGroup> parsedXML = input.f1;

        logger.info("received xml:");
        logger.info(inputXML);
        logger.info("got map:");

        for (Method key : parsedXML.keySet()) {
            logger.info(key + " "  + " " + parsedXML.get(key));
        }

        List<ValueGroup> dbList = db.getValues(xmlConfig.getUniqueFields());

        logger.info("sending data from data base:");
        logger.info(dbList);

        return new DataflowCustomTypes.ParsedXMLDBTuple(inputXML, parsedXML, dbList);
    }
}
