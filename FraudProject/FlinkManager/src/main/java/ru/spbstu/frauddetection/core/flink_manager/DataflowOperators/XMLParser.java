package ru.spbstu.frauddetection.core.flink_manager.DataflowOperators;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.log4j.Logger;
import org.xml.sax.SAXException;
import ru.spbstu.frauddetection.FraudConfig.ConfigurationParser.ConfigurationParser;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.InputDataCalculator.InputCalculator;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;
import ru.spbstu.frauddetection.core.flink_manager.DataflowConfigurationConstants;
import ru.spbstu.frauddetection.core.flink_manager.DataflowCustomTypes;

import javax.xml.bind.JAXBException;
import java.util.Map;

public class XMLParser extends RichMapFunction<String, DataflowCustomTypes.ParsedXMLTuple> {
    private final static Logger logger = Logger.getLogger(XMLParser.class);

    private Configuration xmlConfig;

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws JAXBException, SAXException {
        ParameterTool globalParameters = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String xmlConfigString = globalParameters.getRequired(DataflowConfigurationConstants.XML_CONFIGURATION_KEY);

        xmlConfig = new ConfigurationParser().parseConfiguration(xmlConfigString);
    }

    public DataflowCustomTypes.ParsedXMLTuple map(String xmlString) throws Exception {
        logger.info("got data:");
        logger.info(xmlString);

        Map<Method, ValueGroup> parsedXML = InputCalculator.calculate(xmlConfig, xmlString);

        for (Method key : parsedXML.keySet()) {
            logger.info(key + " "  + " " + parsedXML.get(key));
        }

        return new DataflowCustomTypes.ParsedXMLTuple(xmlString, parsedXML);
    }
}
