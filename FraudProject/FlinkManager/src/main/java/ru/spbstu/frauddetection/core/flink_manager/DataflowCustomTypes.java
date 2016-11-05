package ru.spbstu.frauddetection.core.flink_manager;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

// TODO: make types in separate classes?
public class DataflowCustomTypes {
    public static class ParsedXMLTuple extends Tuple2<String, Map<Method, ValueGroup>> implements Serializable {
        public ParsedXMLTuple() { super(); }

        public ParsedXMLTuple(String inputXML, Map<Method, ValueGroup> parsedXML) {
            super(inputXML, parsedXML);
        }
    }

    public static class ParsedXMLDBTuple extends Tuple3<String, Map<Method, ValueGroup>, List<ValueGroup>> implements Serializable {
        public ParsedXMLDBTuple() { super(); }

        public ParsedXMLDBTuple(String inputXML, Map<Method, ValueGroup> parsedXML,
                                List<ValueGroup> dbData) {
            super(inputXML, parsedXML, dbData);
        }
    }

    public static class VerdictTuple extends Tuple2<Boolean, String> implements Serializable {
        public VerdictTuple() { super(); }

        public VerdictTuple(Boolean verdict, String inputXML) {
            super(verdict, inputXML);
        }
    }
}
