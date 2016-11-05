package ru.spbstu.frauddetection.core.fraud_detector.TransferContainers;

import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

// temporary object architecture
public class DetectableDataContainer implements Serializable {
    private String inputXML;
    private Map<Method, ValueGroup> parsedData;
    private List<ValueGroup> db;

    public DetectableDataContainer(String inputXML, Map<Method, ValueGroup> parsedData,
                                   List<ValueGroup> db)
    {
        this.inputXML = inputXML;
        this.parsedData = parsedData;
        this.db = db;
    }

    public String getInputXML() {
        return inputXML;
    }

    public Map<Method, ValueGroup> getParsedData() {
        return parsedData;
    }

    public List<ValueGroup> getDB() {
        return db;
    }

//    for debugging
    @Override
    public String toString() {
        String str = "InputXML: " + inputXML + "\n" +
                     "ParsedData: " + parsedData + "\n";

        if (db != null) {
            str += "DB not null";
        }

        return str;
    }
}
