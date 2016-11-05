package ru.spbstu.frauddetection.core.storm_manager;

import org.junit.Test;

import org.xml.sax.SAXException;
import ru.spbstu.frauddetection.FraudConfig.ConfigurationParser.ConfigurationParser;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;
import ru.spbstu.frauddetection.InputDataCalculator.ValueType;
import ru.spbstu.frauddetection.detection.QuantilleDetection;

import javax.xml.bind.JAXBException;
import java.util.*;

import static org.junit.Assert.*;

public class FraudDetectionQuantilleTest {
    private final String configStr =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                    "<fraudconfig>\n" +
                    "    <group method=\"Quantile\">\n" +
                    "        <field>\n" +
                    "            <xpath_name>Potion</xpath_name>\n" +
                    "            <type>Integer</type>\n" +
                    "        </field>\n" +
                    "        <field>\n" +
                    "            <xpath_name>TimesPerDay</xpath_name>\n" +
                    "            <type>Integer</type>\n" +
                    "        </field>\n" +
                    "    </group>\n" +
                    "</fraudconfig>";

    @Test
    public void testOneFieldFraudDetected() throws Exception {
        Configuration config = new ConfigurationParser().parseConfiguration(configStr);
        FraudDetectionManager detectionManager = new FraudDetectionManager(config);

        Map<Method, ValueGroup> fraudDetectable = new HashMap<>();
        ValueType value = new ValueType(561, "Potion");
        ValueGroup group = createNewValueGroup(value);

        fraudDetectable.put(Method.Quantile, group);

        List<ValueGroup> dbList = getTestDBList();

        assertTrue(detectionManager.fraudDetected(fraudDetectable, dbList));

        detectionManager.close();
    }

    @Test
    public void testFraudDetectedInOneField() throws Exception  {
        Configuration config = new ConfigurationParser().parseConfiguration(configStr);
        FraudDetectionManager detectionManager = new FraudDetectionManager(config);

        Map<Method, ValueGroup> fraudDetectable = new HashMap<>();

        ValueType value = new ValueType(110, "Potion");
        ValueType value2 = new ValueType(15, "TimesPerDay");
        ValueGroup group = createNewValueGroup(value, value2);

        fraudDetectable.put(Method.Quantile, group);

        List<ValueGroup> dbList = getTestDBList();

        assertTrue(detectionManager.fraudDetected(fraudDetectable, dbList));

        detectionManager.close();
    }

    @Test
    public void testMultipleFraudDetected() throws Exception {
        Configuration config = new ConfigurationParser().parseConfiguration(configStr);
        FraudDetectionManager detectionManager = new FraudDetectionManager(config);

        Map<Method, ValueGroup> fraudDetectable = new HashMap<>();

        ValueType value = new ValueType(561, "Potion");
        ValueType value2 = new ValueType(9, "TimesPerDay");
        ValueGroup valueGroup = createNewValueGroup(value, value2);

        fraudDetectable.put(Method.Quantile, valueGroup);

        List<ValueGroup> dbList = getTestDBList();

        assertTrue(detectionManager.fraudDetected(fraudDetectable, dbList));

        detectionManager.close();
    }


    @Test
    public void testFraudNotDetected() throws Exception {
        Configuration config = new ConfigurationParser().parseConfiguration(configStr);
        FraudDetectionManager detectionManager = new FraudDetectionManager(config);

        Map<Method, ValueGroup> fraudDetectable = new HashMap<>();

        ValueType value = new ValueType(110, "Potion");
        ValueType value2 = new ValueType(3, "TimesPerDay");
        ValueGroup valueGroup = createNewValueGroup(value, value2);

        fraudDetectable.put(Method.Quantile, valueGroup);

        List<ValueGroup> dbList = getTestDBList();

        assertFalse(detectionManager.fraudDetected(fraudDetectable, dbList));

        detectionManager.close();
    }

    private List getTestDBList() {
        List<ValueGroup> dbList = new ArrayList<>();

        ValueType v1 = new ValueType(100, "Potion");
        ValueType v2 = new ValueType(2, "TimesPerDay");
        dbList.add(createNewValueGroup(v1, v2));

        v1 = new ValueType(120, "Potion");
        v2 = new ValueType(3, "TimesPerDay");
        dbList.add(createNewValueGroup(v1, v2));

        v1 = new ValueType(100, "Potion");
        v2 = new ValueType(3, "TimesPerDay");
        dbList.add(createNewValueGroup(v1, v2));

        v1 = new ValueType(110, "Potion");
        v2 = new ValueType(4, "TimesPerDay");
        dbList.add(createNewValueGroup(v1, v2));

        return dbList;
    }

    private ValueGroup createNewValueGroup(ValueType... values) {
        ValueGroup group = new ValueGroup();
        group.setValues(Arrays.asList(values));

        return group;
    }
}