package ru.spbstu.frauddetection.core.storm_manager;

import org.junit.Test;
import ru.spbstu.frauddetection.FraudConfig.ConfigurationParser.ConfigurationParser;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;
import ru.spbstu.frauddetection.InputDataCalculator.ValueType;

import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FraudDetectionKMeansTest {
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
                    "   <group method=\"KMeans\">\n" +
                    "       <field>\n" +
                    "           <xpath_name>DrugCount</xpath_name>\n" +
                    "           <type>Integer</type>\n" +
                    "       </field>\n" +
                    "       <field>\n" +
                    "           <xpath_name>MonthsCount</xpath_name>\n" +
                    "           <type>Integer</type>\n" +
                    "       </field>\n" +
                    "   </group>\n" +
                    "</fraudconfig>";

    @Test
    public void testFraudDetected() throws Exception {
        Configuration config = new ConfigurationParser().parseConfiguration(configStr);

        FraudDetectionManager detectionManager = new FraudDetectionManager(config);

        Map<Method, ValueGroup> fraudDetectable = new HashMap<>();

        ValueType value = new ValueType(600, "DrugCount");
        ValueType value2 = new ValueType(3, "MonthsCount");
        ValueGroup valueGroup = createNewValueGroup(value, value2);

        fraudDetectable.put(Method.KMeans, valueGroup);

        List<ValueGroup> dbList = getTestDBList();

        assertTrue(detectionManager.fraudDetected(fraudDetectable, dbList));

        detectionManager.close();
    }

    @Test
    public void testFraudNotDetected() throws Exception {
        Configuration config = new ConfigurationParser().parseConfiguration(configStr);

        FraudDetectionManager detectionManager = new FraudDetectionManager(config);

        Map<Method, ValueGroup> fraudDetectable = new HashMap<>();

        ValueType value = new ValueType(45, "DrugCount");
        ValueType value2 = new ValueType(3, "MonthsCount");
        ValueGroup valueGroup = createNewValueGroup(value, value2);

        fraudDetectable.put(Method.KMeans, valueGroup);

        List<ValueGroup> dbList = getTestDBList();

        assertFalse(detectionManager.fraudDetected(fraudDetectable, dbList));

        detectionManager.close();
    }

    private List getTestDBList() {
        List<ValueGroup> dbList = new ArrayList<>();

        ValueType v1 = new ValueType(100, "Potion");
        ValueType v2 = new ValueType(2, "TimesPerDay");
        ValueType v3 = new ValueType(50, "DrugCount");
        ValueType v4 = new ValueType(3, "MonthsCount");
        dbList.add(createNewValueGroup(v1, v2, v3, v4));

        v1 = new ValueType(120, "Potion");
        v2 = new ValueType(3, "TimesPerDay");
        v3 = new ValueType(45, "DrugCount");
        v4 = new ValueType(4, "MonthsCount");
        dbList.add(createNewValueGroup(v1, v2, v3, v4));

        v1 = new ValueType(100, "Potion");
        v2 = new ValueType(3, "TimesPerDay");
        v3 = new ValueType(60, "DrugCount");
        v4 = new ValueType(2, "MonthsCount");
        dbList.add(createNewValueGroup(v1, v2, v3, v4));

        v1 = new ValueType(110, "Potion");
        v2 = new ValueType(4, "TimesPerDay");
        v3 = new ValueType(50, "DrugCount");
        v4 = new ValueType(5, "MonthsCount");
        dbList.add(createNewValueGroup(v1, v2, v3, v4));

        return dbList;
    }

    private ValueGroup createNewValueGroup(ValueType... values) {
        ValueGroup group = new ValueGroup();
        group.setValues(Arrays.asList(values));

        return group;
    }
}
