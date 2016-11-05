package ru.spbstu.frauddetection.sparkmanager;

import org.junit.Test;
import ru.spbstu.frauddetection.FraudConfig.ConfigurationParser.ConfigurationParser;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;
import ru.spbstu.frauddetection.InputDataCalculator.ValueType;

import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FraudDetectionKMeansDateTimeTest {
    private static final int NUM_REPEATING = 5;
    
	private final String configStr =
            "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
                    "<fraudconfig>\n" +
                    "    <group method=\"KMeansDateTime\">\n" +
                    "        <field>\n" +
                    "            <xpath_name>TransactionTime</xpath_name>\n" +
                    "            <type>Date</type>\n" +
                    "        </field>\n" +
                    "    </group>\n" +
                    "</fraudconfig>";
    
    @Test
    public void testFraudDetected() throws Exception {
        Configuration config = new ConfigurationParser().parseConfiguration(configStr);

        FraudDetectionManager detectionManager = new FraudDetectionManager(config);

        Map<Method, ValueGroup> fraudDetectable = new HashMap<>();

        ValueType koValue = new ValueType("2016-02-07T00:00Z", "TransactionTime");
        ValueGroup valueGroup = createNewValueGroup(koValue);

        fraudDetectable.put(Method.KMeansDateTime, valueGroup);

        List<ValueGroup> dbList = getTestDBList();

        assertTrue(detectionManager.fraudDetected(fraudDetectable, dbList));

        detectionManager.close();
    }
    
    @Test
    public void testFraudNotDetected() throws Exception {
        Configuration config = new ConfigurationParser().parseConfiguration(configStr);

        FraudDetectionManager detectionManager = new FraudDetectionManager(config);

        Map<Method, ValueGroup> fraudDetectable = new HashMap<>();

        ValueType okValue = new ValueType("2016-02-02T00:00Z", "TransactionTime");
        ValueGroup valueGroup = createNewValueGroup(okValue);

        fraudDetectable.put(Method.KMeansDateTime, valueGroup);

        List<ValueGroup> dbList = getTestDBList();

        assertFalse(detectionManager.fraudDetected(fraudDetectable, dbList));

        detectionManager.close();
    }

    
    private List getTestDBList() {
        List<ValueGroup> dbList = new ArrayList<>();
        
        ValueType v1 = new ValueType("2016-02-01T00:00Z", "TransactionTime");
        for (int i = 0; i < NUM_REPEATING; ++i) {
        	dbList.add(createNewValueGroup(v1));
        }
        ValueType v2 = new ValueType("2016-02-02T00:00Z", "TransactionTime");
        for (int i = 0; i < NUM_REPEATING; ++i) {
        	dbList.add(createNewValueGroup(v2));
        }
        ValueType v3 = new ValueType("2016-02-03T00:00Z", "TransactionTime");
        for (int i = 0; i < NUM_REPEATING; ++i) {
        	dbList.add(createNewValueGroup(v3));
        }
        ValueType v4 = new ValueType("2016-02-04T00:00Z", "TransactionTime");
        for (int i = 0; i < NUM_REPEATING; ++i) {
        	dbList.add(createNewValueGroup(v4));
        }
        ValueType v5 = new ValueType("2016-02-05T00:00Z", "TransactionTime");
        for (int i = 0; i < NUM_REPEATING; ++i) {
        	dbList.add(createNewValueGroup(v5));
        }
        
        

        return dbList;
    }
    
    private ValueGroup createNewValueGroup(ValueType... values) {
        ValueGroup group = new ValueGroup();
        group.setValues(Arrays.asList(values));

        return group;
    }
}
