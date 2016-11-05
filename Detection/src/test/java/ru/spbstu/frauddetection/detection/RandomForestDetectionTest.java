package ru.spbstu.frauddetection.detection;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Scanner;

public class RandomForestDetectionTest extends TestCase implements Serializable {

    static final Integer NUM_SAMPLES = 100000;

    @Test
    public void testPressure() throws Exception
    {

        Scanner s = new Scanner(new File("src/main/resources/blood_pressure_data.txt"));
        ArrayList<String> list = new ArrayList<String>();
        while (s.hasNextLine()){
            list.add(s.nextLine());
        }
        s.close();

        RandomForestDetection detector = new RandomForestDetection();
        assertFalse(detector.detect(list, "150 1"));
        assertTrue(detector.detect(list, "85 1"));
        assertTrue(detector.detect(list, "142 0"));
        detector.close();
    }

}