package ru.spbstu.frauddetection.detection;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class QuantilleDetectionTest extends TestCase {

    static final Integer NUM_SAMPLES = 1000;

    @Test
    public void testDouble() throws Exception
    {
        Random rand = new Random();
        List<Double> data = new ArrayList<>();

        for (int i = 0; i < NUM_SAMPLES; i++) {
            data.add(rand.nextGaussian()*5);
        }

        QuantilleDetection<Double> detector = new QuantilleDetection<>();
        assertTrue(detector.detect(data, 2.0));
        assertFalse(detector.detect(data, 1000.0));
        detector.close();
    }

    @Test
    public void testInteger() throws Exception
    {
        Random rand = new Random();
        List<Integer> data = new ArrayList<>();

        for (int i = 0; i < NUM_SAMPLES; i++) {
            data.add(rand.nextInt(10));
        }

        QuantilleDetection<Integer> detector = new QuantilleDetection<>();
        assertTrue(detector.detect(data, 2));
        assertFalse(detector.detect(data, 1000));
        detector.close();

    }
}
