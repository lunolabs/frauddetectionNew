package ru.spbstu.frauddetection.detection;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SimpleDetectionTest extends TestCase {

    static final Integer NUM_SAMPLES = 100000;

    @Test
    public void testDouble() throws Exception
    {
        Random rand = new Random();
        List<Double> data = new ArrayList<>();

        for (int i = 0; i < NUM_SAMPLES; i++) {
            data.add(rand.nextGaussian()*5);
        }

        SimpleDetection<Double> detector = new SimpleDetection<>();
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

        SimpleDetection<Integer> detector = new SimpleDetection<>();
        assertTrue(detector.detect(data, 2));
        assertFalse(detector.detect(data, 1000));
        detector.close();

    }
}
