package ru.spbstu.frauddetection.detection;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.lang.Math;

public class NaiveBayesTest extends TestCase implements Serializable {

    static final Integer NUM_SAMPLES = 10000;
    static final Integer NUM_TESTS = 100;

    @Test
    public void testGauss() throws Exception
    {

        Random rand = new Random();
        List<List<Double> > data = new ArrayList<List<Double> >();

        for (int i = 0; i < NUM_SAMPLES; i++) {
            List<Double> tmp = new ArrayList<>();
            tmp.add(0.0);
            tmp.add(Math.abs(rand.nextGaussian())*5);
            tmp.add(Math.abs(rand.nextGaussian())*5);
            data.add(tmp);
        }
        NaiveBayesDetection detector = new NaiveBayesDetection();
        try {
            for(int i = 0; i < NUM_TESTS; ++i) {
                List<Double> test = Arrays.asList(0.0,
                    Math.abs(rand.nextGaussian())*5,
                    Math.abs(rand.nextGaussian())*5);
                assertTrue(detector.detect(data, test));
                test = Arrays.asList(0.0,
                    Math.abs(100 + rand.nextGaussian())*100,
                    Math.abs(100 + rand.nextGaussian())*100);
                assertFalse(detector.detect(data, test));
            }
        } catch(Exception ex) {
            ex.printStackTrace();
        }
        detector.close();
    }
}
