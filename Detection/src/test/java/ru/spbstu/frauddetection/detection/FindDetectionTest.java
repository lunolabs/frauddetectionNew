package ru.spbstu.frauddetection.detection;

import junit.framework.TestCase;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;

public class FindDetectionTest {
    String strTrue = "word2";
    String strFalse = "word11";
    List<String> strs = Arrays.asList("word1", strTrue, "word3", "word4");

    Integer intgTrue = 15;
    Integer intgFalse = 1;
    List<Integer> intgs = Arrays.asList(9, 12, intgTrue, 18, 21);

    FindDetection detector = new FindDetection();

    @Test
    public void testExist() {
        assertTrue(detector.detect(strs, strTrue));
        assertTrue(detector.detect(intgs, intgTrue));
    }

    @Test
    public void testNoExist() {
        assertFalse(detector.detect(strs, strFalse));
        assertFalse(detector.detect(intgs, intgFalse));
    }
}
