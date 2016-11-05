package ru.spbstu.frauddetection.detection;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Scanner;

public class SentenceDetectionTest extends TestCase implements Serializable {

    static final Integer NUM_SAMPLES = 100000;

    @Test
    public void testSimple() throws Exception
    {

        Scanner s = new Scanner(new File("src/main/resources/oil.txt"));
        ArrayList<String> list = new ArrayList<String>();
        while (s.hasNextLine()){
            list.add(s.nextLine());
        }
        s.close();

        SentenceDetection detector = new SentenceDetection();
        assertFalse(detector.detect(list, "У людей, страдающих гипертонией, давление может резко и сильно вырасти до " +
                "критических значений, угрожающих здоровью."));
        assertTrue(detector.detect(list, "Кредитный рейтинг России в 2016 году достигнет рекордных величин."));
        detector.close();
    }

}