package ru.spbstu.frauddetection.detection;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Scanner;

public class DocumentsClassificatorTest extends TestCase implements Serializable {

    private final static String DATA = "src/test/resources/testDocClassiferData.txt";
    private final static String TESTTRYE = "src/test/resources/testDocClassiferValuesTrue.txt";
    private final static String TESTFALSE = "src/test/resources/testDocClassiferValuesFalse.txt";
    private final static String CODER = "utf-8";

    @Test
    public void testClassifer() throws Exception {
        Scanner s = new Scanner(new File(DATA), CODER);
        ArrayList<String> data = new ArrayList<String>();
        while(s.hasNextLine()) {
            data.add(s.nextLine());
        }
        s.close();

        s = new Scanner(new File(TESTTRYE), CODER);
        ArrayList<String> valuesTrue = new ArrayList<String>();
        while(s.hasNextLine()) {
            valuesTrue.add(s.nextLine());
        }
        s.close();

        s = new Scanner(new File(TESTFALSE), CODER);
        ArrayList<String> valuesFalse = new ArrayList<String>();
        while(s.hasNextLine()) {
            valuesFalse.add(s.nextLine());
        }
        s.close();

        DocumentsClassificatorDetection docClassifer = new DocumentsClassificatorDetection();

        for(String val : valuesTrue) {
            assertTrue(docClassifer.detect(data, val));
        }
        for(String val : valuesFalse) {
            assertFalse(docClassifer.detect(data, val));
        }
        docClassifer.close();
    }
}
