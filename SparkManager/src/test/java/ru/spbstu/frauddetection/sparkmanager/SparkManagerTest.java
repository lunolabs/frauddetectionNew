package ru.spbstu.frauddetection.sparkmanager;

import junit.framework.TestCase;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Scanner;

public class SparkManagerTest extends TestCase implements Serializable {

    @Test
    public void testRun() throws Exception {
        SparkManager sm = new SparkManager();
        sm.run();
    }
}
