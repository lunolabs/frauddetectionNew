package ru.spbstu.frauddetection.core.fraud_detector;

import org.xml.sax.SAXException;

import javax.xml.bind.JAXBException;
import java.io.IOException;

public class DetectionStarter {
    public static void main(String[] args) throws IOException, JAXBException, SAXException {
        DetectionDataflowManager detectionDataflowManager = new DetectionDataflowManager();
        detectionDataflowManager.start();
    }
}
