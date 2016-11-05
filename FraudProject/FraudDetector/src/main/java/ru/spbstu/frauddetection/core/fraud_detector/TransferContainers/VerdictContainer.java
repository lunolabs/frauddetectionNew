package ru.spbstu.frauddetection.core.fraud_detector.TransferContainers;

import java.io.Serializable;

public class VerdictContainer implements Serializable {
    private Boolean checkPassed;
    private String xmlString;

    public VerdictContainer(Boolean checkPassed, String xmlString) {
        this.checkPassed = checkPassed;
        this.xmlString = xmlString;
    }

    public Boolean getCheckPassed() {
        return checkPassed;
    }

    public String getXmlString() {
        return xmlString;
    }
}
