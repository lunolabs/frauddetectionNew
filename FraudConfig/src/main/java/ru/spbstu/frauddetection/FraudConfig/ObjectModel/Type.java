package ru.spbstu.frauddetection.FraudConfig.ObjectModel;

import java.io.Serializable;

public enum Type implements Serializable {
    Integer,
    String,
    Float,
    Boolean,
    Text,
    Enum,
    Date
}
