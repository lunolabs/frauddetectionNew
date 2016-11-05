package ru.spbstu.frauddetection.detection;

import java.util.List;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Group;
import ru.spbstu.frauddetection.InputDataCalculator.InputGroup;

public abstract class DetectionBase<T> {
    public Boolean detect(List<T> data, T value) {
        return null;
    }

    public Object convertToType(InputGroup valueGroup, Group configGroup) {
        return null;
    }

    public List convertToTypeList(List<InputGroup> data, Group configGroup) {
        return null;
    }
}
