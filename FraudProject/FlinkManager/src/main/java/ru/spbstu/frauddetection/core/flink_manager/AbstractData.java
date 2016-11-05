package ru.spbstu.frauddetection.core.flink_manager;

import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Field;
import ru.spbstu.frauddetection.InputDataCalculator.InputGroup;
import ru.spbstu.frauddetection.InputDataCalculator.InputType;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

abstract class AbstractData implements Serializable {
    private List<InputGroup> data;

    public AbstractData()
    {
        data = new ArrayList<InputGroup>();
    }
    public List<InputGroup> getData() {
        return data;
    }

    public void setData(List<InputGroup> data) {
        this.data = data;
    }

    public List<InputGroup> getValues(List<Field> list){return null;};

    public void addValue(File xmlInput) {}

    public List<InputGroup> getNormalList(InputGroup groupInput, List<InputGroup> db) {
        List<InputGroup> list = new ArrayList<InputGroup>();


        for (InputGroup valueGroup : db) {
            List<InputType> tmp = new ArrayList<InputType>();
            for (InputType val : groupInput.getValues()) {
                for (InputType valDB : valueGroup.getValues())
                    if (valDB.getFieldName().equals(val.getFieldName()))
                        tmp.add(valDB);
            }
            if (!tmp.isEmpty()) {
                InputGroup tmpGroup = new InputGroup();
                tmpGroup.setValues(tmp);
                list.add(tmpGroup);
            }
        }

        return list;
    }

    @Override
    public String toString() {
        return "MockData{" +
                "db=" + data +
                '}';
    }

}
