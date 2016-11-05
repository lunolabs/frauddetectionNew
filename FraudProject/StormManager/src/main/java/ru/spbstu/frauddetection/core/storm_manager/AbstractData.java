package ru.spbstu.frauddetection.core.storm_manager;

import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Field;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;
import ru.spbstu.frauddetection.InputDataCalculator.ValueType;

import java.io.File;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

abstract class AbstractData implements Serializable {
    private List<ValueGroup> data;

    public AbstractData()
    {
        data = new ArrayList<ValueGroup>();
    }
    public List<ValueGroup> getData() {
        return data;
    }

    public void setData(List<ValueGroup> data) {
        this.data = data;
    }

    public List<ValueGroup> getValues(List<Field> list){return null;};

    public void addValue(File xmlInput) {}

    public List<ValueGroup> getNormalList(ValueGroup groupInput, List<ValueGroup> db) {
        List<ValueGroup> list = new ArrayList<ValueGroup>();


        for (ValueGroup valueGroup : db) {
            List<ValueType> tmp = new ArrayList<ValueType>();
            for (ValueType val : groupInput.getValues()) {
                for (ValueType valDB : valueGroup.getValues())
                    if (valDB.getFieldName().equals(val.getFieldName()))
                        tmp.add(valDB);
            }
            if (!tmp.isEmpty()) {
                ValueGroup tmpGroup = new ValueGroup();
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
