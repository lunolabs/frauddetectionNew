package ru.spbstu.frauddetection.InputDataCalculator;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class InputGroup implements Serializable {
    
    private List<InputType> values;
    private InputGroup group;

    public InputGroup()
    {
        values = new ArrayList<InputType>();
    }

    public List<InputType> getValues() {
        return values;
    }

    public void setValues(List<InputType> values) {
        this.values = values;
    }

    public InputGroup getGroup() {
        return group;
    }

    public void setGroup(InputGroup group) {
        this.group = group;
    }

    public void setValues(InputGroup values){
        values.setValues(values.getValues());
    }

    @Override
    public String toString() {
        return "InputGroup{" +
                "values=" + values +
                ", group=" + group +
                '}';
    }
}
