package ru.spbstu.frauddetection.core.storm_manager;

import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Field;
import ru.spbstu.frauddetection.InputDataCalculator.InputGroup;
import ru.spbstu.frauddetection.datastorage.DataBase;

import java.util.List;

public class HDFSData extends AbstractData {
    private DataBase dataBase = new DataBase();
    private String nameTable;

    HDFSData(String nameTable) {
        super();
        this.nameTable = nameTable;
    }

    public List<InputGroup> getValues(List<Field> list) throws Exception {
        return dataBase.getValues(nameTable, list);
    };

    public void addValue(String strXml) throws Exception {
        dataBase.addValue(nameTable, strXml);
    };
}
