package ru.spbstu.frauddetection.detection;

import org.apache.spark.api.java.JavaSparkContext;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Field;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Group;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Type;
import ru.spbstu.frauddetection.InputDataCalculator.InputGroup;
import ru.spbstu.frauddetection.InputDataCalculator.InputType;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SentenceDetection extends DetectionBaseSpark<String> {

    private String modelPath = "./src/main/resources/ruscorpora.model.bin";

    public SentenceDetection(JavaSparkContext sc) {
        super(sc);
    }

    public SentenceDetection() {
        super();
    }

    public SentenceDetection(String master){
        super(master);
    }

    public void setModel(String model) {
        modelPath = model;
    }

    @Override
    public Boolean detect(List<String> data, String value) {
        List<String> args = new ArrayList<>(data);

        String pythonPath = "/use/your/absolute/path/here";
        args.add(0, pythonPath);

//        args.add(0, "./src/python/PyDetectionExternal.py");
        args.add(0, "python3");
        args.add(value);
        ProcessBuilder pc = new ProcessBuilder(args.toArray(new String[0]));
        pc.redirectErrorStream(true);
        Boolean res = false;
        try {
            Process p = pc.start();
            p.waitFor();
            BufferedReader in = new BufferedReader(new InputStreamReader(p.getInputStream()));
            String output = in.readLine();
            if(output != null) {
                System.out.println(output);
                res = output.equals("true");
            }
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }

        return res;
    }

    @Override
    public List<String> convertToType(InputGroup valueGroup, Group configGroup) {
        List<String> converted = new ArrayList<>();

        for (InputType value : valueGroup.getValues()) {
            String valueName = value.getFieldName();

            for (Field field : configGroup.getFields()) {
                if (valueName.equals(field.getXpathName()) && field.getType() == Type.String) {
                    Collections.addAll(converted, ((String) value.getT()).split("[.]"));
                }
            }
        }

        System.out.println("Sentence converted: ");
        System.out.println(converted);

        return converted;
    }

    @Override
    public List<String> convertToTypeList(List<InputGroup> data, Group configGroup) {
        List<String> convertedList = new ArrayList<>();

        for (InputGroup group : data) {
            convertedList.addAll(convertToType(group, configGroup));
        }

        return convertedList;
    }
}
