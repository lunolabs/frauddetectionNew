package ru.spbstu.frauddetection.detection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.*;
import ru.spbstu.frauddetection.InputDataCalculator.InputGroup;
import ru.spbstu.frauddetection.InputDataCalculator.InputType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class QuantilleDetection<T extends Number> extends DetectionBaseSpark<T>{

    private Integer precision = 3;

    public QuantilleDetection(JavaSparkContext sc) {
        super(sc);
    }

    public QuantilleDetection() {
        super();
    }

    public QuantilleDetection(String master){
       super(master);
    }

    public void setPrecision(Integer prec) {
        precision = prec;
    }

    @Override
    public Boolean detect(List<T> data, T value) {

        JavaRDD<T> dataRDD = sc.parallelize(data);
        List<Double> quant25List = dataRDD.map(Number::doubleValue).takeOrdered(data.size() / 4);
        Double quant25 = quant25List.get(quant25List.size() - 1);
        List<Double> quant75List = dataRDD.map(Number::doubleValue).takeOrdered(data.size() / 4, Comparator.<Double>reverseOrder());
        Double quant75 = quant75List.get(quant75List.size()-1);
        System.out.println("Q25: " + quant25 + " Q75: " + quant75);
        Double dist = quant75 - quant25;
        Double dvalue = value.doubleValue();

        return quant25 - precision * dist < dvalue && quant75 + precision * dist > dvalue;
    }

    @Override
    public List<Number> convertToType(InputGroup valueGroup, Group configGroup) {
        List<Number> converted = new ArrayList<>();

        for (InputType value : valueGroup.getValues()) {
            String valueName = value.getFieldName();

            for (Field field : configGroup.getFields()) {
                if (valueName.equals(field.getXpathName()) &&
                   (field.getType() == Type.Integer || field.getType() == Type.Double)) {
                    converted.add((Number) value.getT());
                }
            }
        }

        System.out.println("Quantile converted: ");
        System.out.println(converted);

        return converted;
    }

    @Override
    public List<List<Number>> convertToTypeList(List<InputGroup> data, Group configGroup) {
        List<List<Number>> convertedList = new ArrayList<>();

        for (InputGroup group : data) {
            convertedList.add(convertToType(group, configGroup));
        }

        return convertedList;
    }
}
