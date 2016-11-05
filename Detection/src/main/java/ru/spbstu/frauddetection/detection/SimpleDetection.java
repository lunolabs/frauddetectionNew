package ru.spbstu.frauddetection.detection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class SimpleDetection<T extends Number> extends DetectionBaseSpark<T> {

    public SimpleDetection(JavaSparkContext sc) {
        super(sc);
    }

    public SimpleDetection() {
        super();
    }

    public SimpleDetection(String master){
        super(master);
    }

    @Override
    public Boolean detect(List<T> data, T value) {

        JavaRDD<T> avr = sc.parallelize(data);
        Double average = avr.map(Number::doubleValue).reduce((a, b) -> a + b) /data.size();
        Double sd = Math.sqrt(avr.map(Number::doubleValue).map(x -> (x - average) * (x - average)).reduce((a, b) -> a + b) / data.size());
        System.out.println("Average is is " + average);
        System.out.println("Standard deviation is "+ sd);
        Double dvalue = value.doubleValue();
        return average - 2*sd < dvalue && dvalue < average + 2*sd;
    }
}
