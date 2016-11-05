package ru.spbstu.frauddetection.detection;


import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.List;

public class LogisticDetection extends DetectionBaseSpark<String> {

    public LogisticDetection() {
        super();
    }

    public LogisticDetection(String str) {
        super(str);
    }

    public LogisticDetection(JavaSparkContext sc) {
        super(sc);
    }

    private Vector StringToVector(String s){
        String[] sarray = s.split(" ");
        double[] values = new double[sarray.length];
        for (int i = 0; i < sarray.length; i++)
            values[i] = Double.parseDouble(sarray[i]);
        return Vectors.dense(values);
    }

    @Override
    public Boolean detect(List<String> data, String value) {
        JavaRDD<String> dataStrings = sc.parallelize(data);
        JavaRDD<LabeledPoint> training = dataStrings.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length-1];
            for (int i = 1; i < sarray.length; i++)
                values[i-1] = Double.parseDouble(sarray[i]);
            return new LabeledPoint(Double.parseDouble(sarray[0]), Vectors.dense(values));
        });


        final LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
                .setNumClasses(2)
                .run(training.rdd());

        Double prediction = model.predict(StringToVector(value));
        System.out.println("Value: " + value + " result: " + prediction);

        return prediction >= 0.6;
    }
}

