package ru.spbstu.frauddetection.detection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import java.util.HashMap;
import java.util.List;

public class RandomForestDetection extends DetectionBaseSpark<String>{

    public RandomForestDetection(JavaSparkContext sc) {
        super(sc);
    }

    public RandomForestDetection() {
        super();
    }

    public RandomForestDetection(String master){
        super(master);
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


        // Set parameters.
        //  Empty categoricalFeaturesInfo indicates all features are continuous.
        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        Integer numTrees = 5;
        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        Integer seed = 12345;

        final RandomForestModel model = RandomForest.trainClassifier(training, numClasses,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
                seed);

        Double prediction = model.predict(StringToVector(value));
        System.out.println("Value: " + value + " result: " + prediction);

        return prediction >= 0.6;
    }
}
