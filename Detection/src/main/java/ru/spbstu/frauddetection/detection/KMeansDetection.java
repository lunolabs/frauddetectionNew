package ru.spbstu.frauddetection.detection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Field;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Group;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Type;
import ru.spbstu.frauddetection.InputDataCalculator.InputGroup;
import ru.spbstu.frauddetection.InputDataCalculator.InputType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class KMeansDetection extends DetectionBaseSpark<String> {

    public KMeansDetection() {
        super();
    }

    public KMeansDetection(String str) {
        super(str);
    }

    public KMeansDetection(JavaSparkContext sc) {
        super(sc);
    }

    @Override
    public Boolean detect(List<String> data, String value) {
        List<String> dat = new ArrayList<String>(data);
        dat.add(value);
        JavaRDD<String> dataRDD = sc.parallelize(dat);
        JavaRDD<Vector> parsedData = dataRDD.map(s -> {
            String[] sarray = s.split(" ");
            double[] values = new double[sarray.length];
            for (int i = 0; i < sarray.length; i++)
                values[i] = Double.parseDouble(sarray[i]);
            return Vectors.dense(values);
        });
        parsedData.cache();

        // Cluster the data into two classes using KMeans
        int numClusters = 2;
        int numIterations = 20;
        KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

        // Evaluate clustering by computing Within Set Sum of Squared Errors
        double WSSSE = clusters.computeCost(parsedData.rdd());
        System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

        for(Vector vec : parsedData.collect()){
            System.out.println(vec.toJson() + " Cluster #" + clusters.predict(vec));
        }

        List<Integer> clusterNums = clusters.predict(parsedData).collect();

        return Collections.frequency(clusterNums, clusterNums.get(clusterNums.size()-1)) > 1;
    }

    @Override
    public String convertToType(InputGroup valueGroup, Group configGroup) {
        String converted = "";

        for (InputType value : valueGroup.getValues()) {
            String valueName = value.getFieldName();

            for (Field field : configGroup.getFields()) {
                if (valueName.equals(field.getXpathName()) &&
                        (field.getType() == Type.Integer || field.getType() == Type.Double)) {
                    converted += value.getT().toString() + " ";
                }
            }
        }

        System.out.println("KMeans converted: ");
        System.out.println(converted);

        return converted;
    }

    @Override
    public List<String> convertToTypeList(List<InputGroup> data, Group configGroup) {
        List<String> convertedList = new ArrayList<>();

        for (InputGroup group : data) {
            convertedList.add(convertToType(group, configGroup));
        }

        return convertedList;
    }
}

