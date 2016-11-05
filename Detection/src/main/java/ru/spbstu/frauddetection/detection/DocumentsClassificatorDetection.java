package ru.spbstu.frauddetection.detection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.*;
import static java.util.Arrays.*;


public class DocumentsClassificatorDetection extends DetectionBaseSpark<String> {

    private static final double LAMBDA = 1.0;
    private static final double MINWORDLEN = 4;

    public DocumentsClassificatorDetection(){
        super();
    }

    public DocumentsClassificatorDetection(String master){
        super(master);
    }

    @Override
        public Boolean detect(List<String> data, String value) {

            Function<String, String[]> parseString =
            str -> {
                str = str.toLowerCase();
                //rewrite to external function
                str = str.replaceAll("[[^a-z]&&[^а-я]&&\\S]", "");
                return str.split("\\s");
            };

        JavaRDD<String> trainData = sc.parallelize(data);
        JavaRDD<String[]> trainList = trainData.map(parseString);

        JavaRDD<String> dictionaryRdd = trainList.flatMap(
                words -> {
                    List<String> result = new ArrayList<>();
                    for(int i = 1; i < words.length; ++i) {
                        if(words[i].length() >= MINWORDLEN) {
                            result.add(words[i]);
                        }
                    }
                    return result;
                });
        JavaRDD<String> allclassesRdd = trainList.map(
                words -> words.length > 0 ? words[0] : null);

        final String[] dictionary = dictionaryRdd.distinct().collect().toArray(new String[0]);
        final String[] classes = allclassesRdd.distinct().collect().toArray(new String[0]);
        sort(dictionary);
        sort(classes);

        Function<String[], LabeledPoint> listToLP =
            words -> {
                double[] vec = new double[dictionary.length];
                double inc = 1.0 / dictionary.length;
                Double numClass = (double)binarySearch(classes, words[0]);
                int index;
                for (int i = 1; i < words.length; ++i) {
                    if((index = binarySearch(dictionary, words[i])) >= 0)
                        vec[index] += inc;
                }
                return new LabeledPoint(numClass, Vectors.dense(vec));
            };

        JavaRDD<LabeledPoint> training = trainList.map(listToLP);

        NaiveBayesModel model = NaiveBayes.train(training.rdd(), LAMBDA);

        LabeledPoint valueLB = null;
        try {
            valueLB = listToLP.call(parseString.call(value));
        } catch (Exception e){
            new Exception(e);
        }

        return model.predict(valueLB.features()) == valueLB.label();
    }
}
