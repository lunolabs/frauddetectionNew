package ru.spbstu.frauddetection.detection;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Field;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Group;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Type;
import ru.spbstu.frauddetection.InputDataCalculator.InputGroup;
import ru.spbstu.frauddetection.InputDataCalculator.InputType;
import scala.Tuple2;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;

public class NaiveBayesDetection extends DetectionBaseSpark<List<Double> > {

    private static final double LAMBDA = 1.00;
    private static final double VERACITY = 0.997;

    public NaiveBayesDetection() {
        super();
    }

    public NaiveBayesDetection(String str) {
        super(str);
    }

    public NaiveBayesDetection(JavaSparkContext sc) {
        super(sc);
    }

    @Override
    public Boolean detect(List<List<Double> > data, List<Double> value) {
        JavaRDD<List<Double> > trainDouble = sc.parallelize(data);
        JavaRDD<LabeledPoint> training = trainDouble.map(
            ld -> {
                double[] vec = new double[ld.size() - 1];
                for(int i = 1; i < ld.size(); ++ i) {
                    vec[i - 1] = ld.get(i);
                }
            return new LabeledPoint(ld.get(0).doubleValue(), Vectors.dense(vec));
            });

        final NaiveBayesModel model = NaiveBayes.train(training.rdd(), LAMBDA);

        JavaRDD<List<Double> > testDouble = sc.parallelize(Arrays.asList(value));
        JavaRDD<LabeledPoint> test = testDouble.map(
            ld -> {
                double[] vec = new double[ld.size() - 1];
                for(int i = 1; i < ld.size(); ++ i) {
                    vec[i - 1] = ld.get(i);
                }
            return new LabeledPoint(ld.get(0).doubleValue(), Vectors.dense(vec));
            });

        JavaPairRDD<Double, Double> predictionAndLabel = test.mapToPair(
            p -> (new Tuple2<Double, Double>(model.predict(p.features()), p.label())));
        // rewrite to java 8
        double accuracy = predictionAndLabel.filter(
            new Function<Tuple2<Double, Double>, Boolean>() {
                @Override
                public Boolean call(Tuple2<Double, Double> pl) {
                    return pl._1().equals(pl._2());
            }}).count() / (double) test.count();
        /*
        double accuracy = predictionAndLabel.filter(
            (Tuple2<Double, Double> pl) -> new Boolean(pl._1().equals(pl._2()))
            ).count() / (double)test.count();
        */

        return accuracy > VERACITY;
    }

    @Override
    public List<Double> convertToType(InputGroup valueGroup, Group configGroup) {
        List<Double> converted = new ArrayList<>();

        for (InputType value : valueGroup.getValues()) {
            String valueName = value.getFieldName();

            for (Field field : configGroup.getFields()) {
                if (valueName.equals(field.getXpathName()) &&
                        (field.getType() == Type.Integer || field.getType() == Type.Float)) {
                    converted.add((Double) value.getT());
                }
            }
        }

        System.out.println("Bayes converted: ");
        System.out.println(converted);

        return converted;
    }

    @Override
    public List<List<Double>> convertToTypeList(List<InputGroup> data, Group configGroup) {
        List<List<Double>> convertedList = new ArrayList<>();

        for (InputGroup group : data) {
            convertedList.add(convertToType(group, configGroup));
        }

        return convertedList;
    }
}
