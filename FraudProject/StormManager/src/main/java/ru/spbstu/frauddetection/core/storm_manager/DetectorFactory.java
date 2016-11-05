package ru.spbstu.frauddetection.core.storm_manager;

import org.apache.spark.api.java.JavaSparkContext;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.detection.*;

import java.io.Serializable;

public class DetectorFactory implements Serializable {
    public static DetectionBaseSpark createDetector(Method method, JavaSparkContext context) {
        switch (method) {
            case Quantile:
                return new QuantilleDetection<>(context);

            case KMeans:
                return new KMeansDetection(context);

            case Bayes:
                return new NaiveBayesDetection(context);

            case Sentence:
                return new SentenceDetection(context);
            case KMeansDateTime:
                return new KMeansDateTimeDetection(context);
        }

        return null;
    }
}
