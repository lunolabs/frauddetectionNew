package ru.spbstu.frauddetection.core.fraud_detector;

import org.apache.spark.api.java.JavaSparkContext;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.detection.*;

import java.io.Serializable;

public class DetectorMethodsFactory implements Serializable {
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
        }

        return null;
    }
}
