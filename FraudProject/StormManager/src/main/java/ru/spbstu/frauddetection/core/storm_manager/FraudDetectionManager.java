package ru.spbstu.frauddetection.core.storm_manager;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.*;
import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;
import ru.spbstu.frauddetection.detection.DetectionBase;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.joda.time.DateTime;
import ru.spbstu.frauddetection.detection.DetectionBaseSpark;

public class FraudDetectionManager implements Serializable {
    private final static Logger logger = Logger.getLogger(FraudDetectionManager.class);
    private static JavaSparkContext sparkContext;
    private String lastFraudMessage;

    private Configuration config;

    public FraudDetectionManager(Configuration config) {
        this.config = config;
    }

    public FraudDetectionManager(Configuration config, JavaSparkContext sparkContext) {
        this.config = config;
        FraudDetectionManager.sparkContext = sparkContext;
        lastFraudMessage = "";
    }

    public String getLastFraudMessage() {
        return lastFraudMessage;
    }

    public void close() {
        sparkContext.close();
    }

    public boolean fraudDetected(Map<Method, ValueGroup> fraudDetectableMap, List<ValueGroup> dbList) {
        for (Method key : fraudDetectableMap.keySet()) {
            if (detected(key, fraudDetectableMap.get(key), dbList)) {
                logger.info("With method " + key + " = " + fraudDetectableMap.get(key) + " fraud detected");

                return true;
            }

            logger.info("With method " + key + " = " + fraudDetectableMap.get(key) + " fraud not detected");
        }

        return false;
    }

    private boolean detected(Method method, ValueGroup fraudDetectable, List<ValueGroup> dbList) {
        switch (method) {
            case Quantile:
                if (detectedWithQuantille(getGroup(method), fraudDetectable, dbList)) {
                    return true;
                }

                break;

            case KMeans:
                if (detectedWithKMeans(getGroup(method), fraudDetectable, dbList)) {
                    return true;
                }

                break;

            case Sentence:
                if (detectedWithSentenceDetection(getGroup(method), fraudDetectable, dbList)) {
                    return true;
                }

                break;

//            case Bayes:
//                if (detectedWithNaiveBayes(getGroup(method), fraudDetectable, dbList)) {
//                    return true;
//                }
//
//                break;

            case KMeansDateTime:
                if (detectedWithKMeansDateTime(getGroup(method), fraudDetectable, dbList)) {
                    return true;
                }

                break;
        }

        return false;
    }

    private boolean detectedWithQuantille(Group configGroup, ValueGroup fraudDetectable, List<ValueGroup> dbList) {
        DetectionBaseSpark quantilleDetector = createDetector(Method.Quantile);

        final List<List<Number>> convertedDBList = quantilleDetector.convertToTypeList(
                new MockData().getNormalList(fraudDetectable, dbList), configGroup);
        final List<Number> convertedValue = (List<Number>) quantilleDetector.convertToType(fraudDetectable, configGroup);

        for (int i = 0; i < convertedValue.size(); ++i) {
            List<Number> db = new ArrayList<>();

            for (List<Number> el : convertedDBList) {
                db.add(el.get(i));
            }

            if (!quantilleDetector.detect(db, convertedValue.get(i))) {
                logger.info("Quantile detection: for value + " + fraudDetectable + " fraud detected");
                putLastFraudMessage(Method.Quantile, fraudDetectable);

                return true;
            }
        }

        logger.info("Quantile detection: for value + " + fraudDetectable + " fraud not detected");

        return false;
    }

    private boolean detectedWithSentenceDetection(Group configGroup,
                                                  ValueGroup fraudDetectable, List<ValueGroup> dbList) {
        DetectionBaseSpark sentenceDetector = createDetector(Method.Sentence);

        final List<String> convertedDBList = sentenceDetector.convertToTypeList(
                new MockData().getNormalList(fraudDetectable, dbList), configGroup);
        final List<String> convertedValue = (List<String>) sentenceDetector.convertToType(fraudDetectable, configGroup);

        for (String el : convertedValue) {
            if (!sentenceDetector.detect(convertedDBList, el)) {
                logger.info("Sentence detection: for value + " + fraudDetectable + " fraud detected");
                putLastFraudMessage(Method.Sentence, fraudDetectable);

                return true;
            }
        }

        logger.info("Sentence detection: for value + " + fraudDetectable + " fraud not detected");

        return false;
    }

    private boolean detectedWithKMeans(Group configGroup, ValueGroup fraudDetectable, List<ValueGroup> dbList) {
        DetectionBaseSpark kMeansDetector = createDetector(Method.KMeans);

        final List<String> convertedDBList = kMeansDetector.convertToTypeList(
                new MockData().getNormalList(fraudDetectable, dbList), configGroup);
        final String convertedValue = (String) kMeansDetector.convertToType(fraudDetectable, configGroup);

        if (!kMeansDetector.detect(convertedDBList, convertedValue)) {
            logger.info("KMeans detection: for value " + fraudDetectable + " fraud detected");
            putLastFraudMessage(Method.KMeans, fraudDetectable);

            return true;
        }

        logger.info("KMeans detection: for value " + fraudDetectable + " fraud not detected");

        return false;
    }
    
    private boolean detectedWithKMeansDateTime(Group configGroup, ValueGroup fraudDetectable, List<ValueGroup> dbList) {
        DetectionBaseSpark kMeansDateTimeDetector = createDetector(Method.KMeansDateTime);
        final List<List<DateTime>> convertedDBList = kMeansDateTimeDetector.convertToTypeList(
                new MockData().getNormalList(fraudDetectable, dbList), configGroup);
        final List<DateTime> convertedValue = (List<DateTime>) kMeansDateTimeDetector.convertToType(fraudDetectable, configGroup);

        for (int i = 0; i < convertedValue.size(); ++i) {
        	List<DateTime> db = new ArrayList<DateTime>();
        	
        	for (List<DateTime> el : convertedDBList) {
        		db.add(el.get(i));
        	}
        	
            if (!kMeansDateTimeDetector.detect(db, convertedValue.get(i))) {
                logger.info("KMeans detection of Date and Time: for value " + fraudDetectable + " fraud detected");
                putLastFraudMessage(Method.KMeansDateTime, fraudDetectable);
                return true;
            }
        }

        logger.info("KMeans detection of Date and Time: for value " + fraudDetectable + " fraud not detected");

        return false;
    }

    private boolean detectedWithNaiveBayes(Group configGroup, ValueGroup fraudDetectable, List<ValueGroup> dbList) {
        DetectionBaseSpark naiveBayesDetector = createDetector(Method.Bayes);

        final List<List<Double>> convertedDBList = naiveBayesDetector.convertToTypeList(
                new MockData().getNormalList(fraudDetectable, dbList), configGroup);
        final List<Double> convertedValue = (List<Double>) naiveBayesDetector.convertToType(fraudDetectable, configGroup);

        if (!naiveBayesDetector.detect(convertedDBList, convertedValue)) {
            System.out.println("NaiveBayesDetection: for value " + fraudDetectable + " fraud detected");
            putLastFraudMessage(Method.Bayes, fraudDetectable);

            return true;
        }

        System.out.println("NaiveBayesDetection: for value " + fraudDetectable + " fraud not detected");

        return false;
    }

    private DetectionBaseSpark createDetector(Method method) {
        DetectionBaseSpark detector;

        detector = DetectorFactory.createDetector(method, sparkContext);

        return detector;
    }

    private void putLastFraudMessage(Method detectionMethod, ValueGroup fraudDetectable) {
        lastFraudMessage = "With " + detectionMethod.name() + " method for\n " + fraudDetectable + "\nfraud detected";
    }

    private Group getGroup(Method method) {
        for (Group group : config.getGroups()) {
            if (group.getMethod() == method) {
                return group;
            }
        }

        return null;
    }

//    @Override
//    protected void finalize() throws Throwable {
//        sparkContext.close();
//    }
}
