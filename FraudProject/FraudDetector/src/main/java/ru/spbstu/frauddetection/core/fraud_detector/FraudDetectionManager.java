//package ru.spbstu.frauddetection.core.fraud_detector;
//
//import MockDB.MockData;
//import org.apache.log4j.Logger;
//import org.apache.spark.api.java.JavaSparkContext;
//import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Configuration;
//import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Group;
//import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
//import ru.spbstu.frauddetection.InputDataCalculator.ValueGroup;
//import ru.spbstu.frauddetection.detection.DetectionBaseSpark;
//
//import java.io.Serializable;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//public class FraudDetectionManager implements Serializable {
//    private final static Logger logger = Logger.getLogger(FraudDetectionManager.class);
//
//    private JavaSparkContext sparkContext;
//    private String lastFraudMessage;
//
//    private Configuration xmlConfig;
//
//    public FraudDetectionManager(JavaSparkContext sparkContext, Configuration xmlConfig) {
//        this.sparkContext = sparkContext;
//        this.xmlConfig = xmlConfig;
//    }
//
//    public String getLastFraudMessage() {
//        return lastFraudMessage;
//    }
//
//    public boolean fraudDetected(Map<Method, ValueGroup> fraudDetectableMap, List<ValueGroup> dbList) {
//        if (sparkContext == null) {
//            logger.debug("in fraudDetected sparkContext = null");
//        }
//
//        for (Method key : fraudDetectableMap.keySet()) {
//            if (detected(key, fraudDetectableMap.get(key), dbList)) {
//                logger.info("With method " + key + " = " + fraudDetectableMap.get(key) + " fraud detected");
//
//                return true;
//            }
//
//            logger.info("With method " + key + " = " + fraudDetectableMap.get(key) + " fraud not detected");
//        }
//
//        return false;
//    }
//
//    private boolean detected(Method method, ValueGroup fraudDetectable, List<ValueGroup> dbList) {
//        if (sparkContext == null) {
//            logger.debug("in detected sparkContext = null");
//        }
//
//        switch (method) {
//            case Quantile:
//                if (detectedWithQuantille(getGroup(method), fraudDetectable, dbList)) {
//                    return true;
//                }
//
//                break;
//
//            case KMeans:
//                if (detectedWithKMeans(getGroup(method), fraudDetectable, dbList)) {
//                    return true;
//                }
//
//                break;
//
//            case Sentence:
//                if (detectedWithSentenceDetection(getGroup(method), fraudDetectable, dbList)) {
//                    return true;
//                }
//
//                break;
//        }
//
//        return false;
//    }
//
//    private boolean detectedWithQuantille(Group configGroup, ValueGroup fraudDetectable, List<ValueGroup> dbList) {
//        DetectionBaseSpark quantilleDetector = createDetector(Method.Quantile);
//
//        final List<List<Number>> convertedDBList = quantilleDetector.convertToTypeList(
//                new MockData().getNormalList(fraudDetectable, dbList), configGroup);
//        final List<Number> convertedValue = (List<Number>) quantilleDetector.convertToType(fraudDetectable, configGroup);
//
//        for (int i = 0; i < convertedValue.size(); ++i) {
//            List<Number> db = new ArrayList<>();
//
//            for (List<Number> el : convertedDBList) {
//                db.add(el.get(i));
//            }
//
//            if (!quantilleDetector.detect(db, convertedValue.get(i))) {
//                logger.info("Quantile detection: for value + " + fraudDetectable + " fraud detected");
//                putLastFraudMessage(Method.Quantile, fraudDetectable);
//
//                return true;
//            }
//        }
//
//        logger.info("Quantile detection: for value + " + fraudDetectable + " fraud not detected");
//
//        return false;
//    }
//
//    private boolean detectedWithSentenceDetection(Group configGroup,
//                                                  ValueGroup fraudDetectable, List<ValueGroup> dbList) {
//        DetectionBaseSpark sentenceDetector = createDetector(Method.Sentence);
//        if (sparkContext == null) {
//            logger.debug("in sentence sparkContext = null");
//        }
//
//        final List<String> convertedDBList = sentenceDetector.convertToTypeList(
//                new MockData().getNormalList(fraudDetectable, dbList), configGroup);
//        final List<String> convertedValue = (List<String>) sentenceDetector.convertToType(fraudDetectable, configGroup);
//
//        for (String el : convertedValue) {
//            if (!sentenceDetector.detect(convertedDBList, el)) {
//                logger.info("Sentence detection: for value + " + fraudDetectable + " fraud detected");
//                putLastFraudMessage(Method.Sentence, fraudDetectable);
//
//                return true;
//            }
//        }
//
//        logger.info("Sentence detection: for value + " + fraudDetectable + " fraud not detected");
//
//        return false;
//    }
//
//    private boolean detectedWithKMeans(Group configGroup, ValueGroup fraudDetectable, List<ValueGroup> dbList) {
//        DetectionBaseSpark kMeansDetector = createDetector(Method.KMeans);
//        if (sparkContext == null) {
//            logger.debug("in kmeans sparkContext = null");
//        }
//
//        final List<String> convertedDBList = kMeansDetector.convertToTypeList(
//                new MockData().getNormalList(fraudDetectable, dbList), configGroup);
//        final String convertedValue = (String) kMeansDetector.convertToType(fraudDetectable, configGroup);
//
//        if (!kMeansDetector.detect(convertedDBList, convertedValue)) {
//            logger.info("KMeans detection: for value " + fraudDetectable + " fraud detected");
//            putLastFraudMessage(Method.KMeans, fraudDetectable);
//
//            return true;
//        }
//
//        logger.info("KMeans detection: for value " + fraudDetectable + " fraud not detected");
//
//        return false;
//    }
//
//    private DetectionBaseSpark createDetector(Method method) {
//        DetectionBaseSpark detector;
//
//        detector = DetectorMethodsFactory.createDetector(method, sparkContext);
//        return detector;
//    }
//
//    private void putLastFraudMessage(Method detectionMethod, ValueGroup fraudDetectable) {
//        lastFraudMessage = "With " + detectionMethod.name() + " method for\n " + fraudDetectable + "\nfraud detected";
//    }
//
//    private Group getGroup(Method method) {
//        for (Group group : xmlConfig.getGroups()) {
//            if (group.getMethod() == method) {
//                return group;
//            }
//        }
//
//        return null;
//    }
//}
