package ru.spbstu.frauddetection.sparkmanager;

import akka.japi.Function2;
import akka.japi.Procedure;
import ru.spbstu.frauddetection.FraudConfig.ObjectModel.Method;
import ru.spbstu.frauddetection.detection.DetectionBase;
import ru.spbstu.frauddetection.detection.SentenceDetection;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by lvs on 06.11.16.
 */
public class Methods {
    /*
    private static Map<Method, Function2<Boolean>> methods = new HashMap<>();
    static {
        methods.put(Method.Sentence, () -> new SentenceDetection());
    }
    public static DetectionBase factory(Method method) {
        return methods.get(method);
    }
    */
}
