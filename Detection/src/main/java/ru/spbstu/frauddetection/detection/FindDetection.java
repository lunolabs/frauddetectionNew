package ru.spbstu.frauddetection.detection;

import java.util.List;

/**
 * Created by lvs on 18.10.16.
 */
public class FindDetection<T extends Comparable> extends DetectionBase<T> {
    @Override
    public Boolean detect(List<T> data, T value) {
        for(T val : data) {
            if(val.equals(value)) {
                return true;
            }
        }
        return false;
    }
}
