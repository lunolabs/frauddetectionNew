package ru.spbstu.frauddetection.detection;

import java.io.Closeable;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.SparkConf;

public abstract class DetectionBaseSpark<T> extends DetectionBase<T> implements Closeable {
    protected JavaSparkContext sc;

    public DetectionBaseSpark(JavaSparkContext sc) {
        this.sc = sc;
    }

    public DetectionBaseSpark(){
        SparkConf conf = new SparkConf()
                .setAppName("Detection").setMaster("local[*]");
        sc = new JavaSparkContext(conf);
    }

    public DetectionBaseSpark(String master){
        SparkConf conf = new SparkConf()
                .setAppName("Detection").setMaster(master);
        sc = new JavaSparkContext(conf);
    }

    public JavaSparkContext getContext() {
        return sc;
    }

    public void close() {
        sc.close();
    }
}
