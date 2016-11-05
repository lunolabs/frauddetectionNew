package ru.spbstu.frauddetection.core.flink_manager;

public class ProcessStarter {
    public static void main(String[] args) throws Exception {
        FlinkManager flinkManager = new FlinkManager();
        flinkManager.run();
    }
}
