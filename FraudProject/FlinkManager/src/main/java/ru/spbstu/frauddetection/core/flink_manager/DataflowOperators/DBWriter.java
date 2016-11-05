package ru.spbstu.frauddetection.core.flink_manager.DataflowOperators;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.log4j.Logger;
import ru.spbstu.frauddetection.core.flink_manager.DataflowCustomTypes;

public class DBWriter implements MapFunction<DataflowCustomTypes.VerdictTuple, Void> {
    private final static Logger logger = Logger.getLogger(DBWriter.class);

    public Void map(DataflowCustomTypes.VerdictTuple input) throws Exception {
        logger.info("mapping " + input);
        logger.info("Wrote " + input.f0 + " to DB");

        return null;
    }
}
