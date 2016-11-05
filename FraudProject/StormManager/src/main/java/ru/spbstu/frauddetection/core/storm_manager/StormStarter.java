package ru.spbstu.frauddetection.core.storm_manager;

import org.apache.log4j.Logger;

public class StormStarter {
    private static final Logger logger = Logger.getLogger(StormStarter.class);

//    TODO: move to properties
    private static final String ARGS_ERROR_MESSAGE = "Run mode arg not found. Use \"local\" or \"production\"";
    private static final String ARG_LOCAL_STRING = "local";
    private static final String ARG_PRODUCTION_STRING = "production";

    public static void main(String[] args) throws Exception {
        if (args.length < 1) {
            logger.fatal(ARGS_ERROR_MESSAGE);
            return;
        }

        StormManager.StormRunModeEnum runMode = detectRunMode(args[0]);
        if (runMode == null) {
            logger.fatal(ARGS_ERROR_MESSAGE);
            return;
        }

        StormManager stormManager = new StormManager();
        stormManager.run(runMode);
    }

    private static StormManager.StormRunModeEnum detectRunMode(String strMode) {
        if (strMode.equals(ARG_LOCAL_STRING)) {
            return StormManager.StormRunModeEnum.LOCAL;
        }

        if (strMode.equals(ARG_PRODUCTION_STRING)) {
            return StormManager.StormRunModeEnum.PRODUCTION;
        }

        return null;
    }
}
