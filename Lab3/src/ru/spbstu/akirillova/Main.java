package ru.spbstu.akirillova;

import ru.spbstu.pipeline.RC;

import java.util.logging.Logger;

public class Main {

    public static void main(String[] Args)  {
        Logger logger = Logger.getLogger("Logger");

        if (Args == null || Args.length != 1) {
            logger.severe("Expected one command argument");
            return;
        }
        String configFileName = Args[0];

        PipelineManager manager = PipelineManager.createManager(configFileName, logger);
        if (manager == null) {
            logger.severe("Cant create manager");
            return;
        }

        RC retCode = manager.run();
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Execution failed");
            return;
        }

        logger.info("DONE");
    }
}
