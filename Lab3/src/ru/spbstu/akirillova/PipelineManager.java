package ru.spbstu.akirillova;

import ru.spbstu.pipeline.*;
import ru.spbstu.akirillova.config.SemanticsBase;
import ru.spbstu.akirillova.config.Config;
import ru.spbstu.akirillova.utils.PipelineBaseGrammar;

import java.io.*;
import java.lang.reflect.InvocationTargetException;
import java.util.logging.Logger;

class ManagerGrammar extends PipelineBaseGrammar {

    private static final String[] tokens;

    static {
        ManagerSemanticsBase.Fields[] fValues = ManagerSemanticsBase.Fields.values();

        tokens = new String[fValues.length];

        for (int i = 0; i < fValues.length; ++i) {
            tokens[i] = fValues[i].toString();
        }
    }

    public ManagerGrammar() {
        super(tokens);
    }
}

class ManagerSemanticsBase extends SemanticsBase {
    private static final String workersDelimiter = ";";
    private static final String workersInnerDelimiter = ",";

    public ManagerSemanticsBase(Logger logger) {
        super(logger);
    }

    public static String workersDelimiter() {
        return workersDelimiter;
    }

    public static String workersInnerDelimiter() {
        return workersInnerDelimiter;
    }

    @Override
    public boolean ValidateField(String fieldName, String fieldValue) {
        assert fieldName != null && fieldValue != null;

        if (fieldName.equals(Fields.INPUT_FILE.toString())) {
            if (IsFile(fieldValue)) {
                return true;
            }
            GetLogger().warning("Invalid input file");
            return false;
        }
        else if (fieldName.equals(Fields.OUTPUT_FILE.toString())) {
            return true;
        }
        else if (fieldName.equals(Fields.PIPELINE_STRUCTURE.toString())) {
            return validatePipelineStructure(fieldValue);
        }
        else {
            GetLogger().warning("Unknown field " + fieldName);
        }
        return true;
    }

    private boolean validatePipelineStructure(String pStruct) {
        String[] workerStrings = pStruct.split(workersDelimiter());

        for (int workerId = 0; workerId < workerStrings.length; ++workerId) {
            String[] workerParams = workerStrings[workerId].split(ManagerSemanticsBase.workersInnerDelimiter());

            if (workerParams.length != 2) {
                GetLogger().warning("Invalid element");
                return false;
            }

            for (int i = 0; i < workerParams.length; ++i) {
                workerParams[i] = workerParams[i].trim();
            }

            if (!validatePipelineStepClass(workerParams[0]) || !IsFile(workerParams[1])) {
                GetLogger().warning("Invalid element");
                return false;
            }

            if (workerId == 0 && !validateIReader(workerParams[0]) || workerId == workerStrings.length - 1 && !validateIWriter(workerParams[0])) {
                GetLogger().warning("Invalid element");
                return false;
            }
        }
        return true;
    }

    private boolean validateIReader(String className) {
        try {
            Class<?> clazz = Class.forName(className);

            if (!IReader.class.isAssignableFrom(clazz)) {
                return false;
            }
        } catch (ClassNotFoundException e) {
            return false;
        }
        return true;
    }

    private boolean validateIWriter(String className) {
        try {
            Class<?> clazz = Class.forName(className);

            if (!IWriter.class.isAssignableFrom(clazz)) {
                return false;
            }
        } catch (ClassNotFoundException e) {
            return false;
        }
        return true;
    }

    private boolean validatePipelineStepClass(String className) {
        try {
            Class<?> clazz = Class.forName(className);
            Class<?>[] params = {Logger.class};
            clazz.getConstructor(params);

            if (!(IPipelineStep.class.isAssignableFrom(clazz)) ||
                    !(IConfigurable.class.isAssignableFrom(clazz))) {
                GetLogger().warning("Invalid element");
                return false;
            }
        } catch (ClassNotFoundException | NoSuchMethodException e) {
            GetLogger().warning("Invalid element");
            return false;
        }
        return true;
    }

    public enum Fields {
        INPUT_FILE("input_file"),
        OUTPUT_FILE("output_file"),
        PIPELINE_STRUCTURE("pipeline");

        private final String name;

        Fields(String name) {
            this.name = name;
        }

        public String toString() {
            return this.name;
        }
    }
}

public class PipelineManager implements IConfigurable {

    private String inputFileName;
    private String outputFileName;

    class ConfigMapping {
        String worker;
        String configFile;
        public ConfigMapping(String wrk, String cfg) {
            worker = wrk;
            configFile = cfg;
        }
    }

    private ConfigMapping[] workerConfigMapping;

    private final Logger logger;

    private PipelineManager(Logger logger) {
        this.logger = logger;
    }

    public static PipelineManager createManager(String configFileName, Logger logger) {
        PipelineManager instance = new PipelineManager(logger);
        RC retCode = instance.setConfig(configFileName);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Cant configure manager");
            return null;
        }
        return instance;
    }

    public RC setConfig(String configFileName) {
        Config cfg = new Config();
        RC rc = Config.CreateConfig(configFileName, new ManagerGrammar(), new ManagerSemanticsBase(logger), logger, cfg);
        if (rc != RC.CODE_SUCCESS) {
            logger.severe("Cant create manager config");
            return rc;
        }

        setFieldsFromConfig(cfg);

        return RC.CODE_SUCCESS;
    }

    public RC run() {
        FileInputStream inputStream = null;
        FileOutputStream outputStream = null;
        try {
            inputStream = new FileInputStream(inputFileName);
        }
        catch (FileNotFoundException e) {
            logger.severe("Cant open input file");
            return RC.CODE_INVALID_INPUT_STREAM;
        }
        try {
            outputStream = new FileOutputStream(outputFileName);
        }
        catch (FileNotFoundException e) {
            closeStream(inputStream);
            logger.severe("Cant open output file");
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }

        IPipelineStep[] workers = new IPipelineStep[workerConfigMapping.length];

        RC rc = createWorkers(inputStream, outputStream, workers);
        if (rc != RC.CODE_SUCCESS) {
            closeStream(inputStream);
            closeStream(outputStream);
            logger.severe("Cant create workers");
            return rc;
        }

        rc = putWorkersInChain(workers);
        if (rc != RC.CODE_SUCCESS) {
            logger.severe("Cant construct pipeline");
            return rc;
        }
        rc = ((IConsumer)workers[0]).execute();

        closeStream(inputStream);
        closeStream(outputStream);

        if (rc != RC.CODE_SUCCESS) {
            logger.severe("Unable to execute pipeline");
        }

        return rc;
    }

    private void setFieldsFromConfig(Config cfg) {
        assert cfg != null;

        String inputFileName = cfg.GetParameter(ManagerSemanticsBase.Fields.INPUT_FILE.toString());
        String outputFileName = cfg.GetParameter(ManagerSemanticsBase.Fields.OUTPUT_FILE.toString());
        String pStruct = cfg.GetParameter(ManagerSemanticsBase.Fields.PIPELINE_STRUCTURE.toString());

        assert (inputFileName != null && outputFileName != null && pStruct != null);

        this.workerConfigMapping = getWorkerTemplates(pStruct);
        this.inputFileName = inputFileName;
        this.outputFileName = outputFileName;
    }

    private ConfigMapping[] getWorkerTemplates(String pStruct) {
        assert pStruct != null;

        String[] workerStrings = pStruct.split(ManagerSemanticsBase.workersDelimiter());

        ConfigMapping[] mapping = new ConfigMapping[workerStrings.length];

        for (int k = 0; k < workerStrings.length; ++k) {
            String[] workerParams = workerStrings[k].split(ManagerSemanticsBase.workersInnerDelimiter());

            assert workerParams.length == 2;

            for (int i = 0; i < workerParams.length; ++i) {
                workerParams[i] = workerParams[i].trim();
            }

            mapping[k] = new ConfigMapping(workerParams[0], workerParams[1]);
        }

        return mapping;
    }

    private void closeStream(Closeable c) {
        try {
            c.close();
        }
        catch (IOException e) {

        }
    }

    private RC createWorkers(FileInputStream inputStream, FileOutputStream outputStream, IPipelineStep[] workers) {
        assert inputStream != null;
        assert outputStream != null;

        for (int workerId = 0; workerId < workerConfigMapping.length; ++workerId) {
            IPipelineStep worker = createWorker(workerConfigMapping[workerId].worker);

            RC rc = ((IConfigurable)worker).setConfig(workerConfigMapping[workerId].configFile);
            if (rc != RC.CODE_SUCCESS) {
                return rc;
            }

            workers[workerId] = worker;
        }

        ((IReader)workers[0]).setInputStream(inputStream);
        ((IWriter)workers[workers.length - 1]).setOutputStream(outputStream);

        return RC.CODE_SUCCESS;
    }

    private IPipelineStep createWorker(String className) {
        assert className != null;

        IPipelineStep step;
        try {
            Class<?> clazz = Class.forName(className);
            Class<?>[] params = {Logger.class};

            assert IPipelineStep.class.isAssignableFrom(clazz);

            step = (IPipelineStep) clazz.getConstructor(params).newInstance(logger);
        } catch (ClassNotFoundException | NoSuchMethodException |
                InvocationTargetException | IllegalAccessException |
                InstantiationException e) {
            logger.warning("Cant create " + className + " object");
            return null;
        }
        return step;
    }

    private RC putWorkersInChain(IPipelineStep[] workers) {
        if (workers == null)
            return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;

        for(int i = 0; i < workers.length - 1; i++) {
            RC rc = workers[i].setConsumer((IConsumer) workers[i + 1]);
            if (rc != RC.CODE_SUCCESS)
                return  rc;
            rc = workers[i + 1].setProducer((IProducer) workers[i]);
            if (rc != RC.CODE_SUCCESS)
                return rc;
        }
        return  RC.CODE_SUCCESS;
    }
}