package ru.spbstu.akirillova.workers;

import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IReader;
import ru.spbstu.pipeline.RC;
import ru.spbstu.akirillova.config.SemanticsBase;
import ru.spbstu.akirillova.config.Config;
import ru.spbstu.akirillova.utils.PipelineBaseGrammar;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.logging.Logger;

class ReaderGrammar extends PipelineBaseGrammar {

    private static final String[] tokens;

    static {
        ReaderSemantics.Fields[] fields = ReaderSemantics.Fields.values();

        tokens = new String[fields.length];

        for (int i = 0; i < fields.length; ++i) {
            tokens[i] = fields[i].toString();
        }
    }

    public ReaderGrammar() {
        super(tokens);
    }
}

class ReaderSemantics extends SemanticsBase {

    public ReaderSemantics(Logger logger) {
        super(logger);
    }

    @Override
    public boolean ValidateField(String fieldName, String fieldValue) {
        if (fieldName.equals(Fields.BUFFER_SIZE.toString())) {
            return SemanticsBase.IsPositiveInt(fieldValue);
        }
        else {
            GetLogger().warning("Unknown field " + fieldName);
        }
        return true;
    }

    public enum Fields {
        BUFFER_SIZE("buffer_size");

        private final String name;

        Fields(String name) {
            this.name = name;
        }

        public String toString() {
            return this.name;
        }
    }
}


public class FileReader implements IReader {

    private FileInputStream stream;

    private IExecutable producer;
    private IExecutable consumer;

    private final Logger logger;

    private int bufferSize;

    public FileReader(Logger logger) {
        this.logger = logger;
    }

    @Override
    public RC setInputStream(FileInputStream fileInputStream) {
        if (fileInputStream == null) {
            logger.warning("Invalid input stream");
            return RC.CODE_INVALID_ARGUMENT;
        }
        stream = fileInputStream;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConsumer(IExecutable newConsumer) {
        if (newConsumer == null) {
            logger.warning("Invalid consumer");
            return RC.CODE_INVALID_ARGUMENT;
        }
        consumer = newConsumer;

        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IExecutable newProducer) {
        producer = newProducer;

        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConfig(String configFileName) {
        Config cfg = new Config();
        RC rc = Config.CreateConfig(configFileName, new ReaderGrammar(), new ReaderSemantics(logger), logger, cfg);
        if (rc != RC.CODE_SUCCESS) {
            logger.severe("Cant create reader config");
            return rc;
        }

        Integer bufferSize = cfg.GetIntParameter(ReaderSemantics.Fields.BUFFER_SIZE.toString());
        assert bufferSize != null;

        this.bufferSize = bufferSize;

        return RC.CODE_SUCCESS;
    }

    private int readBytePortion(byte[] buffer, int bufferSize) {
        int bytesRead;
        try {
            bytesRead = stream.read(buffer, 0, bufferSize);
        } catch (IOException ex) {
            logger.severe("IOexception while reading");
            return -1;
        }
        return bytesRead;
    }

    @Override
    public RC execute(byte[] data) {
        if (stream == null) {
            logger.severe("Invalid input stream");
            return RC.CODE_INVALID_INPUT_STREAM;
        }

        byte[] buffer = new byte[bufferSize];

        int bytesRead;

        while(true) {
            bytesRead = readBytePortion(buffer, bufferSize);
            if (bytesRead < 1) {
                break;
            }

            byte[] output = new byte[bytesRead];
            System.arraycopy(buffer, 0, output, 0, bytesRead);

            RC retCode = consumer.execute(output);
            if (retCode != RC.CODE_SUCCESS) {
                logger.severe("error while executing reader consumer");
                return retCode;
            }
        }

        return RC.CODE_SUCCESS;
    }
}

