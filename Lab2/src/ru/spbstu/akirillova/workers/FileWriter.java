package ru.spbstu.akirillova.workers;

import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IWriter;
import ru.spbstu.pipeline.RC;
import ru.spbstu.akirillova.config.SemanticsBase;
import ru.spbstu.akirillova.config.Config;
import ru.spbstu.akirillova.utils.PipelineBaseGrammar;

import java.io.FileOutputStream;
import java.io.IOException;
import java.util.logging.Logger;

class WriterGrammar extends PipelineBaseGrammar {

    private static final String[] tokens;

    static {
        WriterSemantics.Fields[] fValues = WriterSemantics.Fields.values();

        tokens = new String[fValues.length];

        for (int i = 0; i < fValues.length; ++i) {
            tokens[i] = fValues[i].toString();
        }
    }

    public WriterGrammar() {
        super(tokens);
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

class WriterSemantics extends SemanticsBase {

    public WriterSemantics(Logger logger) {
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

public class FileWriter implements IWriter {
    private FileOutputStream stream;

    private IExecutable producer;
    private IExecutable consumer;

    private final Logger logger;

    private int bufferSize;

    public FileWriter(Logger logger) {
        this.logger = logger;
    }

    @Override
    public RC setOutputStream(FileOutputStream fileOutputStream) {
        if (fileOutputStream == null) {
            logger.warning("Invalid output stream");
            return RC.CODE_INVALID_ARGUMENT;
        }
        stream = fileOutputStream;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConsumer(IExecutable newConsumer) {
        consumer = newConsumer;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IExecutable newProducer) {
        if (newProducer == null) {
            logger.warning("Invalid producer");
            return RC.CODE_INVALID_ARGUMENT;
        }
        producer = newProducer;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setConfig(String configFileName) {
        Config cfg = new Config();
        RC rc = Config.CreateConfig(configFileName, new WriterGrammar(), new WriterSemantics(logger), logger, cfg);
        if (rc != RC.CODE_SUCCESS) {
            logger.severe("Cant create writer config");
            return rc;
        }

        Integer bufferSize = cfg.GetIntParameter(WriterGrammar.Fields.BUFFER_SIZE.toString());
        assert bufferSize != null;

        this.bufferSize = bufferSize;

        return RC.CODE_SUCCESS;
    }

    @Override
    public RC execute(byte[] data) {
        if (data == null) {
            logger.severe("Invalid writer input");
            return RC.CODE_INVALID_ARGUMENT;
        }

        if (stream == null) {
            logger.severe("Invalid output stream");
            return RC.CODE_INVALID_OUTPUT_STREAM;
        }

        try {
            for(int i = 0; i < data.length - bufferSize; i+=bufferSize) {
                stream.write(data, i, bufferSize);
            }
            int lastChunkSize = data.length % bufferSize == 0 ? bufferSize : data.length % bufferSize;
            stream.write(data, data.length - lastChunkSize, lastChunkSize);
        }
        catch (IOException ex) {
            logger.severe("IOexception while writing");
            return RC.CODE_FAILED_TO_WRITE;
        }
        return RC.CODE_SUCCESS;
    }
}