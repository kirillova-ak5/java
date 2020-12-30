package ru.spbstu.akirillova.workers;

import ru.spbstu.akirillova.utils.Data;
import ru.spbstu.pipeline.*;
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

    private IConsumer consumer;
    private IProducer producer;

    private final Logger logger;

    private int bufferSize;

    private Data outData;

    private final TYPE[] outTypes = {TYPE.BYTE, TYPE.CHAR, TYPE.SHORT};

    private boolean isEnd;

    class ByteMediator implements IMediator {
        @Override
        public Object getData() {
            byte[] data = outData.ExtractBytes();
            if (data.length == 0 && isEnd) {
                return null;
            }
            return data;
        }
    }

    class ShortMediator implements IMediator {
        @Override
        public Object getData() {
            short[] data = outData.ExtractShorts();
            if (data.length == 0 && isEnd) {
                return null;
            }
            return data;
        }
    }

    class CharMediator implements IMediator {
        @Override
        public Object getData() {
            char[] data = outData.ExtractChars();
            if (data.length == 0 && isEnd) {
                return null;
            }
            return data;
        }
    }

    public FileReader(Logger logger) {
        this.logger = logger;
        this.isEnd = false;
        this.outData = new Data();
    }

    @Override
    public TYPE[] getOutputTypes() {
        return outTypes;
    }

    @Override
    public IMediator getMediator(TYPE type) {
        switch (type) {
            case BYTE:
                return new ByteMediator();
            case SHORT:
                return new ShortMediator();
            case CHAR:
                return new CharMediator();
            default:
                logger.warning("No such mediator");
                return null;
        }
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
    public RC setConsumer(IConsumer newConsumer) {
        if (newConsumer == null) {
            logger.warning("Invalid consumer");
            return RC.CODE_INVALID_ARGUMENT;
        }
        consumer = newConsumer;

        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IProducer newProducer) {
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
        if (bytesRead < 0)
            return 0;
        return bytesRead;
    }

    @Override
    public RC execute() {
        if (stream == null) {
            logger.severe("Invalid input stream");
            return RC.CODE_INVALID_INPUT_STREAM;
        }

        byte[] buffer = new byte[bufferSize];


        while(!isEnd) {
            int bytesRead = readBytePortion(buffer, bufferSize);
            if (bytesRead < 0) {
                logger.warning("Cant read");
                return RC.CODE_FAILED_TO_READ;
            }

            if (bytesRead == 0)
                isEnd = true;
            else
                outData.PushBack(buffer, bytesRead);

            while(!outData.IsEmpty()) {
                RC rc = consumer.execute();
                if (rc != RC.CODE_SUCCESS) {
                    logger.severe("error while executing reader consumer");
                    return rc;
                }
            }
        }

        return RC.CODE_SUCCESS;
    }
}

