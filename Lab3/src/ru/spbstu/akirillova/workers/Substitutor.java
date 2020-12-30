package ru.spbstu.akirillova.workers;

import ru.spbstu.akirillova.utils.Data;
import ru.spbstu.pipeline.*;
import ru.spbstu.akirillova.config.SemanticsBase;
import ru.spbstu.akirillova.config.Config;
import ru.spbstu.akirillova.utils.PipelineBaseGrammar;

import java.util.logging.Logger;

class SubstitutorGrammar extends PipelineBaseGrammar {

    private static final String[] tokens;

    static {
        SubstitutorSemantics.Fields[] fValues = SubstitutorSemantics.Fields.values();

        tokens = new String[fValues.length];

        for (int i = 0; i < fValues.length; ++i) {
            tokens[i] = fValues[i].toString();
        }
    }

    public SubstitutorGrammar() {
        super(tokens);
    }
}

class SubstitutorSemantics extends SemanticsBase {

    public SubstitutorSemantics(Logger logger) {
        super(logger);
    }

    @Override
    public boolean ValidateField(String fieldName, String fieldValue) {
        if (fieldName.equals(Fields.TABLE_FILE.toString())) {
            return SemanticsBase.IsFile(fieldValue);
        }
        else {
            GetLogger().warning("Unknown field");
        }
        return true;
    }

    public enum Fields {
        TABLE_FILE("table_file");

        private final String name;

        Fields(String name) {
            this.name = name;
        }

        public String toString() {
            return this.name;
        }
    }
}


public class Substitutor implements IExecutor {

    SubstitutionTable table;
    IProducer producer;
    IConsumer consumer;

    private final Logger logger;

    private Data outData;

    private IMediator mediator;
    private TYPE mediatorType;

    private boolean isEnd;

    final TYPE[] inTypes = {TYPE.BYTE, TYPE.CHAR, TYPE.SHORT};
    final TYPE[] outTypes = {TYPE.BYTE, TYPE.CHAR, TYPE.SHORT};

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
    public TYPE[] getOutputTypes() {
        return outTypes;
    }


    public Substitutor(Logger logger) {
        this.logger = logger;
        isEnd = false;
        outData = new Data();
    }

    @Override
    public RC setConsumer(IConsumer newConsumer) {
        if (newConsumer == null) {
            logger.warning("Invalid consumer passed to substitutor");
            return RC.CODE_INVALID_ARGUMENT;
        }
        consumer = newConsumer;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IProducer newProducer) {
        if (newProducer == null) {
            logger.warning("Invalid producer");
            return RC.CODE_INVALID_ARGUMENT;
        }
        TYPE[] prodTypes = newProducer.getOutputTypes();

        for (TYPE p : prodTypes) {
            for (TYPE i : inTypes) {
                if (i == p) {
                    mediator = newProducer.getMediator(p);
                    mediatorType = p;
                    return RC.CODE_SUCCESS;
                }
            }
        }

        logger.severe("Cant find common type");
        return RC.CODE_FAILED_PIPELINE_CONSTRUCTION;
    }

    @Override
    public RC setConfig(String configFileName) {
        Config cfg = new Config();
        RC rc = Config.CreateConfig(configFileName, new SubstitutorGrammar(), new SubstitutorSemantics(logger), logger, cfg);
        if (rc != RC.CODE_SUCCESS) {
            logger.severe("Failed to read substitutor config");
            return rc;
        }

        String tableFilename = cfg.GetParameter(SubstitutorSemantics.Fields.TABLE_FILE.toString());
        assert  tableFilename != null;

        SubstitutionTable subsTable = new SubstitutionTable();
        rc = SubstitutionTable.createSubstitutionTable(tableFilename, logger, subsTable);
        if (rc != RC.CODE_SUCCESS) {
            logger.severe("Cant create substitution table");
            return rc;
        }
        this.table = subsTable;
        return RC.CODE_SUCCESS;
    }

    private byte[] GetBytes(Object data) {
        if (data == null)
            return null;

        switch (mediatorType) {
            case BYTE:
            case CHAR:
                return (byte[]) data;
            case SHORT:
                short[] shorts = (short[]) data;
                byte[] bytes = new byte[shorts.length * 2];
                for (int i = 0; i < shorts.length * 2; i++) {
                    bytes[i] = (byte) (shorts[i / 2] >> 8);
                    bytes[i + 1] = (byte) (shorts[i / 2] & 0xFF);
                }
                return bytes;
            default:
                logger.warning("Cannot convert");
                return null;
        }
    }


    @Override
    public RC execute() {
        Object obj = mediator.getData();
        if (obj == null) {
            isEnd = true;
        }
        else {
            byte[] data = GetBytes(obj);
            if (data == null) {
                logger.severe("Invalid substitutor input");
                return RC.CODE_INVALID_ARGUMENT;
            }

            RC rc = table.Substitute(data);
            if (rc != RC.CODE_SUCCESS) {
                logger.severe("Substitution error");
                return rc;
            }
            outData.PushBack(data);
        }
        while(!outData.IsEmpty()) {
            RC rc = consumer.execute();
            if (rc != RC.CODE_SUCCESS) {
                logger.severe("error while executing substitutor consumer");
                return rc;
            }
        }

        return RC.CODE_SUCCESS;
    }
}