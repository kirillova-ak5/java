package ru.spbstu.akirillova.workers;

import ru.spbstu.pipeline.IExecutable;
import ru.spbstu.pipeline.IExecutor;
import ru.spbstu.pipeline.RC;
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
    IExecutable producer;
    IExecutable consumer;

    private final Logger logger;

    public Substitutor(Logger logger) {
        this.logger = logger;
    }

    @Override
    public RC setConsumer(IExecutable newConsumer) {
        if (newConsumer == null) {
            logger.warning("Invalid consumer passed to substitutor");
            return RC.CODE_INVALID_ARGUMENT;
        }
        consumer = newConsumer;
        return RC.CODE_SUCCESS;
    }

    @Override
    public RC setProducer(IExecutable newProducer) {
        if (newProducer == null) {
            logger.warning("Invalid producer passed to substitutor");
            return RC.CODE_INVALID_ARGUMENT;
        }
        producer = newProducer;
        return RC.CODE_SUCCESS;
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

    @Override
    public RC execute(byte[] data) {

        if (data == null) {
            logger.severe("Invalid substitutor input");
            return RC.CODE_INVALID_ARGUMENT;
        }

        RC retCode = table.Substitute(data);
        if (retCode != RC.CODE_SUCCESS) {
            logger.severe("Substitution error");
            return retCode;
        }

        retCode = consumer.execute(data);

        if(retCode != RC.CODE_SUCCESS) {
            logger.severe("error while executing substitutor consumer");
        }

        return retCode;
    }
}