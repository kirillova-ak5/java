package ru.spbstu.akirillova.config;

import ru.spbstu.pipeline.RC;
import ru.spbstu.akirillova.utils.FileParser;
import ru.spbstu.akirillova.utils.PipelineBaseGrammar;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Config {

    private HashMap<String, String> fields;

    private Logger logger;

    private void SetData(HashMap<String, String> fields, Logger logger)
    {
        this.fields = fields;
        this.logger = logger;
    }

    public String GetParameter(String key) {
        String value;
        try {
            value = fields.get(key);
        }
        catch (ClassCastException | NullPointerException ex) {
            logger.warning("Cant parse config parameter");
            value = null;
        }
        return value;
    }

    public Integer GetIntParameter(String key) {
        String stringParameter = GetParameter(key);
        if (stringParameter == null) {
            logger.warning("Cant parse config parameter");
            return null;
        }
        Integer value;
        try {
            value = Integer.parseInt(stringParameter);
        }
        catch (NumberFormatException ex) {
            value = null;
            logger.warning("Cant parse config parameter");
        }
        return value;
    }

    public static RC CreateConfig(String filename, PipelineBaseGrammar grammar,
                                  SemanticsBase semantics, Logger logger, Config cfg) {

        HashMap<String, String> map = new HashMap<String, String>();
        RC rc = FileParser.ReadMap(filename, grammar, logger, map);

        if (rc != RC.CODE_SUCCESS) {
            logger.severe("Coudnt to read a map from file");
            return rc;
        }

        if (!ValidateSemantics(map, semantics, logger)) {
            logger.warning("Semantic isnt valid");
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }

        cfg.SetData(map, logger);

        return RC.CODE_SUCCESS;
    }

    private static boolean ValidateSemantics(HashMap<String, String> cfgMap, SemanticsBase semantics, Logger logger) {
        for (Map.Entry<String, String> entry : cfgMap.entrySet()) {
            if (!semantics.ValidateField(entry.getKey(), entry.getValue())) {
                logger.warning("Invalid config semantics");
                return false;
            }
        }
        return true;
    }
}