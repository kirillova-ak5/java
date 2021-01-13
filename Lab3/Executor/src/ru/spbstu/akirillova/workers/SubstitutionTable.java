package ru.spbstu.akirillova.workers;

import ru.spbstu.pipeline.RC;
import ru.spbstu.akirillova.utils.FileParser;
import ru.spbstu.akirillova.utils.PipelineBaseGrammar;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class SubstitutionTable {

    private HashMap<Byte, Byte> table;

    private Logger logger;

    private void SetData(HashMap<Byte, Byte> table, Logger logger) {
        this.table = table;
        this.logger = logger;
    }

    public byte Substitute(byte x) {
        Byte y = table.get(x);
        if (y == null) {
            return x;
        }
        else {
            return y;
        }
    }

    public RC Substitute(byte[] data) {
        if (data == null) {
            logger.warning("Substitution data is null");
            return RC.CODE_INVALID_ARGUMENT;
        }

        for (int i = 0; i < data.length; ++i) {
            data[i] = Substitute(data[i]);
        }

        return RC.CODE_SUCCESS;
    }

    public static RC createSubstitutionTable(String filename, Logger logger, SubstitutionTable table) {
        PipelineBaseGrammar tableGrammar = new PipelineBaseGrammar(new String[] {}) {
            private final String delimiter = "->";

            @Override
            public String delimiter() {
                return delimiter;
            }

            @Override
            public boolean Contains(String token) {
                return true;
            }
        } ;

        HashMap<String, String> map = new HashMap<String, String>();
        RC res = FileParser.ReadMap(filename, tableGrammar, logger, map);
        if (res != RC.CODE_SUCCESS) {
            logger.severe("Cant read table");
            return res;
        }

        HashMap<Byte, Byte> subsTable = Convert(map);
        if (subsTable == null) {
            logger.severe("Failed to convert table to byte");
            table = null;
            return RC.CODE_CONFIG_SEMANTIC_ERROR;
        }

        table.SetData(subsTable, logger);

        return RC.CODE_SUCCESS;
    }

    private static HashMap<Byte, Byte> Convert(HashMap<String, String> table) {
        HashMap<Byte, Byte> byteTable = new HashMap<>();

        for (Map.Entry<String, String> entry:table.entrySet()) {
            Byte key = parseByte(entry.getKey());
            Byte value = parseByte(entry.getValue());
            if (key == null || value == null) {
                return null;
            }
            byteTable.put(key, value);
        }
        return byteTable;
    }

    private static Byte parseByte(String line) {
        if (line == null || line.length() != 4 || !line.startsWith("0x")) {
            return null;
        }
        byte res;
        try {
            res = (byte) Integer.parseInt(line.substring(2), 16);
        } catch (NumberFormatException ex) {
            return null;
        }
        return res;
    }
}