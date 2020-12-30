package ru.spbstu.akirillova.utils;

import ru.spbstu.pipeline.RC;

import java.io.*;
import java.util.HashMap;
import java.util.logging.Logger;

public class FileParser {

    private static void CloseStream(Closeable stream) {
        try {
            stream.close();
        }
        catch (IOException ex) { }
    }

    public static RC ReadMap(String filename, PipelineBaseGrammar grammar, Logger logger, HashMap<String, String> map) {
        FileInputStream inputStream;
        try {
            inputStream = new FileInputStream(filename);
        } catch (FileNotFoundException ex) {
            logger.warning("Cant open file");
            map = null;
            return RC.CODE_INVALID_INPUT_STREAM;
        }

        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        try {
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.isEmpty()) {
                    continue;
                }

                String key[] = new String[1], val[] = new String[1];
                key[0] = "";
                val[0] = "";
                boolean success = ParseMapElement(line, grammar, key, val);
                if (!success) {
                    logger.warning("Cant parse key value pair");
                    map = null;
                    return RC.CODE_CONFIG_GRAMMAR_ERROR;
                }

                if (map.containsKey(key[0])) {
                    logger.warning("ambiguous value for the key");
                    map = null;
                    return RC.CODE_CONFIG_GRAMMAR_ERROR;
                }
                map.put(key[0], val[0]);
            }
        } catch(IOException ex) {
            logger.warning("IOexception while reading a map");
            map = null;
            return RC.CODE_FAILED_TO_READ;
        }
        finally {
            CloseStream(reader);
        }

        return RC.CODE_SUCCESS;
    }

    private static boolean ParseMapElement(String line, PipelineBaseGrammar grammar, String[] key, String[] val) {
        String[] split = line.split(grammar.delimiter());

        if (split.length != 2) {
            return false;
        }
        else {
            key[0] += split[0].trim();
            val[0] += split[1].trim();

            return grammar.Contains(key[0]);
        }
    }
}