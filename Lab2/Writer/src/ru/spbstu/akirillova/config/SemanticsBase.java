package ru.spbstu.akirillova.config;

import java.io.File;
import java.util.logging.Logger;

public abstract class SemanticsBase {

    private final Logger logger;
    protected SemanticsBase(Logger logger) {
        this.logger = logger;
    }

    public abstract boolean ValidateField(String fieldName, String fieldValue);

    public static boolean IsPositiveInt(String value) {
        try {
            int i = Integer.parseInt(value);
            if (i <= 0) {
                return false;
            }
        }
        catch (NumberFormatException ex) {
            return false;
        }
        return true;
    }

    public static boolean IsFile(String value) {
        if (value == null) {
            return false;
        }
        return new File(value).exists();
    }

    protected Logger GetLogger() {
        return logger;
    }
}
