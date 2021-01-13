package ru.spbstu.akirillova.utils;

import ru.spbstu.pipeline.BaseGrammar;

public class PipelineBaseGrammar extends BaseGrammar {

    protected PipelineBaseGrammar(String[] tokens) {
        super(tokens);
    }

    public boolean Contains(String token) {
        for (int i = 0; i < numberTokens(); ++i) {
            if (token(i).equals(token)) {
                return true;
            }
        }
        return false;
    }
}
