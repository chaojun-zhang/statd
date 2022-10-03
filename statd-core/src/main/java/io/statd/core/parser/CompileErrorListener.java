
package io.statd.core.parser;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;


public class CompileErrorListener extends BaseErrorListener {
    private final String expression;

    public CompileErrorListener(String expression) {
        this.expression = expression;
    }

    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String msg, RecognitionException e) {
        throw new CompileException(String.format("syntax error (%s : %s) line:%s, pos:%s ", msg, expression, line, charPositionInLine));
    }

}
