/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.deltalake.expression;

import com.google.common.annotations.VisibleForTesting;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.function.Function;

import static com.google.common.base.MoreObjects.firstNonNull;

public final class SparkExpressionParser
{
    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new ParsingException(message, e);
        }
    };

    private SparkExpressionParser() {}

    public static String toTrinoExpression(String sparkExpression)
    {
        SparkExpression expression = createExpression(sparkExpression);
        return SparkExpressionConverter.toTrinoExpression(expression);
    }

    @VisibleForTesting
    static SparkExpression createExpression(String expression)
    {
        try {
            return (SparkExpression) invokeParser(expression, SparkExpressionBaseParser::standaloneExpression);
        }
        catch (Exception e) {
            throw new ParsingException("Cannot parse Spark expression [%s]: %s".formatted(expression, firstNonNull(e.getMessage(), e)), e);
        }
    }

    private static Object invokeParser(String input, Function<SparkExpressionBaseParser, ParserRuleContext> parseFunction)
    {
        try {
            SparkExpressionBaseLexer lexer = new SparkExpressionBaseLexer(new CaseInsensitiveStream(CharStreams.fromString(input)));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            SparkExpressionBaseParser parser = new SparkExpressionBaseParser(tokenStream);

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            ParserRuleContext tree;
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parseFunction.apply(parser);
            }
            catch (ParseCancellationException ex) {
                // if we fail, parse with LL mode
                tokenStream.seek(0); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parseFunction.apply(parser);
            }
            return new SparkExpressionBuilder().visit(tree);
        }
        catch (StackOverflowError e) {
            throw new IllegalArgumentException("expression is too large (stack overflow while parsing)");
        }
    }
}
