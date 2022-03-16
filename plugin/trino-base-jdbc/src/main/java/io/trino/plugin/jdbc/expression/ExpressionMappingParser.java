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
package io.trino.plugin.jdbc.expression;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.ParseCancellationException;

import java.util.function.Function;

import static java.lang.String.format;

public class ExpressionMappingParser
{
    private static final BaseErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new IllegalArgumentException(format("Error at %s:%s: %s", line, charPositionInLine, message), e);
        }
    };

    public ExpressionPattern createExpressionPattern(String expressionPattern)
    {
        return (ExpressionPattern) invokeParser(expressionPattern, ConnectorExpressionPatternParser::standaloneExpression);
    }

    public SimpleTypePattern createTypePattern(String typePattern)
    {
        return (SimpleTypePattern) invokeParser(typePattern, ConnectorExpressionPatternParser::standaloneType);
    }

    public Object invokeParser(String input, Function<ConnectorExpressionPatternParser, ParserRuleContext> parseFunction)
    {
        try {
            ConnectorExpressionPatternLexer lexer = new ConnectorExpressionPatternLexer(CharStreams.fromString(input));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            ConnectorExpressionPatternParser parser = new ConnectorExpressionPatternParser(tokenStream);

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
            return new ExpressionPatternBuilder().visit(tree);
        }
        catch (StackOverflowError e) {
            throw new IllegalArgumentException("expression pattern is too large (stack overflow while parsing)");
        }
    }
}
