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
package io.trino.sql.jsonpath;

import io.trino.grammar.jsonpath.JsonPathBaseListener;
import io.trino.grammar.jsonpath.JsonPathLexer;
import io.trino.grammar.jsonpath.JsonPathParser;
import io.trino.sql.jsonpath.tree.PathNode;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.tree.NodeLocation;
import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonToken;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.tree.TerminalNode;

import java.util.Arrays;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class PathParser
{
    private final BaseErrorListener errorListener;

    public static PathParser withRelativeErrorLocation(Location startLocation)
    {
        requireNonNull(startLocation, "startLocation is null");

        int pathStartLine = startLocation.line();
        int pathStartColumn = startLocation.column();
        return new PathParser(new BaseErrorListener()
        {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
            {
                // The line and charPositionInLine correspond to the character within the string literal with JSON path expression.
                // Line and offset in error returned to the user should be computed based on the beginning of the whole query text.
                // We re-position the exception relatively to the start of the path expression within the query.
                int lineInQuery = pathStartLine - 1 + line;
                int columnInQuery = line == 1 ? pathStartColumn + 1 + charPositionInLine : charPositionInLine + 1;
                throw new ParsingException(message, e, lineInQuery, columnInQuery);
            }
        });
    }

    public static PathParser withFixedErrorLocation(Location location)
    {
        requireNonNull(location, "location is null");

        return new PathParser(new BaseErrorListener()
        {
            @Override
            public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
            {
                throw new ParsingException(message, e, location.line, location.column);
            }
        });
    }

    private PathParser(BaseErrorListener errorListener)
    {
        this.errorListener = requireNonNull(errorListener, "errorListener is null");
    }

    public PathNode parseJsonPath(String path)
    {
        try {
            // according to the SQL specification, the path language is case-sensitive in both identifiers and key words
            JsonPathLexer lexer = new JsonPathLexer(CharStreams.fromString(path));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            JsonPathParser parser = new JsonPathParser(tokenStream);

            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames()), parser));

            lexer.removeErrorListeners();
            lexer.addErrorListener(errorListener);

            parser.removeErrorListeners();
            parser.addErrorListener(errorListener);

            ParserRuleContext tree;
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parser.path();
            }
            catch (ParsingException ex) {
                // if we fail, parse with LL mode
                tokenStream.seek(0); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parser.path();
            }

            return new PathTreeBuilder().visit(tree);
        }
        catch (StackOverflowError e) {
            throw new ParsingException("stack overflow while parsing JSON path", new NodeLocation(1, 1));
        }
    }

    private static class PostProcessor
            extends JsonPathBaseListener
    {
        private final List<String> ruleNames;
        private final JsonPathParser parser;

        public PostProcessor(List<String> ruleNames, JsonPathParser parser)
        {
            this.ruleNames = ruleNames;
            this.parser = parser;
        }

        @Override
        public void exitNonReserved(JsonPathParser.NonReservedContext context)
        {
            // only a terminal can be replaced during rule exit event handling. Make sure that the nonReserved item is a token
            if (!(context.getChild(0) instanceof TerminalNode)) {
                int rule = ((ParserRuleContext) context.getChild(0)).getRuleIndex();
                throw new AssertionError("nonReserved can only contain tokens. Found nested rule: " + ruleNames.get(rule));
            }

            // replace nonReserved keyword with IDENTIFIER token
            context.getParent().removeLastChild();

            Token token = (Token) context.getChild(0).getPayload();
            Token newToken = new CommonToken(
                    new Pair<>(token.getTokenSource(), token.getInputStream()),
                    JsonPathLexer.IDENTIFIER,
                    token.getChannel(),
                    token.getStartIndex(),
                    token.getStopIndex());

            context.getParent().addChild(parser.createTerminalNode(context.getParent(), newToken));
        }
    }

    public record Location(int line, int column)
    {
        public Location
        {
            checkArgument(line >= 1, "line must be at least 1");
            checkArgument(column >= 0, "column must be at least 0");
        }
    }
}
