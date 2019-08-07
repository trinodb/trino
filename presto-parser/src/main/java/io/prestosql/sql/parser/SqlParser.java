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
package io.prestosql.sql.parser;

import io.airlift.log.Logger;
import io.prestosql.sql.parser.hive.HiveAstBuilder;
import io.prestosql.sql.tree.*;
import org.antlr.v4.runtime.*;
import org.antlr.v4.runtime.InputMismatchException;
import org.antlr.v4.runtime.atn.PredictionMode;
import org.antlr.v4.runtime.misc.Pair;
import org.antlr.v4.runtime.misc.ParseCancellationException;
import org.antlr.v4.runtime.tree.AbstractParseTreeVisitor;
import org.antlr.v4.runtime.tree.TerminalNode;

import javax.inject.Inject;

import java.util.*;
import java.util.function.Function;

import static java.util.Objects.requireNonNull;

public class SqlParser
{
    private static final Logger LOG = Logger.get(SqlParser.class);

    private static final BaseErrorListener LEXER_ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new ParsingException(message, e, line, charPositionInLine);
        }
    };

    private static final ErrorHandler PARSER_ERROR_HANDLER = ErrorHandler.builder()
            .specialRule(SqlBaseParser.RULE_expression, "<expression>")
            .specialRule(SqlBaseParser.RULE_booleanExpression, "<expression>")
            .specialRule(SqlBaseParser.RULE_valueExpression, "<expression>")
            .specialRule(SqlBaseParser.RULE_primaryExpression, "<expression>")
            .specialRule(SqlBaseParser.RULE_identifier, "<identifier>")
            .specialRule(SqlBaseParser.RULE_string, "<string>")
            .specialRule(SqlBaseParser.RULE_query, "<query>")
            .specialRule(SqlBaseParser.RULE_type, "<type>")
            .specialToken(SqlBaseLexer.INTEGER_VALUE, "<integer>")
            .ignoredRule(SqlBaseParser.RULE_nonReserved)
            .build();

    private final EnumSet<IdentifierSymbol> allowedIdentifierSymbols;
    private boolean enhancedErrorHandlerEnabled;

    public SqlParser()
    {
        this(new SqlParserOptions());
    }

    @Inject
    public SqlParser(SqlParserOptions options)
    {
        requireNonNull(options, "options is null");
        allowedIdentifierSymbols = EnumSet.copyOf(options.getAllowedIdentifierSymbols());
        enhancedErrorHandlerEnabled = options.isEnhancedErrorHandlerEnabled();
    }

    /**
     * Consider using {@link #createStatement(String, ParsingOptions)}
     */
    @Deprecated
    public Statement createStatement(String sql)
    {
        return createStatement(sql, new ParsingOptions());
    }

    public Statement createStatement(String sql, ParsingOptions parsingOptions)
    {
        Node node = invokeParser("statement",
                sql, SqlBaseParser::singleStatement, parsingOptions, "singleStatement");
        Statement statement = (Statement) node;
        return statement;
    }

    /**
     * Consider using {@link #createExpression(String, ParsingOptions)}
     */
    @Deprecated
    public Expression createExpression(String expression)
    {
        LOG.info("from Expression createExpression(String expression)");
        return createExpression(expression, new ParsingOptions());
    }

    public Expression createExpression(String expression, ParsingOptions parsingOptions)
    {
        LOG.info("from Expression createExpression(String expression, ParsingOptions parsingOptions)");
        Expression ex = (Expression) invokeParser("expression", expression,
                SqlBaseParser::standaloneExpression, parsingOptions, "standaloneExpression");
        return ex;
    }

    public PathSpecification createPathSpecification(String expression)
    {
        LOG.info("from PathSpecification createPathSpecification(String expression)");
        PathSpecification plan = (PathSpecification) invokeParser("path specification",
                expression, SqlBaseParser::standalonePathSpecification, new ParsingOptions(), "standalonePathSpecification");
        return plan;
    }

    private Node invokeParser(String name, String sql, Function<SqlBaseParser,
            ParserRuleContext> parseFunction, ParsingOptions parsingOptions, String type)
    {
        LOG.info("sql:" + sql);
        if (!parsingOptions.useHiveParser()) {
            LOG.info("use presto sql");
            return invokePrestoParser(name, sql, parsingOptions, type);
        } else {
            LOG.info("use hive sql");
            return invokeSparkBasedHiveParser(name, sql, parsingOptions, type);
        }
    }


    private Node invokeCommonParser(String name, String type,
                                    Lexer lexer, Parser parser, AbstractParseTreeVisitor<Node> visitor)
    {
        try {
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);

            // Override the default error strategy to not attempt inserting or deleting a token.
            // Otherwise, it messes up error reporting
            parser.setErrorHandler(new DefaultErrorStrategy()
            {
                @Override
                public Token recoverInline(Parser recognizer)
                        throws RecognitionException
                {
                    if (nextTokensContext == null) {
                        throw new InputMismatchException(recognizer);
                    }
                    else {
                        throw new InputMismatchException(recognizer, nextTokensState, nextTokensContext);
                    }
                }
            });

            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames())));

            lexer.removeErrorListeners();
            lexer.addErrorListener(LEXER_ERROR_LISTENER);

            parser.removeErrorListeners();

            if (enhancedErrorHandlerEnabled) {
                parser.addErrorListener(PARSER_ERROR_HANDLER);
            }
            else {
                parser.addErrorListener(LEXER_ERROR_LISTENER);
            }

            ParserRuleContext tree = getTree(type, parser, tokenStream);

            return visitor.visit(tree);
        }
        catch (StackOverflowError e) {
            throw new ParsingException(name + " is too large (stack overflow while parsing)");
        }
    }

    private ParserRuleContext getTree(String type, Parser parser, CommonTokenStream tokenStream) {

        ParserRuleContext tree = null;
        if (type.startsWith("spark")) {
            Function<io.hivesql.sql.parser.SqlBaseParser, ParserRuleContext> parseFunction = null;
            if (type.endsWith("singleStatement")) {
                parseFunction = io.hivesql.sql.parser.SqlBaseParser::singleStatement;
            } else if (type.endsWith("standaloneExpression")) {
                parseFunction = io.hivesql.sql.parser.SqlBaseParser::singleExpression;
            } else {
                parseFunction = io.hivesql.sql.parser.SqlBaseParser::singleStatement;
            }
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parseFunction.apply((io.hivesql.sql.parser.SqlBaseParser)parser);
            }
            catch (ParseCancellationException ex) {
                // if we fail, parse with LL mode
                tokenStream.reset(); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parseFunction.apply((io.hivesql.sql.parser.SqlBaseParser)parser);
            }
        } else if (type.startsWith("presto")) {
            Function<SqlBaseParser, ParserRuleContext> parseFunction = null;
            if (type.endsWith("singleStatement")) {
                parseFunction = SqlBaseParser::singleStatement;
            } else if (type.endsWith("standaloneExpression")) {
                parseFunction = SqlBaseParser::standaloneExpression;
            } else {
                parseFunction = SqlBaseParser::standalonePathSpecification;
            }
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parseFunction.apply((SqlBaseParser)parser);
            }
            catch (ParseCancellationException ex) {
                // if we fail, parse with LL mode
                tokenStream.reset(); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parseFunction.apply((SqlBaseParser)parser);
            }
        }
        return tree;
    }

    private Node invokeSparkBasedHiveParser(String name, String sql, ParsingOptions parsingOptions, String type)
    {
        try {
            io.hivesql.sql.parser.SqlBaseLexer lexer = new io.hivesql.sql.parser.SqlBaseLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            io.hivesql.sql.parser.SqlBaseParser parser = new io.hivesql.sql.parser.SqlBaseParser(tokenStream);
            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames())));

            HiveAstBuilder hiveAstBuilder = new HiveAstBuilder(parsingOptions);

            return invokeCommonParser(name, "spark_" + type, lexer,
                    parser, hiveAstBuilder);
        }
        catch (StackOverflowError e) {
            throw new ParsingException(name + " is too large (stack overflow while parsing)");
        }
    }

    private Node invokePrestoParser(String name, String sql,
            ParsingOptions parsingOptions, String type)
    {
        try {
            SqlBaseLexer lexer = new SqlBaseLexer(new CaseInsensitiveStream(CharStreams.fromString(sql)));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            SqlBaseParser parser = new SqlBaseParser(tokenStream);
            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames())));

            AstBuilder hiveAstBuilder = new AstBuilder(parsingOptions);

            return invokeCommonParser(name, "presto_" + type, lexer,
                    parser, hiveAstBuilder);
        }
        catch (StackOverflowError e) {
            throw new ParsingException(name + " is too large (stack overflow while parsing)");
        }
    }

    private class PostProcessor
            extends SqlBaseBaseListener
    {
        private final List<String> ruleNames;

        public PostProcessor(List<String> ruleNames)
        {
            this.ruleNames = ruleNames;
        }

        @Override
        public void exitQuotedIdentifier(SqlBaseParser.QuotedIdentifierContext context)
        {
            Token token = context.QUOTED_IDENTIFIER().getSymbol();
            if (token.getText().length() == 2) { // empty identifier
                throw new ParsingException("Zero-length delimited identifier not allowed", null, token.getLine(), token.getCharPositionInLine());
            }
        }

        @Override
        public void exitUnquotedIdentifier(SqlBaseParser.UnquotedIdentifierContext context)
        {
            String identifier = context.IDENTIFIER().getText();
            for (IdentifierSymbol identifierSymbol : EnumSet.complementOf(allowedIdentifierSymbols)) {
                char symbol = identifierSymbol.getSymbol();
                if (identifier.indexOf(symbol) >= 0) {
                    throw new ParsingException("identifiers must not contain '" + identifierSymbol.getSymbol() + "'", null, context.IDENTIFIER().getSymbol().getLine(), context.IDENTIFIER().getSymbol().getCharPositionInLine());
                }
            }
        }

        @Override
        public void exitBackQuotedIdentifier(SqlBaseParser.BackQuotedIdentifierContext context)
        {
            Token token = context.BACKQUOTED_IDENTIFIER().getSymbol();
            throw new ParsingException(
                    "backquoted identifiers are not supported; use double quotes to quote identifiers",
                    null,
                    token.getLine(),
                    token.getCharPositionInLine());
        }

        @Override
        public void exitDigitIdentifier(SqlBaseParser.DigitIdentifierContext context)
        {
            Token token = context.DIGIT_IDENTIFIER().getSymbol();
            throw new ParsingException(
                    "identifiers must not start with a digit; surround the identifier with double quotes",
                    null,
                    token.getLine(),
                    token.getCharPositionInLine());
        }

        @Override
        public void exitNonReserved(SqlBaseParser.NonReservedContext context)
        {
            // we can't modify the tree during rule enter/exit event handling unless we're dealing with a terminal.
            // Otherwise, ANTLR gets confused an fires spurious notifications.
            if (!(context.getChild(0) instanceof TerminalNode)) {
                int rule = ((ParserRuleContext) context.getChild(0)).getRuleIndex();
                throw new AssertionError("nonReserved can only contain tokens. Found nested rule: " + ruleNames.get(rule));
            }

            // replace nonReserved words with IDENT tokens
            context.getParent().removeLastChild();

            Token token = (Token) context.getChild(0).getPayload();
            context.getParent().addChild(new CommonToken(
                    new Pair<>(token.getTokenSource(), token.getInputStream()),
                    SqlBaseLexer.IDENTIFIER,
                    token.getChannel(),
                    token.getStartIndex(),
                    token.getStopIndex()));
        }
    }
}