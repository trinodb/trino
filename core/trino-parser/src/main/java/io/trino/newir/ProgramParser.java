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
package io.trino.newir;

import io.trino.grammar.newir.NewIrBaseListener;
import io.trino.grammar.newir.NewIrLexer;
import io.trino.grammar.newir.NewIrParser;
import io.trino.newir.tree.ProgramNode;
import io.trino.sql.parser.ParsingException;
import io.trino.sql.tree.NodeLocation;
import org.antlr.v4.runtime.ANTLRErrorListener;
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

public class ProgramParser
{
    private static final ANTLRErrorListener ERROR_LISTENER = new BaseErrorListener()
    {
        @Override
        public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine, String message, RecognitionException e)
        {
            throw new ParsingException("parsing of the program failed: " + message, e, line, charPositionInLine + 1);
        }
    };

    private ProgramParser() {}

    public static ProgramNode parseProgram(String program)
    {
        try {
            NewIrLexer lexer = new NewIrLexer(CharStreams.fromString(program));
            CommonTokenStream tokenStream = new CommonTokenStream(lexer);
            NewIrParser parser = new NewIrParser(tokenStream);

            parser.addParseListener(new PostProcessor(Arrays.asList(parser.getRuleNames()), parser));

            lexer.removeErrorListeners();
            lexer.addErrorListener(ERROR_LISTENER);

            parser.removeErrorListeners();
            parser.addErrorListener(ERROR_LISTENER);

            ParserRuleContext tree;
            try {
                // first, try parsing with potentially faster SLL mode
                parser.getInterpreter().setPredictionMode(PredictionMode.SLL);
                tree = parser.program();
            }
            catch (ParsingException ex) {
                // if we fail, parse with LL mode
                tokenStream.seek(0); // rewind input stream
                parser.reset();

                parser.getInterpreter().setPredictionMode(PredictionMode.LL);
                tree = parser.program();
            }

            return (ProgramNode) new ProgramTreeBuilder().visit(tree);
        }
        catch (StackOverflowError e) {
            throw new ParsingException("stack overflow while parsing the program: ", new NodeLocation(1, 1));
        }
    }

    private static class PostProcessor
            extends NewIrBaseListener
    {
        private final List<String> ruleNames;
        private final NewIrParser parser;

        public PostProcessor(List<String> ruleNames, NewIrParser parser)
        {
            this.ruleNames = ruleNames;
            this.parser = parser;
        }

        @Override
        public void exitNonReserved(NewIrParser.NonReservedContext context)
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
                    NewIrLexer.IDENTIFIER,
                    token.getChannel(),
                    token.getStartIndex(),
                    token.getStopIndex());

            context.getParent().addChild(parser.createTerminalNode(context.getParent(), newToken));
        }
    }
}
