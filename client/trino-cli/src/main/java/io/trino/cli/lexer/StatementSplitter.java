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
package io.trino.cli.lexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.trino.grammar.sql.SqlBaseBaseVisitor;
import io.trino.grammar.sql.SqlBaseLexer;
import io.trino.grammar.sql.SqlBaseParser;
import io.trino.grammar.sql.SqlBaseParser.FunctionSpecificationContext;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.ParserRuleContext;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenSource;
import org.antlr.v4.runtime.tree.ParseTree;
import org.antlr.v4.runtime.tree.RuleNode;

import java.util.List;
import java.util.Objects;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class StatementSplitter
{
    private final List<Statement> completeStatements;
    private final String partialStatement;

    public StatementSplitter(String sql)
    {
        this(sql, ImmutableSet.of(";"));
    }

    public StatementSplitter(String sql, Set<String> delimiters)
    {
        DelimiterLexer lexer = getLexer(sql, delimiters);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);
        tokenStream.fill();

        SqlBaseParser parser = new SqlBaseParser(tokenStream);
        parser.removeErrorListeners();

        ImmutableList.Builder<Statement> list = ImmutableList.builder();
        StringBuilder sb = new StringBuilder();
        int index = 0;

        while (index < tokenStream.size()) {
            ParserRuleContext context = parser.statement();

            if (containsFunction(context)) {
                Token stop = context.getStop();
                if ((stop != null) && (stop.getTokenIndex() >= index)) {
                    int endIndex = stop.getTokenIndex();
                    while (index <= endIndex) {
                        Token token = tokenStream.get(index);
                        index++;
                        sb.append(token.getText());
                    }
                }
            }

            while (index < tokenStream.size()) {
                Token token = tokenStream.get(index);
                index++;
                if (token.getType() == Token.EOF) {
                    break;
                }
                if (lexer.isDelimiter(token)) {
                    String statement = sb.toString().trim();
                    if (!statement.isEmpty()) {
                        list.add(new Statement(statement, token.getText()));
                    }
                    sb = new StringBuilder();
                    break;
                }
                sb.append(token.getText());
            }
        }

        this.completeStatements = list.build();
        this.partialStatement = sb.toString().trim();
    }

    public List<Statement> getCompleteStatements()
    {
        return completeStatements;
    }

    public String getPartialStatement()
    {
        return partialStatement;
    }

    public static String squeezeStatement(String sql)
    {
        TokenSource tokens = getLexer(sql, ImmutableSet.of());
        StringBuilder sb = new StringBuilder();
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                break;
            }
            if (token.getType() == SqlBaseLexer.WS) {
                sb.append(' ');
            }
            else {
                sb.append(token.getText());
            }
        }
        return sb.toString().trim();
    }

    public static boolean isEmptyStatement(String sql)
    {
        TokenSource tokens = getLexer(sql, ImmutableSet.of());
        while (true) {
            Token token = tokens.nextToken();
            if (token.getType() == Token.EOF) {
                return true;
            }
            if (token.getChannel() != Token.HIDDEN_CHANNEL) {
                return false;
            }
        }
    }

    public static DelimiterLexer getLexer(String sql, Set<String> terminators)
    {
        requireNonNull(sql, "sql is null");
        return new DelimiterLexer(CharStreams.fromString(sql), terminators);
    }

    private static boolean containsFunction(ParseTree tree)
    {
        return new SqlBaseBaseVisitor<Boolean>()
        {
            @Override
            protected Boolean defaultResult()
            {
                return false;
            }

            @Override
            protected Boolean aggregateResult(Boolean aggregate, Boolean nextResult)
            {
                return aggregate || nextResult;
            }

            @Override
            protected boolean shouldVisitNextChild(RuleNode node, Boolean currentResult)
            {
                return !currentResult;
            }

            @Override
            public Boolean visitFunctionSpecification(FunctionSpecificationContext context)
            {
                return true;
            }
        }.visit(tree);
    }

    public static class Statement
    {
        private final String statement;
        private final String terminator;

        public Statement(String statement, String terminator)
        {
            this.statement = requireNonNull(statement, "statement is null");
            this.terminator = requireNonNull(terminator, "terminator is null");
        }

        public String statement()
        {
            return statement;
        }

        public String terminator()
        {
            return terminator;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if ((obj == null) || (getClass() != obj.getClass())) {
                return false;
            }
            Statement o = (Statement) obj;
            return Objects.equals(statement, o.statement) &&
                    Objects.equals(terminator, o.terminator);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(statement, terminator);
        }

        @Override
        public String toString()
        {
            return statement + terminator;
        }
    }
}
