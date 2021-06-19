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
package io.trino.sql.parser;

import io.trino.sql.SqlFormatter;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.Node;
import io.trino.sql.tree.RowPattern;
import io.trino.sql.tree.Statement;
import org.assertj.core.api.AssertProvider;
import org.assertj.core.api.RecursiveComparisonAssert;
import org.assertj.core.api.recursive.comparison.RecursiveComparisonConfiguration;
import org.assertj.core.presentation.StandardRepresentation;

import java.util.function.Function;

import static io.trino.sql.SqlFormatter.formatSql;
import static io.trino.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;

public class ParserAssert
        extends RecursiveComparisonAssert<ParserAssert>
{
    private static final StandardRepresentation NODE_REPRESENTATION = new StandardRepresentation()
    {
        @Override
        public String toStringOf(Object object)
        {
            if (object instanceof Statement || object instanceof Expression || object instanceof RowPattern) {
                return SqlFormatter.formatSql((Node) object);
            }
            return super.toStringOf(object);
        }
    };

    public static AssertProvider<ParserAssert> type(String sql)
    {
        return createAssertion(new SqlParser()::createType, sql);
    }

    public static AssertProvider<ParserAssert> expression(String sql)
    {
        return createAssertion(expression -> new SqlParser().createExpression(expression, new ParsingOptions(AS_DECIMAL)), sql);
    }

    public static AssertProvider<ParserAssert> statement(String sql)
    {
        return createAssertion(statement -> new SqlParser().createStatement(statement, new ParsingOptions(AS_DECIMAL)), sql);
    }

    public static AssertProvider<ParserAssert> rowPattern(String sql)
    {
        return createAssertion(new SqlParser()::createRowPattern, sql);
    }

    private ParserAssert(Node actual, RecursiveComparisonConfiguration recursiveComparisonConfiguration)
    {
        super(actual, recursiveComparisonConfiguration);
    }

    public ParserAssert ignoringLocation()
    {
        return ignoringFieldsMatchingRegexes("(.*\\.)?location");
    }

    private static <T extends Node> AssertProvider<ParserAssert> createAssertion(Function<String, T> parser, String sql)
    {
        return () -> new ParserAssert(parser.apply(sql), newRecursiveComparisonConfig())
                .withRepresentation(NODE_REPRESENTATION)
                .satisfies(parsed -> new ParserAssert(parser.apply(formatSql((Node) parsed)), newRecursiveComparisonConfig())
                        .describedAs("Validate SQL->AST->SQL roundtrip")
                        .withRepresentation(NODE_REPRESENTATION)
                        .ignoringLocation()
                        .isEqualTo(parser.apply(sql)));
    }

    private static RecursiveComparisonConfiguration newRecursiveComparisonConfig()
    {
        RecursiveComparisonConfiguration config = new RecursiveComparisonConfiguration();
        config.ignoreAllOverriddenEquals();
        config.strictTypeChecking(true);
        return config;
    }
}
