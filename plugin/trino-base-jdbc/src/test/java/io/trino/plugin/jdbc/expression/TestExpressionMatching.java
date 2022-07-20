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

import io.trino.matching.Match;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.FunctionName;
import io.trino.spi.expression.Variable;
import io.trino.spi.type.Type;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionMatching
{
    private static final Map<String, Set<String>> TYPE_CLASSES = Map.of("integer_class", Set.of("tinyint", "smallint", "integer", "bigint"));

    @Test
    public void testMatchType()
    {
        Type type = createDecimalType(10, 2);
        TypePattern pattern = typePattern("decimal(p, s)");

        Match match = pattern.getPattern().match(type).collect(onlyElement());
        MatchContext matchContext = new MatchContext();
        pattern.resolve(match.captures(), matchContext);

        assertThat(matchContext.keys()).containsExactlyInAnyOrder("p", "s");
        assertThat(matchContext.get("p")).isEqualTo(10L);
        assertThat(matchContext.get("s")).isEqualTo(2L);
    }

    @Test
    public void testExpressionCapture()
    {
        ConnectorExpression expression = new Call(
                createDecimalType(21, 2),
                new FunctionName("add"),
                List.of(
                        new Variable("first", createDecimalType(10, 2)),
                        new Variable("second", BIGINT)));
        ExpressionPattern pattern = expressionPattern("foo: decimal(p, s)");

        Match match = pattern.getPattern().match(expression).collect(onlyElement());
        MatchContext matchContext = new MatchContext();
        pattern.resolve(match.captures(), matchContext);

        assertThat(matchContext.keys()).containsExactlyInAnyOrder("p", "s", "foo");
        assertThat(matchContext.get("p")).isEqualTo(21L);
        assertThat(matchContext.get("s")).isEqualTo(2L);
        assertThat(matchContext.get("foo")).isSameAs(expression);
    }

    @Test
    public void testMatchCall()
    {
        ConnectorExpression expression = new Call(
                createDecimalType(21, 2),
                new FunctionName("add"),
                List.of(
                        new Variable("first", createDecimalType(10, 2)),
                        new Variable("second", BIGINT)));
        ExpressionPattern pattern = expressionPattern("add(foo: decimal(p, s), bar: bigint)");

        Match match = pattern.getPattern().match(expression).collect(onlyElement());
        MatchContext matchContext = new MatchContext();
        pattern.resolve(match.captures(), matchContext);

        assertThat(matchContext.keys()).containsExactlyInAnyOrder("p", "s", "foo", "bar");
        assertThat(matchContext.get("p")).isEqualTo(10L);
        assertThat(matchContext.get("s")).isEqualTo(2L);
        assertThat(matchContext.get("foo")).isEqualTo(new Variable("first", createDecimalType(10, 2)));
        assertThat(matchContext.get("bar")).isEqualTo(new Variable("second", BIGINT));
    }

    @Test
    public void testMatchCallWithTypeClass()
    {
        ConnectorExpression expression = new Call(
                BIGINT,
                new FunctionName("add"),
                List.of(
                        new Variable("first", INTEGER),
                        new Variable("second", BIGINT)));
        ExpressionPattern pattern = expressionPattern("add(foo: integer_class, bar: integer_class): integer_class");

        Match match = pattern.getPattern().match(expression).collect(onlyElement());
        MatchContext matchContext = new MatchContext();
        pattern.resolve(match.captures(), matchContext);

        assertThat(matchContext.keys()).containsExactlyInAnyOrder("foo", "bar");
        assertThat(matchContext.get("foo")).isEqualTo(new Variable("first", INTEGER));
        assertThat(matchContext.get("bar")).isEqualTo(new Variable("second", BIGINT));
    }

    private static ExpressionPattern expressionPattern(String expressionPattern)
    {
        return new ExpressionMappingParser(TYPE_CLASSES).createExpressionPattern(expressionPattern);
    }

    private static TypePattern typePattern(String typePattern)
    {
        return new ExpressionMappingParser(TYPE_CLASSES).createTypePattern(typePattern);
    }
}
