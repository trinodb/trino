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
package io.trino.sql.ir;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;
import io.airlift.json.ObjectMapperProvider;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static io.trino.sql.ir.ArithmeticUnaryExpression.negative;
import static io.trino.sql.ir.ArithmeticUnaryExpression.positive;
import static io.trino.sql.ir.BooleanLiteral.FALSE_LITERAL;
import static io.trino.sql.ir.BooleanLiteral.TRUE_LITERAL;
import static io.trino.sql.tree.SortItem.NullOrdering.UNDEFINED;
import static io.trino.sql.tree.SortItem.Ordering.ASCENDING;
import static io.trino.sql.tree.SortItem.Ordering.DESCENDING;
import static io.trino.sql.tree.Trim.Specification.BOTH;
import static io.trino.sql.tree.Trim.Specification.LEADING;
import static io.trino.sql.tree.Trim.Specification.TRAILING;
import static org.testng.Assert.assertEquals;

public class TestIrExpressionSerialization
{
    private JsonCodec<Expression> codec;

    @BeforeClass
    public void setUp()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        codec = new JsonCodecFactory(provider).jsonCodec(Expression.class);
    }

    @Test
    public void testLiteral()
            throws Exception
    {
        assertJsonRoundTrip(codec, new NullLiteral());
        assertJsonRoundTrip(codec, new BinaryLiteral("12fa"));
        assertJsonRoundTrip(codec, TRUE_LITERAL);
        assertJsonRoundTrip(codec, FALSE_LITERAL);
        assertJsonRoundTrip(codec, BooleanLiteral.of("true"));
        assertJsonRoundTrip(codec, BooleanLiteral.of("false"));
        assertJsonRoundTrip(codec, CharLiteral.of("blabla "));
        assertJsonRoundTrip(codec, CharLiteral.of(" blabla "));
        assertJsonRoundTrip(codec, CharLiteral.of("  blab la"));
        assertJsonRoundTrip(codec, CharLiteral.of("  blab la"));
    }

    @Test
    public void testPosition()
    {
        assertJsonRoundTrip(codec, new FunctionCall(QualifiedName.of("strpos"), ImmutableList.of(
                new StringLiteral("b"),
                new StringLiteral("a"))));
        assertJsonRoundTrip(codec, new FunctionCall(QualifiedName.of("strpos"), ImmutableList.of(
                new StringLiteral("b"),
                new StringLiteral("a"))));
    }

    @Test
    public void testQualifiedName()
    {
        ObjectMapperProvider provider = new ObjectMapperProvider();
        JsonCodec<QualifiedName> qualifiedNameCodec = new JsonCodecFactory(provider).jsonCodec(QualifiedName.class);
        assertJsonRoundTrip(qualifiedNameCodec, QualifiedName.of("A", "b", "C", "d"));
        assertJsonRoundTrip(qualifiedNameCodec, QualifiedName.of("a", "d"));
        assertJsonRoundTrip(qualifiedNameCodec, QualifiedName.of("a", "c", "d"));
    }

    @Test
    public void testBinaryLiteral()
    {
        assertJsonRoundTrip(codec, new BinaryLiteral(""));
        assertJsonRoundTrip(codec, new BinaryLiteral("abcdef1234567890ABCDEF"));
    }

    @Test
    public void testGenericLiteral()
    {
        assertGenericLiteral("VARCHAR");
        assertGenericLiteral("BIGINT");
        assertGenericLiteral("DOUBLE");
        assertGenericLiteral("BOOLEAN");
        assertGenericLiteral("DATE");
        assertGenericLiteral("foo");
    }

    private void assertGenericLiteral(String type)
    {
        assertJsonRoundTrip(codec, new GenericLiteral(type, "abc"));
    }

    @Test
    public void testLiterals()
    {
        assertJsonRoundTrip(codec, new TimeLiteral("abc"));
        assertJsonRoundTrip(codec, new IntervalLiteral("33", io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE, io.trino.sql.tree.IntervalLiteral.IntervalField.DAY, Optional.empty()));
        assertJsonRoundTrip(codec, new IntervalLiteral("33", io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE, io.trino.sql.tree.IntervalLiteral.IntervalField.DAY, Optional.of(io.trino.sql.tree.IntervalLiteral.IntervalField.SECOND)));
        assertJsonRoundTrip(codec, CharLiteral.of("abc"));
    }

    @Test
    public void testNumbers()
    {
        assertJsonRoundTrip(codec, new LongLiteral("9223372036854775807"));
        assertJsonRoundTrip(codec, new LongLiteral("-9223372036854775808"));

        assertJsonRoundTrip(codec, new DoubleLiteral("1E5"));
        assertJsonRoundTrip(codec, new DoubleLiteral("1E-5"));
        assertJsonRoundTrip(codec, new DoubleLiteral(".1E-5"));
        assertJsonRoundTrip(codec, new DoubleLiteral("1.1E5"));
        assertJsonRoundTrip(codec, new DoubleLiteral("1.1E-5"));

        assertJsonRoundTrip(codec, new DoubleLiteral("-1E5"));
        assertJsonRoundTrip(codec, new DoubleLiteral("-1E-5"));
        assertJsonRoundTrip(codec, new DoubleLiteral("-.1E-5"));
        assertJsonRoundTrip(codec, new DoubleLiteral("-1.1E5"));
        assertJsonRoundTrip(codec, new DoubleLiteral("-1.1E-5"));

        assertJsonRoundTrip(codec, new DecimalLiteral(".1"));
        assertJsonRoundTrip(codec, new DecimalLiteral("1.2"));
        assertJsonRoundTrip(codec, new DecimalLiteral("-1.2"));
    }

    @Test
    public void testArrayConstructor()
    {
        assertJsonRoundTrip(codec, new ArrayConstructor(ImmutableList.of()));
        assertJsonRoundTrip(codec, new ArrayConstructor(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))));
        assertJsonRoundTrip(codec, new ArrayConstructor(ImmutableList.of(new DoubleLiteral("1.0"), new DoubleLiteral("2.5"))));
        assertJsonRoundTrip(codec, new ArrayConstructor(ImmutableList.of(new StringLiteral("hi"))));
        assertJsonRoundTrip(codec, new ArrayConstructor(ImmutableList.of(new StringLiteral("hi"), new StringLiteral("hello"))));
    }

    @Test
    public void testArraySubscript()
    {
        assertJsonRoundTrip(codec, new SubscriptExpression(
                new ArrayConstructor(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))),
                new LongLiteral("1")));

        assertJsonRoundTrip(codec, new SearchedCaseExpression(
                        ImmutableList.of(
                                new WhenClause(
                                        BooleanLiteral.of("true"),
                                        new ArrayConstructor(ImmutableList.of(new LongLiteral("1"), new LongLiteral("2"))))),
                        Optional.empty()));
    }

    @Test
    public void testRowSubscript()
    {
        assertJsonRoundTrip(codec, new SubscriptExpression(
                new Row(ImmutableList.of(new LongLiteral("1"), new StringLiteral("a"), BooleanLiteral.of("true"))),
                new LongLiteral("1")));
    }

    @Test
    public void testDouble()
    {
        assertJsonRoundTrip(codec, new DoubleLiteral("123E7"));
        assertJsonRoundTrip(codec, new DoubleLiteral("123E-7"));

        assertJsonRoundTrip(codec, new DoubleLiteral("123.456E7"));
        assertJsonRoundTrip(codec, new DoubleLiteral("123.456E-7"));

        assertJsonRoundTrip(codec, new DoubleLiteral(".4E42"));
        assertJsonRoundTrip(codec, new DoubleLiteral(".4E-42"));
    }

    @Test
    public void testArithmeticUnary()
    {
        assertJsonRoundTrip(codec, new LongLiteral("9"));
        assertJsonRoundTrip(codec, positive(new LongLiteral("9")));
        assertJsonRoundTrip(codec, positive(positive(new LongLiteral("9"))));
        assertJsonRoundTrip(codec, positive(positive(positive(new LongLiteral("9")))));
        assertJsonRoundTrip(codec, new LongLiteral("-9"));
        assertJsonRoundTrip(codec, negative(new LongLiteral("-9")));
        assertJsonRoundTrip(codec, negative(positive(negative(positive(new LongLiteral("9"))))));
        assertJsonRoundTrip(codec, negative(negative(new LongLiteral("-9"))));
    }

    @Test
    public void testCoalesce()
    {
        assertJsonRoundTrip(codec, new CoalesceExpression(new LongLiteral("13"), new LongLiteral("42")));
        assertJsonRoundTrip(codec, new CoalesceExpression(new LongLiteral("6"), new LongLiteral("7"), new LongLiteral("8")));
        assertJsonRoundTrip(codec, new CoalesceExpression(new LongLiteral("13"), new NullLiteral()));
        assertJsonRoundTrip(codec, new CoalesceExpression(new NullLiteral(), new LongLiteral("13")));
        assertJsonRoundTrip(codec, new CoalesceExpression(new NullLiteral(), new NullLiteral()));
    }

    @Test
    public void testIf()
    {
        assertJsonRoundTrip(codec, new IfExpression(BooleanLiteral.of("true"), new LongLiteral("1"), new LongLiteral("0")));
        assertJsonRoundTrip(codec, new IfExpression(BooleanLiteral.of("true"), new LongLiteral("3"), new NullLiteral()));
        assertJsonRoundTrip(codec, new IfExpression(BooleanLiteral.of("false"), new NullLiteral(), new LongLiteral("4")));
        assertJsonRoundTrip(codec, new IfExpression(BooleanLiteral.of("false"), new NullLiteral(), new NullLiteral()));
        assertJsonRoundTrip(codec, new IfExpression(BooleanLiteral.of("true"), new LongLiteral("3"), null));
    }

    @Test
    public void testNullIf()
    {
        assertJsonRoundTrip(codec, new NullIfExpression(new LongLiteral("42"), new LongLiteral("87")));
        assertJsonRoundTrip(codec, new NullIfExpression(new LongLiteral("42"), new NullLiteral()));
        assertJsonRoundTrip(codec, new NullIfExpression(new NullLiteral(), new NullLiteral()));
    }

    @Test
    public void testBetween()
    {
        assertJsonRoundTrip(codec, new BetweenPredicate(new LongLiteral("1"), new LongLiteral("2"), new LongLiteral("3")));
        assertJsonRoundTrip(codec, new NotExpression(new BetweenPredicate(new LongLiteral("1"), new LongLiteral("2"), new LongLiteral("3"))));
    }

    @Test
    public void testRow()
    {
        assertJsonRoundTrip(codec, new Row(ImmutableList.of(new StringLiteral("a"), new LongLiteral("1"), new DoubleLiteral("2.2"))));
    }

    @Test
    public void testLogicalAndArithmeticExpression()
    {
        assertJsonRoundTrip(codec,
                new LogicalExpression(
                        io.trino.sql.tree.LogicalExpression.Operator.AND,
                        ImmutableList.of(
                                new LongLiteral("1"),
                                new LongLiteral("2"),
                                new LongLiteral("3"),
                                new LongLiteral("4"))));

        assertJsonRoundTrip(codec,
                new LogicalExpression(
                        io.trino.sql.tree.LogicalExpression.Operator.OR,
                        ImmutableList.of(
                                new LongLiteral("1"),
                                new LongLiteral("2"),
                                new LongLiteral("3"),
                                new LongLiteral("4"))));

        assertJsonRoundTrip(codec,
                new LogicalExpression(
                        io.trino.sql.tree.LogicalExpression.Operator.OR,
                        ImmutableList.of(
                                new LogicalExpression(
                                        io.trino.sql.tree.LogicalExpression.Operator.AND,
                                        ImmutableList.of(
                                                new LongLiteral("1"),
                                                new LongLiteral("2"),
                                                new LongLiteral("3"))),
                                new LogicalExpression(
                                        io.trino.sql.tree.LogicalExpression.Operator.AND,
                                        ImmutableList.of(
                                                new LongLiteral("4"),
                                                new LongLiteral("5"),
                                                new LongLiteral("6"))),
                                new LogicalExpression(
                                        io.trino.sql.tree.LogicalExpression.Operator.AND,
                                        ImmutableList.of(
                                                new LongLiteral("7"),
                                                new LongLiteral("8"),
                                                new LongLiteral("9"))))));

        assertJsonRoundTrip(codec, LogicalExpression.or(
                LogicalExpression.and(
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertJsonRoundTrip(codec, LogicalExpression.or(
                new LongLiteral("1"),
                LogicalExpression.and(
                        new LongLiteral("2"),
                        new LongLiteral("3"))));

        assertJsonRoundTrip(codec, LogicalExpression.and(
                new NotExpression(new LongLiteral("1")),
                new LongLiteral("2")));

        assertJsonRoundTrip(codec, LogicalExpression.or(
                new NotExpression(new LongLiteral("1")),
                new LongLiteral("2")));

        assertJsonRoundTrip(codec, new ArithmeticBinaryExpression(io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD,
                new LongLiteral("-1"),
                new LongLiteral("2")));

        assertJsonRoundTrip(codec, new ArithmeticBinaryExpression(io.trino.sql.tree.ArithmeticBinaryExpression.Operator.SUBTRACT,
                new ArithmeticBinaryExpression(io.trino.sql.tree.ArithmeticBinaryExpression.Operator.SUBTRACT,
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertJsonRoundTrip(codec, new ArithmeticBinaryExpression(io.trino.sql.tree.ArithmeticBinaryExpression.Operator.DIVIDE,
                new ArithmeticBinaryExpression(io.trino.sql.tree.ArithmeticBinaryExpression.Operator.DIVIDE,
                        new LongLiteral("1"),
                        new LongLiteral("2")),
                new LongLiteral("3")));

        assertJsonRoundTrip(codec, new ArithmeticBinaryExpression(io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD,
                new LongLiteral("1"),
                new ArithmeticBinaryExpression(io.trino.sql.tree.ArithmeticBinaryExpression.Operator.MULTIPLY,
                        new LongLiteral("2"),
                        new LongLiteral("3"))));
    }

    @Test
    public void testInterval()
    {
        assertJsonRoundTrip(codec, new IntervalLiteral("123", io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE, io.trino.sql.tree.IntervalLiteral.IntervalField.YEAR));
        assertJsonRoundTrip(codec, new IntervalLiteral("123-3", io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE, io.trino.sql.tree.IntervalLiteral.IntervalField.YEAR, Optional.of(io.trino.sql.tree.IntervalLiteral.IntervalField.MONTH)));
        assertJsonRoundTrip(codec, new IntervalLiteral("123 23:58:53.456", io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE, io.trino.sql.tree.IntervalLiteral.IntervalField.DAY, Optional.of(io.trino.sql.tree.IntervalLiteral.IntervalField.SECOND)));
        assertJsonRoundTrip(codec, new IntervalLiteral("123", io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE, io.trino.sql.tree.IntervalLiteral.IntervalField.DAY));
        assertJsonRoundTrip(codec, new IntervalLiteral("123", io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE, io.trino.sql.tree.IntervalLiteral.IntervalField.HOUR));
        assertJsonRoundTrip(codec, new IntervalLiteral("123", io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE, io.trino.sql.tree.IntervalLiteral.IntervalField.SECOND));
        assertJsonRoundTrip(codec, new IntervalLiteral("123", io.trino.sql.tree.IntervalLiteral.Sign.POSITIVE, io.trino.sql.tree.IntervalLiteral.IntervalField.MINUTE));
    }

    @Test
    public void testDecimal()
    {
        assertJsonRoundTrip(codec, new DecimalLiteral("12.34"));
        assertJsonRoundTrip(codec, new DecimalLiteral("12."));
        assertJsonRoundTrip(codec, new DecimalLiteral("12"));
        assertJsonRoundTrip(codec, new DecimalLiteral(".34"));
        assertJsonRoundTrip(codec, new DecimalLiteral("+12.34"));
        assertJsonRoundTrip(codec, new DecimalLiteral("+12"));
        assertJsonRoundTrip(codec, new DecimalLiteral("-12"));
        assertJsonRoundTrip(codec, new DecimalLiteral("+.34"));
        assertJsonRoundTrip(codec, new DecimalLiteral("123."));
        assertJsonRoundTrip(codec, new DecimalLiteral("123.0"));
        assertJsonRoundTrip(codec, new DecimalLiteral("123.0"));
        assertJsonRoundTrip(codec, new DecimalLiteral(".5"));
        assertJsonRoundTrip(codec, new DecimalLiteral("123.5"));
    }

    @Test
    public void testTime()
    {
        assertJsonRoundTrip(codec, new TimeLiteral("03:04:05"));
    }

    @Test
    public void testCurrentTimestamp()
    {
        assertJsonRoundTrip(codec, new CurrentTime(io.trino.sql.tree.CurrentTime.Function.TIMESTAMP));
    }

    @Test
    public void testTrim()
    {
        assertJsonRoundTrip(codec,
                new Trim(BOTH, new StringLiteral(" abc "), Optional.empty()));
        assertJsonRoundTrip(codec,
                new Trim(LEADING, new StringLiteral(" abc "), Optional.empty()));
        assertJsonRoundTrip(codec,
                new Trim(TRAILING, new StringLiteral(" abc "), Optional.empty()));
        assertJsonRoundTrip(codec,
                new Trim(TRAILING, new StringLiteral(" abc "), Optional.empty()));
        assertJsonRoundTrip(codec,
                new Trim(LEADING, new StringLiteral(" abc "), Optional.empty()));
        assertJsonRoundTrip(codec,
                new Trim(TRAILING, new StringLiteral(" abc "), Optional.of(new StringLiteral(" "))));
    }

    @Test
    public void testFormat()
    {
        assertJsonRoundTrip(codec, new Format(ImmutableList.of(new StringLiteral("%s"), new StringLiteral("abc"))));
        assertJsonRoundTrip(codec, new Format(ImmutableList.of(new StringLiteral("%d %s"), new LongLiteral("123"), new StringLiteral("x"))));
    }

    @Test
    public void testCase()
    {
        assertJsonRoundTrip(
                codec,
                new SimpleCaseExpression(
                        new IsNullPredicate(new LongLiteral("1")),
                        ImmutableList.of(
                                new WhenClause(
                                        BooleanLiteral.of("true"),
                                        new LongLiteral("2"))),
                        Optional.of(new LongLiteral("3"))));
    }

    @Test
    public void testSearchedCase()
    {
        assertJsonRoundTrip(
                codec,
                new SearchedCaseExpression(
                        ImmutableList.of(
                                new WhenClause(
                                        new ComparisonExpression(io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN, new Identifier("a"), new LongLiteral("3")),
                                        new LongLiteral("23")),
                                new WhenClause(
                                        new ComparisonExpression(io.trino.sql.tree.ComparisonExpression.Operator.EQUAL, new Identifier("b"), new Identifier("a")),
                                        new LongLiteral("33"))),
                        Optional.empty()));
    }

    @Test
    public void testSubstringBuiltInFunction()
    {
        String givenString = "ABCDEF";
        assertJsonRoundTrip(codec,
                        new FunctionCall(QualifiedName.of("substr"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2"))));

        assertJsonRoundTrip(codec,
                new FunctionCall(QualifiedName.of("substr"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2"), new LongLiteral("3"))));

        assertJsonRoundTrip(codec,
                        new FunctionCall(QualifiedName.of("substring"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2"))));

        assertJsonRoundTrip(codec,
                        new FunctionCall(QualifiedName.of("substring"), Lists.newArrayList(new StringLiteral(givenString), new LongLiteral("2"), new LongLiteral("3"))));
    }

    public static <T> void assertJsonRoundTrip(JsonCodec<T> codec, T object)
    {
        String json = codec.toJson(object);
        T copy = codec.fromJson(json);
        assertEquals(copy, object);
    }

    @Test
    public void testAggregationFilter()
    {
        assertJsonRoundTrip(codec,
                        new FunctionCall(
                                QualifiedName.of("SUM"),
                                Optional.empty(),
                                Optional.of(new ComparisonExpression(
                                        io.trino.sql.tree.ComparisonExpression.Operator.GREATER_THAN,
                                        new Identifier("x"),
                                        new LongLiteral("4"))),
                                Optional.empty(),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(new Identifier("x"))));
    }

    @Test
    public void testAggregationWithOrderBy()
    {
        assertJsonRoundTrip(codec,
                new FunctionCall(
                        QualifiedName.of("array_agg"),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.of(new OrderBy(ImmutableList.of(new SortItem(new Identifier("x"), DESCENDING, UNDEFINED)))),
                        false,
                        Optional.empty(),
                        Optional.empty(),
                        ImmutableList.of(new Identifier("x"))));
        assertJsonRoundTrip(codec,
                        new FunctionCall(
                                QualifiedName.of("array_agg"),
                                Optional.empty(),
                                Optional.empty(),
                                Optional.of(new OrderBy(ImmutableList.of(new SortItem(new DereferenceExpression(new Identifier("t"), new Identifier("y")), ASCENDING, UNDEFINED)))),
                                false,
                                Optional.empty(),
                                Optional.empty(),
                                ImmutableList.of(new Identifier("x"))));
    }

    public static <T> String toJson(JsonCodec<T> codec, T object)
    {
        return codec.toJson(object);
    }
}
