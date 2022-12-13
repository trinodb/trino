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
package io.trino.sql.planner;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.type.Type;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.ArithmeticUnaryExpression;
import io.trino.sql.tree.Cast;
import io.trino.sql.tree.ComparisonExpression;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import io.trino.sql.tree.GenericLiteral;
import io.trino.sql.tree.Identifier;
import io.trino.sql.tree.InPredicate;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.LongLiteral;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.SymbolReference;
import io.trino.sql.tree.TryExpression;
import org.testng.annotations.Test;

import java.util.List;

import static io.trino.spi.type.RealType.REAL;
import static io.trino.sql.analyzer.TypeSignatureTranslator.toSqlType;
import static io.trino.sql.planner.SafeExpressionEvaluator.isSafeExpression;
import static io.trino.sql.tree.ArithmeticUnaryExpression.Sign.MINUS;
import static io.trino.sql.tree.ComparisonExpression.Operator.EQUAL;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestSafeExpressionEvaluator
{
    private static final TestingFunctionResolution functionResolution = new TestingFunctionResolution();

    @Test
    public void testSanity()
    {
        assertTrue(isSafeExpression(new ComparisonExpression(EQUAL, new LongLiteral("1"), new LongLiteral("2"))));
        assertTrue(isSafeExpression(new InPredicate(new SymbolReference("value"), new SymbolReference("element"))));

        assertFalse(isSafeExpression(new Cast(new GenericLiteral("CHAR", "a"), toSqlType(REAL))));
        assertFalse(isSafeExpression(new ArithmeticUnaryExpression(MINUS, new SymbolReference("c1"))));
        assertFalse(isSafeExpression(new ArithmeticBinaryExpression(ArithmeticBinaryExpression.Operator.ADD, new LongLiteral("1"), new LongLiteral("2"))));
        assertFalse(isSafeExpression(new TryExpression(new LongLiteral("1"))));
        assertFalse(isSafeExpression(new LambdaExpression(ImmutableList.of(), new Identifier("x"))));
        assertFalse(isSafeExpression(function("typeof", ImmutableList.of(REAL), ImmutableList.of(function("rand")))));

        assertFalse(isSafeExpression(new ComparisonExpression(EQUAL, new ArithmeticUnaryExpression(MINUS, new SymbolReference("c1")), new LongLiteral("2"))));
    }

    private FunctionCall function(String name)
    {
        return function(name, ImmutableList.of(), ImmutableList.of());
    }

    private FunctionCall function(String name, List<Type> types, List<Expression> arguments)
    {
        return functionResolution
                .functionCallBuilder(QualifiedName.of(name))
                .setArguments(types, arguments)
                .build();
    }
}
