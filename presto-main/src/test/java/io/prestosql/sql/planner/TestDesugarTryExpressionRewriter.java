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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import io.prestosql.operator.scalar.TryFunction;
import io.prestosql.sql.planner.iterative.rule.test.BaseRuleTest;
import io.prestosql.sql.tree.ArithmeticBinaryExpression;
import io.prestosql.sql.tree.DecimalLiteral;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.TryExpression;
import io.prestosql.type.FunctionType;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.DecimalType.createDecimalType;
import static io.prestosql.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static org.testng.Assert.assertEquals;

public class TestDesugarTryExpressionRewriter
        extends BaseRuleTest
{
    @Test
    public void testTryExpressionDesugaringRewriter()
    {
        // 1 + try(2)
        Expression before = new ArithmeticBinaryExpression(
                ADD,
                new DecimalLiteral("1"),
                new TryExpression(new DecimalLiteral("2")));

        // 1 + try_function(() -> 2)
        Expression after = new ArithmeticBinaryExpression(
                ADD,
                new DecimalLiteral("1"),
                new FunctionCallBuilder(tester().getMetadata())
                        .setName(QualifiedName.of(TryFunction.NAME))
                        .addArgument(new FunctionType(ImmutableList.of(), createDecimalType(1)), new LambdaExpression(ImmutableList.of(), new DecimalLiteral("2")))
                        .build());

        assertEquals(DesugarTryExpressionRewriter.rewrite(
                before,
                tester().getMetadata(),
                tester().getTypeAnalyzer(),
                tester().getSession(),
                new SymbolAllocator()),
                after);
    }
}
