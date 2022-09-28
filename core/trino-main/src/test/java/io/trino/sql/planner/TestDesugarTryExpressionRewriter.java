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
import io.trino.operator.scalar.TryFunction;
import io.trino.sql.planner.assertions.ExpressionVerifier;
import io.trino.sql.planner.assertions.SymbolAliases;
import io.trino.sql.planner.iterative.rule.test.BaseRuleTest;
import io.trino.sql.tree.ArithmeticBinaryExpression;
import io.trino.sql.tree.DecimalLiteral;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.LambdaExpression;
import io.trino.sql.tree.QualifiedName;
import io.trino.sql.tree.TryExpression;
import io.trino.type.FunctionType;
import org.testng.annotations.Test;

import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.sql.tree.ArithmeticBinaryExpression.Operator.ADD;
import static org.testng.Assert.assertTrue;

public class TestDesugarTryExpressionRewriter
        extends BaseRuleTest
{
    @Test
    public void testTryExpressionDesugaringRewriter()
    {
        // 1 + try(2)
        Expression initial = new ArithmeticBinaryExpression(
                ADD,
                new DecimalLiteral("1"),
                new TryExpression(new DecimalLiteral("2")));
        Expression rewritten = DesugarTryExpressionRewriter.rewrite(
                initial,
                tester().getMetadata(),
                tester().getTypeAnalyzer(),
                tester().getSession(),
                new SymbolAllocator());

        // 1 + try_function(() -> 2)
        Expression expected = new ArithmeticBinaryExpression(
                ADD,
                new DecimalLiteral("1"),
                new TestingFunctionResolution()
                        .functionCallBuilder(QualifiedName.of(TryFunction.NAME))
                        .addArgument(new FunctionType(ImmutableList.of(), createDecimalType(1)), new LambdaExpression(ImmutableList.of(), new DecimalLiteral("2")))
                        .build());

        ExpressionVerifier verifier = new ExpressionVerifier(new SymbolAliases());
        assertTrue(verifier.process(rewritten, expected));
    }
}
