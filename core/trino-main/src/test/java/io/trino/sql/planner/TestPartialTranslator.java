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
import io.airlift.slice.Slices;
import io.trino.Session;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.RowType;
import io.trino.sql.ir.Call;
import io.trino.sql.ir.Cast;
import io.trino.sql.ir.Constant;
import io.trino.sql.ir.Expression;
import io.trino.sql.ir.FieldReference;
import io.trino.sql.ir.NodeRef;
import io.trino.sql.ir.Reference;
import io.trino.transaction.TransactionId;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.trino.sql.planner.ConnectorExpressionTranslator.translate;
import static io.trino.sql.planner.PartialTranslator.extractPartialTranslations;
import static io.trino.sql.planner.TestingPlannerContext.PLANNER_CONTEXT;
import static io.trino.testing.TestingSession.testSessionBuilder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestPartialTranslator
{
    private static final TestingFunctionResolution FUNCTIONS = new TestingFunctionResolution();
    private static final ResolvedFunction ADD_INTEGER = FUNCTIONS.resolveOperator(OperatorType.ADD, ImmutableList.of(INTEGER, INTEGER));

    private static final Session TEST_SESSION = testSessionBuilder()
            .setTransactionId(TransactionId.create())
            .build();

    @Test
    public void testPartialTranslator()
    {
        Expression rowSymbolReference = new Reference(RowType.anonymousRow(INTEGER, INTEGER), "row_symbol_1");
        Expression dereferenceExpression1 = new FieldReference(rowSymbolReference, 0);
        Expression dereferenceExpression2 = new FieldReference(rowSymbolReference, 1);
        Expression stringLiteral = new Constant(VARCHAR, Slices.utf8Slice("abcd"));
        Expression symbolReference1 = new Reference(INTEGER, "double_symbol_1");

        assertFullTranslation(symbolReference1);
        assertFullTranslation(dereferenceExpression1);
        assertFullTranslation(stringLiteral);
        assertFullTranslation(new Call(ADD_INTEGER, ImmutableList.of(symbolReference1, dereferenceExpression1)));

        Expression functionCallExpression = new Call(
                PLANNER_CONTEXT.getMetadata().resolveBuiltinFunction("concat", fromTypes(VARCHAR, VARCHAR)),
                ImmutableList.of(stringLiteral, new Cast(dereferenceExpression2, VARCHAR)));
        assertFullTranslation(functionCallExpression);
    }

    private void assertFullTranslation(Expression expression)
    {
        Map<NodeRef<Expression>, ConnectorExpression> translation = extractPartialTranslations(expression, TEST_SESSION);
        assertThat(getOnlyElement(translation.keySet())).isEqualTo(NodeRef.of(expression));
        assertThat(getOnlyElement(translation.values())).isEqualTo(translate(TEST_SESSION, expression).get());
    }
}
