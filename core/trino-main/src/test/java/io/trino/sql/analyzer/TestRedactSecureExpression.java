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
package io.trino.sql.analyzer;

import com.google.common.collect.ImmutableMap;
import io.trino.metadata.ResolvedFunction;
import io.trino.metadata.TestingFunctionResolution;
import io.trino.spi.eventlistener.RoutineInfo;
import io.trino.sql.ir.SecureExpression;
import io.trino.sql.parser.SqlParser;
import io.trino.sql.tree.Expression;
import io.trino.sql.tree.FunctionCall;
import org.junit.jupiter.api.Test;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.sql.analyzer.TypeDescriptorProvider.fromTypes;
import static io.trino.sql.util.AstUtils.preOrder;
import static org.assertj.core.api.Assertions.assertThat;

public class TestRedactSecureExpression
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    @Test
    public void testSecureExpressionIsRedacted()
    {
        Analysis analysis = newAnalysis();
        Expression secured = SQL_PARSER.createExpression("orderkey < 10");
        analysis.markSecureExpression(secured);

        assertThat(analysis.isSecureExpression(secured)).isTrue();
        assertThat(analysis.redactSecureExpression(secured)).isEqualTo(SecureExpression.REDACTED);
    }

    @Test
    public void testMarkingUsesExpressionIdentity()
    {
        Analysis analysis = newAnalysis();
        Expression secured = SQL_PARSER.createExpression("orderkey < 10");
        Expression equalButDistinct = SQL_PARSER.createExpression("orderkey < 10");
        analysis.markSecureExpression(secured);

        assertThat(analysis.isSecureExpression(equalButDistinct)).isFalse();
        assertThat(analysis.redactSecureExpression(equalButDistinct)).isEqualTo(equalButDistinct.toString());
    }

    @Test
    public void testRoutinesOmitFunctionsOfSecureExpressions()
    {
        Analysis analysis = newAnalysis();
        ResolvedFunction abs = new TestingFunctionResolution().resolveFunction("abs", fromTypes(BIGINT));
        Expression secured = SQL_PARSER.createExpression("abs(orderkey) < 10");
        Expression plain = SQL_PARSER.createExpression("abs(custkey) < 10");

        analysis.markSecureExpression(secured);
        analysis.addResolvedFunction(functionCall(secured), abs, "policy-owner");
        analysis.addResolvedFunction(functionCall(plain), abs, "user");

        // the policy's function must not be reported, the user's own call still is
        assertThat(analysis.getRoutines())
                .extracting(RoutineInfo::getAuthorization)
                .containsExactly("user");
    }

    private static FunctionCall functionCall(Expression expression)
    {
        return (FunctionCall) preOrder(expression)
                .filter(FunctionCall.class::isInstance)
                .findFirst()
                .orElseThrow();
    }

    private static Analysis newAnalysis()
    {
        return new Analysis(null, ImmutableMap.of(), QueryType.OTHERS);
    }
}
