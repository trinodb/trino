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
package io.trino.plugin.base.util;

import com.google.common.collect.ImmutableList;
import io.trino.spi.expression.Call;
import io.trino.spi.expression.ConnectorExpression;
import io.trino.spi.expression.Variable;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.plugin.base.expression.ConnectorExpressions.extractDisjuncts;
import static io.trino.plugin.base.expression.ConnectorExpressions.or;
import static io.trino.spi.expression.Constant.FALSE;
import static io.trino.spi.expression.Constant.TRUE;
import static io.trino.spi.expression.StandardFunctions.OR_FUNCTION_NAME;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;

public class TestConnectorExpressionUtil
{
    private static final ConnectorExpression A = new Variable("a", BOOLEAN);
    private static final ConnectorExpression B = new Variable("b", BOOLEAN);
    private static final ConnectorExpression C = new Variable("c", BOOLEAN);

    @Test
    public void testExtractDisjunctsSingleExpression()
    {
        assertThat(extractDisjuncts(A)).containsExactly(A);
    }

    @Test
    public void testExtractDisjunctsOrExpression()
    {
        ConnectorExpression orExpression = new Call(BOOLEAN, OR_FUNCTION_NAME, ImmutableList.of(A, B));
        assertThat(extractDisjuncts(orExpression)).containsExactly(A, B);
    }

    @Test
    public void testExtractDisjunctsNestedOrExpression()
    {
        ConnectorExpression innerOr = new Call(BOOLEAN, OR_FUNCTION_NAME, ImmutableList.of(A, B));
        ConnectorExpression outerOr = new Call(BOOLEAN, OR_FUNCTION_NAME, ImmutableList.of(innerOr, C));
        assertThat(extractDisjuncts(outerOr)).containsExactly(A, B, C);
    }

    @Test
    public void testExtractDisjunctsSkipsFalse()
    {
        assertThat(extractDisjuncts(FALSE)).isEmpty();
    }

    @Test
    public void testExtractDisjunctsOrWithFalse()
    {
        ConnectorExpression orExpression = new Call(BOOLEAN, OR_FUNCTION_NAME, ImmutableList.of(A, FALSE));
        assertThat(extractDisjuncts(orExpression)).containsExactly(A);
    }

    @Test
    public void testOrMultipleExpressions()
    {
        ConnectorExpression result = or(A, B);
        assertThat(result).isInstanceOf(Call.class);
        Call call = (Call) result;
        assertThat(call.getFunctionName()).isEqualTo(OR_FUNCTION_NAME);
        assertThat(call.getArguments()).containsExactly(A, B);
    }

    @Test
    public void testOrSingleExpression()
    {
        assertThat(or(A)).isEqualTo(A);
    }

    @Test
    public void testOrEmptyList()
    {
        assertThat(or(List.of())).isEqualTo(FALSE);
    }

    @Test
    public void testOrFiltersFalse()
    {
        assertThat(or(A, FALSE)).isEqualTo(A);
    }

    @Test
    public void testOrAllFalse()
    {
        assertThat(or(FALSE, FALSE)).isEqualTo(FALSE);
    }

    @Test
    public void testOrPreservesTrueAsRegularDisjunct()
    {
        ConnectorExpression result = or(A, TRUE);
        assertThat(result).isInstanceOf(Call.class);
        Call call = (Call) result;
        assertThat(call.getArguments()).containsExactly(A, TRUE);
    }

    @Test
    public void testOrVarargsList()
    {
        ConnectorExpression result = or(A, B, C);
        assertThat(result).isInstanceOf(Call.class);
        Call call = (Call) result;
        assertThat(call.getFunctionName()).isEqualTo(OR_FUNCTION_NAME);
        assertThat(call.getArguments()).containsExactly(A, B, C);
    }
}
