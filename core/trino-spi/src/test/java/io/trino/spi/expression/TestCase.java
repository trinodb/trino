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
package io.trino.spi.expression;

import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestCase
{
    private static final Variable CONDITION = new Variable("condition", BOOLEAN);
    private static final Variable RESULT = new Variable("result", BIGINT);
    private static final Variable DEFAULT_VALUE = new Variable("default", BIGINT);

    @Test
    public void testValidCase()
    {
        Case.WhenClause whenClause = new Case.WhenClause(CONDITION, RESULT);
        Case caseExpression = new Case(BIGINT, List.of(whenClause), DEFAULT_VALUE);

        assertThat(caseExpression.getType()).isEqualTo(BIGINT);
        assertThat(caseExpression.getWhenClauses()).containsExactly(whenClause);
        assertThat(caseExpression.getDefaultValue()).isEqualTo(DEFAULT_VALUE);
        assertThat(caseExpression.getChildren()).isEqualTo(List.of(CONDITION, RESULT, DEFAULT_VALUE));
        assertThat(caseExpression).hasToString(
                "Case[whenClauses=[WhenClause[condition=condition::boolean, result=result::bigint]], defaultValue=default::bigint]");
    }

    @Test
    public void testMultipleWhenClauses()
    {
        Case.WhenClause first = new Case.WhenClause(CONDITION, RESULT);
        Case.WhenClause second = new Case.WhenClause(new Variable("other_condition", BOOLEAN), new Variable("other_result", BIGINT));
        Case caseExpression = new Case(BIGINT, List.of(first, second), DEFAULT_VALUE);

        assertThat(caseExpression.getWhenClauses()).containsExactly(first, second);
        assertThat(caseExpression.getChildren()).isEqualTo(List.of(
                first.getCondition(),
                first.getResult(),
                second.getCondition(),
                second.getResult(),
                DEFAULT_VALUE));
    }

    @Test
    public void testConstructorValidation()
    {
        assertThatThrownBy(() -> new Case(BIGINT, List.of(), DEFAULT_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("whenClauses is empty");

        assertThatThrownBy(() -> new Case(BIGINT, List.of(new Case.WhenClause(RESULT, RESULT)), DEFAULT_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("condition must be boolean");

        assertThatThrownBy(() -> new Case(BIGINT, List.of(new Case.WhenClause(CONDITION, CONDITION)), DEFAULT_VALUE))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("result type does not match expression type");

        assertThatThrownBy(() -> new Case(BIGINT, List.of(new Case.WhenClause(CONDITION, RESULT)), CONDITION))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("default value type does not match expression type");
    }
}
