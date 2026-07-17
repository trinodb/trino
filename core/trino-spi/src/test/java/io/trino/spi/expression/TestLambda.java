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

import io.trino.spi.type.FunctionType;
import io.trino.spi.type.Type;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestLambda
{
    private static final Type BOOLEAN_TO_BOOLEAN = new FunctionType(List.of(BOOLEAN), BOOLEAN);
    private static final Type BOOLEAN_TO_BIGINT = new FunctionType(List.of(BOOLEAN), BIGINT);
    private static final Type BIGINT_TO_BIGINT = new FunctionType(List.of(BIGINT), BIGINT);
    private static final Type BOOLEAN_AND_BIGINT_TO_BOOLEAN = new FunctionType(List.of(BOOLEAN, BIGINT), BOOLEAN);
    private static final Type BOOLEAN_TO_BIGINT_FUNCTION = new FunctionType(List.of(BOOLEAN), BIGINT_TO_BIGINT);

    @Test
    public void testValidLambda()
    {
        Variable argument = new Variable("value", BOOLEAN);
        Lambda lambda = new Lambda(BOOLEAN_TO_BOOLEAN, List.of(argument), argument);

        assertThat(lambda.getArguments()).containsExactly(argument);
        assertThat(lambda.getBody()).isEqualTo(argument);
        assertThat(lambda.getChildren()).isEmpty();
        assertThat(lambda).hasToString("Lambda[arguments=[value::boolean], body=value::boolean]");
    }

    @Test
    public void testValidNestedLambda()
    {
        Variable argument = new Variable("value", BOOLEAN);
        Variable nestedArgument = new Variable("value", BIGINT);
        Lambda nestedLambda = new Lambda(BIGINT_TO_BIGINT, List.of(nestedArgument), nestedArgument);
        Lambda lambda = new Lambda(BOOLEAN_TO_BIGINT_FUNCTION, List.of(argument), nestedLambda);

        assertThat(nestedLambda.getArguments()).containsExactly(nestedArgument);
        assertThat(nestedLambda.getBody()).isEqualTo(nestedArgument);
        assertThat(nestedLambda.getChildren()).isEmpty();
        assertThat(nestedLambda).hasToString("Lambda[arguments=[value::bigint], body=value::bigint]");

        assertThat(lambda.getArguments()).containsExactly(argument);
        assertThat(lambda.getBody()).isEqualTo(nestedLambda);
        assertThat(lambda.getChildren()).isEmpty();
        assertThat(lambda).hasToString("Lambda[arguments=[value::boolean], body=Lambda[arguments=[value::bigint], body=value::bigint]]");
    }

    @Test
    public void testValidLambdaWithMultipleArguments()
    {
        Variable first = new Variable("first", BOOLEAN);
        Variable second = new Variable("second", BIGINT);
        Lambda lambda = new Lambda(BOOLEAN_AND_BIGINT_TO_BOOLEAN, List.of(first, second), first);

        assertThat(lambda.getArguments()).containsExactly(first, second);
        assertThat(lambda.getBody()).isEqualTo(first);
        assertThat(lambda.getChildren()).isEmpty();
        assertThat(lambda).hasToString("Lambda[arguments=[first::boolean, second::bigint], body=first::boolean]");
    }

    @Test
    public void testConstructorValidation()
    {
        Variable booleanArgument = new Variable("value", BOOLEAN);
        Variable bigintArgument = new Variable("value", BIGINT);

        assertThatThrownBy(() -> new Lambda(BOOLEAN, List.of(booleanArgument), booleanArgument))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessage("type must be a function type: boolean");

        Type wrongArgumentCountType = new FunctionType(List.of(BOOLEAN, BOOLEAN), BOOLEAN);
        assertThatThrownBy(() -> new Lambda(wrongArgumentCountType, List.of(booleanArgument), booleanArgument))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type argument count does not match lambda arguments");

        Type wrongArgumentType = new FunctionType(List.of(BIGINT), BOOLEAN);
        assertThatThrownBy(() -> new Lambda(wrongArgumentType, List.of(booleanArgument), booleanArgument))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type argument types do not match lambda arguments");

        assertThatThrownBy(() -> new Lambda(BOOLEAN_TO_BOOLEAN, List.of(booleanArgument), bigintArgument))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("type return type does not match lambda body");

        assertThatThrownBy(() -> new Lambda(BOOLEAN_TO_BIGINT, List.of(booleanArgument), bigintArgument))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lambda argument reference type does not match argument declaration");

        assertThatThrownBy(() -> new Lambda(wrongArgumentCountType, List.of(booleanArgument, booleanArgument), booleanArgument))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("lambda argument names must be unique");
    }
}
