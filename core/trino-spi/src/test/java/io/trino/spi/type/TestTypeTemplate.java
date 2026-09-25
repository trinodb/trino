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
package io.trino.spi.type;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.spi.type.NumericExpression.Operator.ADD;
import static io.trino.spi.type.NumericExpression.Operator.DIVIDE;
import static io.trino.spi.type.NumericExpression.Operator.MAX;
import static io.trino.spi.type.NumericExpression.Operator.MIN;
import static io.trino.spi.type.NumericExpression.Operator.MULTIPLY;
import static io.trino.spi.type.TypeParameter.numericParameter;
import static io.trino.spi.type.TypeParameter.typeParameter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestTypeTemplate
{
    @Test
    void testCaseInsensitiveEquality()
    {
        // Variable references and type names match their declarations regardless of case
        assertThat(new TypeTemplate.TypeVariable("E")).isEqualTo(new TypeTemplate.TypeVariable("e"));
        assertThat(new TypeTemplate.TypeVariable("E").hashCode()).isEqualTo(new TypeTemplate.TypeVariable("e").hashCode());
        assertThat(new TypeTemplate.TypeApplication("ARRAY", List.of(new TemplateParameter.TypeArgument(Optional.empty(), new TypeTemplate.TypeVariable("E")))))
                .isEqualTo(new TypeTemplate.TypeApplication("array", List.of(new TemplateParameter.TypeArgument(Optional.empty(), new TypeTemplate.TypeVariable("e")))));
        assertThat(new TypeTemplate.TypeVariable("E")).isNotEqualTo(new TypeTemplate.TypeVariable("F"));
    }

    @Test
    void testRenderInternalForm()
    {
        // render() produces the internal base(arg, …) IR form, never the user-visible SQL surface (the
        // SQL spelling — unbounded varchar elision, time-zone word order — is a separate presentation
        // layer applied above this). The IR keeps the sentinel length, the trailing parameter, and the
        // opaque internal base names (`$timestamp_tz`, not `timestamp with time zone`).
        assertThat(TypeTemplates.type("varchar", new NumericExpression.Literal(2147483647)).render()).isEqualTo("varchar(2147483647)");
        assertThat(TypeTemplates.type(StandardTypes.TIMESTAMP_WITH_TIME_ZONE, new NumericExpression.Literal(3)).render()).isEqualTo("$timestamp_tz(3)");
        assertThat(TypeTemplates.type(StandardTypes.TIME_WITH_TIME_ZONE, new NumericExpression.Literal(3)).render()).isEqualTo("$time_tz(3)");
        assertThat(new TypeTemplate.TypeApplication(
                "row",
                List.of(new TemplateParameter.TypeArgument(Optional.of("a\"b"), new TypeTemplate.TypeApplication("bigint", List.of())))).render())
                .isEqualTo("row(\"a\"\"b\" bigint)");

        // an interval renders generically too — its fields and precisions are just numeric parameters;
        // the SQL qualifier spelling (`interval day(9) to second`) is the surface layer's job
        assertThat(TypeTemplates.type("interval day to second", new NumericExpression.Literal(2), new NumericExpression.Literal(5), new NumericExpression.Literal(9)).render())
                .isEqualTo("interval day to second(2,5,9)");
    }

    @Test
    void testEvaluateNumericExpression()
    {
        // min(38, p + 1)
        NumericExpression expression = new NumericExpression.Operation(
                MIN,
                new NumericExpression.Literal(38),
                new NumericExpression.Operation(ADD, new NumericExpression.Variable("p"), new NumericExpression.Literal(1)));
        assertThat(NumericExpressions.evaluate(expression, Map.of("p", 10L)).longValueExact()).isEqualTo(11L);
        assertThat(NumericExpressions.evaluate(expression, Map.of("p", 40L)).longValueExact()).isEqualTo(38L);
    }

    @Test
    void testEvaluateClampsLargeIntermediateWithoutOverflow()
    {
        // min(2147483647, x + max(x * y / 2, y) * (x + 1)) — the regexp_replace result-length formula.
        // For an unbounded varchar (x = y = Integer.MAX_VALUE) the intermediate exceeds long range, so it
        // must be computed exactly and clamped, not wrapped.
        NumericExpression x = new NumericExpression.Variable("x");
        NumericExpression y = new NumericExpression.Variable("y");
        NumericExpression expression = new NumericExpression.Operation(
                MIN,
                new NumericExpression.Literal(2147483647),
                new NumericExpression.Operation(
                        ADD,
                        x,
                        new NumericExpression.Operation(
                                MULTIPLY,
                                new NumericExpression.Operation(
                                        MAX,
                                        new NumericExpression.Operation(DIVIDE, new NumericExpression.Operation(MULTIPLY, x, y), new NumericExpression.Literal(2)),
                                        y),
                                new NumericExpression.Operation(ADD, x, new NumericExpression.Literal(1)))));
        assertThat(NumericExpressions.evaluate(expression, Map.of("x", (long) Integer.MAX_VALUE, "y", (long) Integer.MAX_VALUE)).longValueExact()).isEqualTo(2147483647L);
    }

    @Test
    void testBindCalculatedTemplate()
    {
        // decimal(min(38, p + 1), s)
        TypeTemplate template = new TypeTemplate.TypeApplication("decimal", List.of(
                new TemplateParameter.NumericArgument(new NumericExpression.Operation(
                        MIN,
                        new NumericExpression.Literal(38),
                        new NumericExpression.Operation(ADD, new NumericExpression.Variable("p"), new NumericExpression.Literal(1)))),
                new TemplateParameter.NumericArgument(new NumericExpression.Variable("s"))));

        TypeDescriptor bound = TypeTemplates.bind(template, Map.of(), Map.of("p", 10L, "s", 2L));
        assertThat(bound).isEqualTo(new TypeDescriptor("decimal", List.of(numericParameter(11), numericParameter(2))));
    }

    @Test
    void testBindTypeVariable()
    {
        // array(E)
        TypeTemplate template = new TypeTemplate.TypeApplication("array", List.of(
                new TemplateParameter.TypeArgument(Optional.empty(), new TypeTemplate.TypeVariable("E"))));

        TypeDescriptor bound = TypeTemplates.bind(template, Map.of("E", BigintType.BIGINT.getTypeDescriptor()), Map.of());
        assertThat(bound).isEqualTo(new TypeDescriptor("array", List.of(typeParameter(BigintType.BIGINT.getTypeDescriptor()))));
    }

    @Test
    void testLiftRoundTrip()
    {
        // array(decimal(10, 2)) lifts to a variable-free template and binds back to itself
        TypeDescriptor signature = new TypeDescriptor("array", List.of(typeParameter(
                new TypeDescriptor("decimal", List.of(numericParameter(10), numericParameter(2))))));

        TypeTemplate template = TypeTemplates.fromTypeDescriptor(signature);
        assertThat(TypeTemplates.bind(template, Map.of(), Map.of())).isEqualTo(signature);
    }

    @Test
    void testMissingBindings()
    {
        assertThatThrownBy(() -> TypeTemplates.bind(new TypeTemplate.TypeVariable("E"), Map.of(), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No binding for type variable E");

        assertThatThrownBy(() -> NumericExpressions.evaluate(new NumericExpression.Variable("p"), Map.of()))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("No binding for numeric variable p");
    }
}
