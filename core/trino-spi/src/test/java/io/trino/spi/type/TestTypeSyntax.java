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
import java.util.Optional;

import static io.trino.spi.type.TypeParameter.numericParameter;
import static io.trino.spi.type.TypeParameter.typeParameter;
import static org.assertj.core.api.Assertions.assertThat;

class TestTypeSyntax
{
    @Test
    void testGroundTypeSurface()
    {
        // The generic shape is base(parameters), matching the internal IR.
        assertThat(TypeSyntax.toSql(new TypeDescriptor("decimal", List.of(numericParameter(10), numericParameter(2))))).isEqualTo("decimal(10,2)");
        // An unbounded varchar elides its sentinel length; a bounded one keeps it.
        assertThat(TypeSyntax.toSql(new TypeDescriptor("varchar", List.of(numericParameter(VarcharType.UNBOUNDED_LENGTH))))).isEqualTo("varchar");
        assertThat(TypeSyntax.toSql(new TypeDescriptor("varchar", List.of(numericParameter(10))))).isEqualTo("varchar(10)");
        // The opaque internal base of a time-zone type is mapped to its SQL spelling, with the time-zone
        // qualifier as a trailing phrase around the precision, not a parameter.
        assertThat(TypeSyntax.toSql(new TypeDescriptor(StandardTypes.TIMESTAMP_WITH_TIME_ZONE, List.of(numericParameter(3))))).isEqualTo("timestamp(3) with time zone");
        assertThat(TypeSyntax.toSql(new TypeDescriptor(StandardTypes.TIME_WITH_TIME_ZONE, List.of(numericParameter(3))))).isEqualTo("time(3) with time zone");
    }

    @Test
    void testNestedTypeSurface()
    {
        // The SQL surface recurses into parameters, so the elision applies inside a container too.
        assertThat(TypeSyntax.toSql(new TypeDescriptor("array", List.of(typeParameter(
                new TypeDescriptor("varchar", List.of(numericParameter(VarcharType.UNBOUNDED_LENGTH))))))))
                .isEqualTo("array(varchar)");
        // A named row field is rendered with its quoted name; embedded quotes are doubled.
        assertThat(TypeSyntax.toSql(new TypeDescriptor("row", List.of(
                new TypeParameter.Type(Optional.of("a\"b"), new TypeDescriptor("bigint", List.of()))))))
                .isEqualTo("row(\"a\"\"b\" bigint)");
    }

    @Test
    void testIntervalSurface()
    {
        // An interval's fields render as an SQL qualifier built from the start field, end field, and
        // leading precision — not the generic interval day to second(2,5,9) parameter list.
        assertThat(TypeSyntax.toSql(TypeTemplates.type(StandardTypes.INTERVAL_DAY_TO_SECOND, new NumericExpression.Literal(2), new NumericExpression.Literal(5), new NumericExpression.Literal(9))))
                .isEqualTo("interval day(9) to second");
        assertThat(TypeSyntax.toSql(TypeTemplates.type(StandardTypes.INTERVAL_DAY_TO_SECOND, new NumericExpression.Literal(5), new NumericExpression.Literal(5), new NumericExpression.Literal(13))))
                .isEqualTo("interval second(13)");
        assertThat(TypeSyntax.toSql(TypeTemplates.type(StandardTypes.INTERVAL_YEAR_TO_MONTH, new NumericExpression.Literal(0), new NumericExpression.Literal(1), new NumericExpression.Literal(2))))
                .isEqualTo("interval year(2) to month");
        // A variable precision renders symbolically, like decimal(p, s).
        assertThat(TypeSyntax.toSql(TypeTemplates.type(StandardTypes.INTERVAL_DAY_TO_SECOND, new NumericExpression.Literal(2), new NumericExpression.Literal(5), new NumericExpression.Variable("p"))))
                .isEqualTo("interval day(p) to second");
        // Variable fields — a signature generic over every interval qualifier, as in the interval-field
        // accessor functions — have no SQL field form, so they render the canonical full qualifier rather
        // than leaking the internal base token.
        assertThat(TypeSyntax.toSql(TypeTemplates.type(StandardTypes.INTERVAL_DAY_TO_SECOND, new NumericExpression.Variable("start"), new NumericExpression.Variable("end"), new NumericExpression.Variable("precision"), new NumericExpression.Variable("fractional"))))
                .isEqualTo("interval day to second");
        assertThat(TypeSyntax.toSql(TypeTemplates.type(StandardTypes.INTERVAL_YEAR_TO_MONTH, new NumericExpression.Variable("start"), new NumericExpression.Variable("end"), new NumericExpression.Variable("precision"))))
                .isEqualTo("interval year to month");
    }

    @Test
    void testOpenTypeSurface()
    {
        // A bare type variable renders as its name.
        assertThat(TypeSyntax.toSql(new TypeTemplate.TypeVariable("E"))).isEqualTo("E");
        // An open type renders the same SQL surface as a ground one, including the varchar elision and
        // the time-zone word order, with numeric arguments rendered symbolically.
        assertThat(TypeSyntax.toSql(TypeTemplates.type("varchar", new NumericExpression.Literal(VarcharType.UNBOUNDED_LENGTH)))).isEqualTo("varchar");
        assertThat(TypeSyntax.toSql(TypeTemplates.type(StandardTypes.TIMESTAMP_WITH_TIME_ZONE, new NumericExpression.Variable("p")))).isEqualTo("timestamp(p) with time zone");
    }
}
