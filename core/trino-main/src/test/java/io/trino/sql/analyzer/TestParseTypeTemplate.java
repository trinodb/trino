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

import io.trino.spi.type.NumericExpression;
import io.trino.spi.type.TemplateParameter;
import io.trino.spi.type.TypeDescriptor;
import io.trino.spi.type.TypeTemplate;
import io.trino.spi.type.TypeTemplates;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.TypeParameter.numericParameter;
import static io.trino.sql.analyzer.TypeDescriptorTranslator.parseTypeTemplate;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestParseTypeTemplate
{
    @Test
    void testTypeVariableInArgument()
    {
        TypeTemplate template = parseTypeTemplate("array(E)", Set.of("E"), Set.of());
        assertThat(template).isEqualTo(new TypeTemplate.TypeApplication("array", List.of(
                new TemplateParameter.TypeArgument(Optional.empty(), new TypeTemplate.TypeVariable("E")))));
    }

    @Test
    void testTopLevelTypeVariable()
    {
        assertThat(parseTypeTemplate("E", Set.of("E"), Set.of())).isEqualTo(new TypeTemplate.TypeVariable("E"));
    }

    @Test
    void testNumericVariables()
    {
        TypeTemplate template = parseTypeTemplate("decimal(p, s)", Set.of(), Set.of("p", "s"));
        assertThat(template).isEqualTo(new TypeTemplate.TypeApplication("decimal", List.of(
                new TemplateParameter.NumericArgument(new NumericExpression.Variable("p")),
                new TemplateParameter.NumericArgument(new NumericExpression.Variable("s")))));
    }

    @Test
    void testDatetimePrecisionVariable()
    {
        TypeTemplate template = parseTypeTemplate("timestamp(p)", Set.of(), Set.of("p"));
        assertThat(template).isEqualTo(new TypeTemplate.TypeApplication("timestamp", List.of(
                new TemplateParameter.NumericArgument(new NumericExpression.Variable("p")))));
    }

    @Test
    void testGroundTypeBindsBackToSignature()
    {
        // A variable-free template parses and binds to the same signature TypeDescriptorTranslator would produce.
        TypeTemplate template = parseTypeTemplate("row(a decimal(10,2), b varchar(5))", Set.of(), Set.of());
        TypeDescriptor expected = TypeDescriptorTranslator.parseTypeDescriptor("row(a decimal(10,2), b varchar(5))");
        assertThat(TypeTemplates.bind(template, Map.of(), Map.of())).isEqualTo(expected);
    }

    @Test
    void testBindNumericTemplate()
    {
        TypeTemplate template = parseTypeTemplate("decimal(p, s)", Set.of(), Set.of("p", "s"));
        assertThat(TypeTemplates.bind(template, Map.of(), Map.of("p", 10L, "s", 2L)))
                .isEqualTo(new TypeDescriptor("decimal", List.of(numericParameter(10), numericParameter(2))));
    }

    @Test
    void testConcreteBaseTypeUnknownToVariableSets()
    {
        // A name in neither set is a concrete type, not a variable.
        TypeTemplate template = parseTypeTemplate("array(bigint)", Set.of(), Set.of());
        assertThat(template).isEqualTo(new TypeTemplate.TypeApplication("array", List.of(
                new TemplateParameter.TypeArgument(Optional.empty(), new TypeTemplate.TypeApplication("bigint", List.of())))));
    }

    @Test
    void testRejectsNameInBothVariableSets()
    {
        assertThatThrownBy(() -> parseTypeTemplate("array(E)", Set.of("E"), Set.of("E")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("cannot be both a type variable and a numeric variable");
    }

    @Test
    void testRejectsNumericVariableInTypePosition()
    {
        assertThatThrownBy(() -> parseTypeTemplate("n", Set.of(), Set.of("n")))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Numeric variable cannot appear in a type position");
    }
}
