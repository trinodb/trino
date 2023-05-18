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
package io.trino.plugin.jdbc.expression;

import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

public class TestExpressionMappingParser
{
    private static final Map<String, Set<String>> TYPE_CLASSES = Map.of("integer_class", Set.of("integer", "bigint"));

    @Test
    public void testCapture()
    {
        assertExpressionPattern("b", new ExpressionCapture("b"));

        assertExpressionPattern(
                "b: bigint",
                new ExpressionCapture(
                        "b",
                        type("bigint")));

        assertExpressionPattern(
                "bar: varchar(n)",
                new ExpressionCapture(
                        "bar",
                        type("varchar", parameter("n"))));
    }

    @Test
    public void testParameterizedType()
    {
        assertExpressionPattern(
                "bar: varchar(3)",
                new ExpressionCapture(
                        "bar",
                        type("varchar", parameter(3L))));
    }

    @Test
    public void testCallPattern()
    {
        assertExpressionPattern(
                "is_null(a)",
                new CallPattern(
                        "is_null",
                        List.of(new ExpressionCapture("a")),
                        Optional.empty()));

        assertExpressionPattern(
                "$like(a: varchar(n), b: varchar(m))",
                new CallPattern(
                        "$like",
                        List.of(
                                new ExpressionCapture("a", type("varchar", parameter("n"))),
                                new ExpressionCapture("b", type("varchar", parameter("m")))),
                        Optional.empty()));

        assertExpressionPattern(
                "$like(a: varchar(n), b: varchar(m)): boolean",
                new CallPattern(
                        "$like",
                        List.of(
                                new ExpressionCapture("a", type("varchar", parameter("n"))),
                                new ExpressionCapture("b", type("varchar", parameter("m")))),
                        Optional.of(type("boolean"))));
    }

    @Test
    public void testCallPatternWithTypeClass()
    {
        TypeClassPattern integerClass = typeClass("integer_class", Set.of("integer", "bigint"));
        assertExpressionPattern(
                "add(a: integer_class, b: integer_class): integer_class",
                new CallPattern(
                        "add",
                        List.of(
                                new ExpressionCapture("a", integerClass),
                                new ExpressionCapture("b", integerClass)),
                        Optional.of(integerClass)));
    }

    private static void assertExpressionPattern(String expressionPattern, ExpressionPattern expected)
    {
        assertExpressionPattern(expressionPattern, expressionPattern, expected);
    }

    private static void assertExpressionPattern(String expressionPattern, String canonical, ExpressionPattern expected)
    {
        assertThat(expressionPattern(expressionPattern)).isEqualTo(expected);
        assertThat(expected.toString()).isEqualTo(canonical);
    }

    private static ExpressionPattern expressionPattern(String expressionPattern)
    {
        return new ExpressionMappingParser(TYPE_CLASSES).createExpressionPattern(expressionPattern);
    }

    private static SimpleTypePattern type(String baseName)
    {
        return new SimpleTypePattern(baseName, ImmutableList.of());
    }

    private static SimpleTypePattern type(String baseName, TypeParameterPattern... parameter)
    {
        return new SimpleTypePattern(baseName, ImmutableList.copyOf(parameter));
    }

    private static TypeParameterPattern parameter(long value)
    {
        return new LongTypeParameter(value);
    }

    private static TypeParameterPattern parameter(String name)
    {
        return new TypeParameterCapture(name);
    }

    private static TypeClassPattern typeClass(String typeClassName, Set<String> typeNames)
    {
        return new TypeClassPattern(typeClassName, typeNames);
    }
}
