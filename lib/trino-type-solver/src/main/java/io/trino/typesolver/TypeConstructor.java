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
package io.trino.typesolver;

import io.trino.spi.type.NumericExpression;
import io.trino.spi.type.TemplateParameter;
import io.trino.spi.type.TemplateParameter.NumericArgument;
import io.trino.spi.type.TemplateParameter.TypeArgument;
import io.trino.spi.type.TypeTemplate.TypeApplication;
import io.trino.spi.type.TypeTemplate.TypeVariable;

import java.util.List;

import static io.trino.spi.type.TypeTemplates.type;
import static java.util.Objects.requireNonNull;

/// Symbolic metadata for a type constructor known to the solver.
///
/// This describes the constructor's parameter shape, validation constraints, and traits.
/// It deliberately has no factory for runtime type instances. Solved types cross the Trino
/// integration boundary as [io.trino.spi.type.TypeDescriptor] values and are materialized by
/// Trino's [io.trino.spi.type.TypeManager].
public record TypeConstructor(
        TypeApplication template,
        List<Constraint> constraints,
        boolean variadic,
        Trait comparable,
        Trait orderable)
{
    public TypeConstructor
    {
        requireNonNull(template, "template is null");
        constraints = List.copyOf(constraints);
        requireNonNull(comparable, "comparable is null");
        requireNonNull(orderable, "orderable is null");
        for (int index = 0; index < template.parameters().size(); index++) {
            parameterName(template.parameters().get(index));
        }
    }

    public TypeConstructor(TypeApplication template, List<Constraint> constraints)
    {
        this(template, constraints, false, Trait.PRESENT, Trait.PRESENT);
    }

    public String name()
    {
        return template.base();
    }

    public List<TemplateParameter> parameters()
    {
        return template.parameters();
    }

    public String parameterName(int index)
    {
        return parameterName(parameters().get(index));
    }

    private static String parameterName(TemplateParameter parameter)
    {
        return switch (parameter) {
            case TypeArgument(_, TypeVariable(String name)) -> name;
            case NumericArgument(NumericExpression.Variable(String name)) -> name;
            default -> throw new IllegalArgumentException("Constructor parameter must be a variable: " + parameter);
        };
    }

    public Kind parameterKind(int index)
    {
        return switch (parameters().get(index)) {
            case TypeArgument(_, TypeVariable _) -> Kind.TYPE;
            case NumericArgument(NumericExpression.Variable _) -> Kind.NUMBER;
            default -> throw new IllegalStateException("Constructor parameter must be a variable: " + parameters().get(index));
        };
    }

    public static TypeConstructor primitive(String name)
    {
        return primitive(name, Trait.PRESENT, Trait.PRESENT);
    }

    public static TypeConstructor primitive(String name, Trait comparable, Trait orderable)
    {
        return new TypeConstructor((TypeApplication) type(name), List.of(), false, comparable, orderable);
    }

    public static TypeConstructor parametric(String name, List<TemplateParameter> parameters, List<Constraint> constraints)
    {
        return new TypeConstructor(new TypeApplication(name, parameters), constraints);
    }

    @Override
    public String toString()
    {
        if (parameters().isEmpty()) {
            return name();
        }
        return template.render() + " ∀ " + constraints;
    }

    /// How comparability or orderability applies to a constructor.
    public enum Trait
    {
        ABSENT,
        PRESENT,
        STRUCTURAL,
    }
}
