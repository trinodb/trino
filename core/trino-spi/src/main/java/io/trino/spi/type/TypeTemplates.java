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

import io.trino.spi.type.TemplateParameter.NumericArgument;
import io.trino.spi.type.TemplateParameter.TypeArgument;
import io.trino.spi.type.TypeTemplate.TypeApplication;
import io.trino.spi.type.TypeTemplate.TypeVariable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/// Operations over [TypeTemplate] — binding it to ground types, and lifting a ground
/// [TypeSignature] into one.
public final class TypeTemplates
{
    private TypeTemplates() {}

    /// Substitutes the given type- and numeric-variable bindings, producing a ground type signature.
    public static TypeSignature bind(TypeTemplate template, Map<String, TypeSignature> typeBindings, Map<String, Long> numericBindings)
    {
        return switch (template) {
            case TypeVariable(String name) -> {
                TypeSignature binding = typeBindings.get(name);
                if (binding == null) {
                    throw new IllegalArgumentException("No binding for type variable " + name);
                }
                yield binding;
            }
            case TypeApplication(String base, List<TemplateParameter> parameters) -> {
                List<TypeParameter> bound = new ArrayList<>(parameters.size());
                for (TemplateParameter parameter : parameters) {
                    bound.add(bind(parameter, typeBindings, numericBindings));
                }
                yield new TypeSignature(base, bound);
            }
        };
    }

    private static TypeParameter bind(TemplateParameter parameter, Map<String, TypeSignature> typeBindings, Map<String, Long> numericBindings)
    {
        return switch (parameter) {
            case TypeArgument(Optional<String> name, TypeTemplate type) -> TypeParameter.typeParameter(name, bind(type, typeBindings, numericBindings));
            case NumericArgument(NumericExpression value) -> TypeParameter.numericParameter(NumericExpressions.evaluate(value, numericBindings).longValueExact());
        };
    }

    /// Lowers a ground template to a type signature. Fails if the template carries an unbound variable.
    public static TypeSignature toTypeSignature(TypeTemplate template)
    {
        return bind(template, Map.of(), Map.of());
    }

    /// Lowers a list of ground templates to type signatures. Fails if any template carries an unbound variable.
    public static List<TypeSignature> toTypeSignatures(List<TypeTemplate> types)
    {
        return types.stream()
                .map(TypeTemplates::toTypeSignature)
                .toList();
    }

    /// Whether the template contains a calculated (numeric-variable or expression) parameter — i.e. a numeric
    /// parameter that is not a fixed literal. A bare type variable is not "calculated" (it is generic).
    public static boolean isCalculated(TypeTemplate template)
    {
        return switch (template) {
            case TypeVariable(String name) -> false;
            case TypeApplication(String base, List<TemplateParameter> parameters) -> parameters.stream().anyMatch(TypeTemplates::isCalculated);
        };
    }

    private static boolean isCalculated(TemplateParameter parameter)
    {
        return switch (parameter) {
            case TypeArgument(Optional<String> name, TypeTemplate type) -> isCalculated(type);
            case NumericArgument(NumericExpression value) -> !(value instanceof NumericExpression.Literal);
        };
    }

    /// Lifts a ground type signature into a template: every parameter is lifted structurally, with a
    /// numeric parameter becoming a literal numeric argument.
    public static TypeTemplate fromTypeSignature(TypeSignature signature)
    {
        List<TemplateParameter> parameters = new ArrayList<>(signature.getParameters().size());
        for (TypeParameter parameter : signature.getParameters()) {
            parameters.add(fromTypeParameter(parameter));
        }
        return new TypeApplication(signature.getBase(), parameters);
    }

    private static TemplateParameter fromTypeParameter(TypeParameter parameter)
    {
        return switch (parameter) {
            case TypeParameter.Type(Optional<String> name, TypeSignature type) -> new TypeArgument(name, fromTypeSignature(type));
            case TypeParameter.Numeric(long value) -> new NumericArgument(new NumericExpression.Literal(value));
        };
    }

    public static TypeTemplate typeVariable(String name)
    {
        return new TypeVariable(name);
    }

    public static NumericExpression numericVariable(String name)
    {
        return new NumericExpression.Variable(name);
    }

    public static TemplateParameter argument(TypeTemplate type)
    {
        return new TypeArgument(Optional.empty(), type);
    }

    public static TemplateParameter argument(NumericExpression value)
    {
        return new NumericArgument(value);
    }

    public static TypeTemplate type(String base)
    {
        return new TypeApplication(base, List.of());
    }

    public static TypeTemplate type(String base, TemplateParameter... arguments)
    {
        return new TypeApplication(base, List.of(arguments));
    }

    public static TypeTemplate type(String base, TypeTemplate... parameters)
    {
        List<TemplateParameter> arguments = new ArrayList<>(parameters.length);
        for (TypeTemplate parameter : parameters) {
            arguments.add(argument(parameter));
        }
        return new TypeApplication(base, arguments);
    }

    public static TypeTemplate type(String base, NumericExpression... parameters)
    {
        List<TemplateParameter> arguments = new ArrayList<>(parameters.length);
        for (NumericExpression parameter : parameters) {
            arguments.add(argument(parameter));
        }
        return new TypeApplication(base, arguments);
    }

    public static TypeTemplate arrayType(TypeTemplate elementType)
    {
        return type("array", elementType);
    }

    public static TypeTemplate mapType(TypeTemplate keyType, TypeTemplate valueType)
    {
        return type("map", keyType, valueType);
    }

    public static TypeTemplate functionType(TypeTemplate first, TypeTemplate... rest)
    {
        List<TemplateParameter> parameters = new ArrayList<>(rest.length + 1);
        parameters.add(new TypeArgument(Optional.empty(), first));
        for (TypeTemplate type : rest) {
            parameters.add(new TypeArgument(Optional.empty(), type));
        }
        return new TypeApplication("function", parameters);
    }

    public static TypeTemplate rowType(List<TemplateParameter> fields)
    {
        return new TypeApplication("row", List.copyOf(fields));
    }

    public static TypeTemplate rowType(TypeTemplate... fieldTypes)
    {
        List<TemplateParameter> fields = new ArrayList<>(fieldTypes.length);
        for (TypeTemplate fieldType : fieldTypes) {
            fields.add(argument(fieldType));
        }
        return new TypeApplication("row", fields);
    }
}
