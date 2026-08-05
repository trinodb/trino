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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/// A parameter of a [TypeTemplate.TypeApplication]: either a nested type (optionally named, for row
/// fields) or a numeric expression (a precision, scale or length).
///
/// Mirrors [TypeParameter] on the ground side, but a numeric parameter is a [NumericExpression]
/// rather than a fixed `long`, so a calculated parameter like `char(x + y)` is representable.
public sealed interface TemplateParameter
        permits TemplateParameter.TypeArgument, TemplateParameter.NumericArgument
{
    /// Renders this parameter in type syntax — a quoted field name plus the nested type for a named
    /// [TypeArgument], or the rendered expression for a [NumericArgument].
    String render();

    /// A nested type argument, e.g. the element of `array(E)` or a `row` field (with a name).
    record TypeArgument(Optional<String> name, TypeTemplate type)
            implements TemplateParameter
    {
        public TypeArgument
        {
            requireNonNull(name, "name is null");
            requireNonNull(type, "type is null");
        }

        @Override
        public String render()
        {
            return name.map(fieldName -> "\"" + fieldName.replace("\"", "\"\"") + "\" ").orElse("") + type.render();
        }
    }

    /// A numeric argument, e.g. a precision/scale in `decimal(p, s)` or a length in `varchar(x * 12)`.
    record NumericArgument(NumericExpression value)
            implements TemplateParameter
    {
        public NumericArgument
        {
            requireNonNull(value, "value is null");
        }

        @Override
        public String render()
        {
            return NumericExpressions.render(value);
        }
    }
}
