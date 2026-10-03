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

import java.util.List;
import java.util.Locale;
import java.util.Objects;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

/// The open, variable-bearing structural form of a type, used in the argument and return positions of a
/// function [io.trino.spi.function.Signature] — e.g. `array(E)`, `decimal(p, s)`,
/// `char(x + y)`.
///
/// It is the counterpart to the ground [TypeDescriptor]: where a `TypeDescriptor` denotes one
/// concrete type, a `TypeTemplate` denotes a family parameterized by type and numeric variables.
/// Binding its variables (see [TypeTemplates#bind]) produces a ground [TypeDescriptor];
/// [TypeTemplates#fromTypeDescriptor] lifts a variable-free signature back into a template.
public sealed interface TypeTemplate
        permits TypeTemplate.TypeApplication, TypeTemplate.TypeVariable
{
    /// The base name: the variable name for a [TypeVariable], the constructor name for a [TypeApplication].
    String baseName();

    /// Renders this template in the internal `base(arg, …)` IR form, e.g. `array(E)`, `decimal(p,s)`,
    /// `char(x + y)`. Mirrors [TypeDescriptor#toString] so the open and ground forms share one
    /// representation; the user-visible SQL spelling is produced separately by [TypeSyntax].
    String render();

    /// A type constructor applied to template parameters, e.g. `array(E)` or `decimal(p, s)`.
    /// A nullary application (no parameters) is a concrete scalar type such as `bigint`.
    record TypeApplication(String base, List<TemplateParameter> parameters)
            implements TypeTemplate
    {
        public TypeApplication
        {
            requireNonNull(base, "base is null");
            parameters = List.copyOf(parameters);
        }

        @Override
        public String baseName()
        {
            return base;
        }

        @Override
        public String render()
        {
            if (parameters.isEmpty()) {
                return base;
            }
            return base + parameters.stream().map(TemplateParameter::render).collect(joining(",", "(", ")"));
        }

        // Type names are case-insensitive in Trino, matching the TypeDescriptor identity.
        @Override
        public boolean equals(Object o)
        {
            return o instanceof TypeApplication application
                    && base.equalsIgnoreCase(application.base)
                    && parameters.equals(application.parameters);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(base.toLowerCase(Locale.ROOT), parameters);
        }
    }

    /// A reference to a declared type variable, e.g. the `E` in `array(E)`.
    record TypeVariable(String name)
            implements TypeTemplate
    {
        public TypeVariable
        {
            requireNonNull(name, "name is null");
        }

        @Override
        public String baseName()
        {
            return name;
        }

        @Override
        public String render()
        {
            return name;
        }

        // Type-variable names are case-insensitive: a reference in a template matches its declaration
        // regardless of case, consistent with type-name identity.
        @Override
        public boolean equals(Object o)
        {
            return o instanceof TypeVariable variable && name.equalsIgnoreCase(variable.name);
        }

        @Override
        public int hashCode()
        {
            return name.toLowerCase(Locale.ROOT).hashCode();
        }
    }
}
