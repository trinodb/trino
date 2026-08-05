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
package io.trino.spi.function;

import static java.util.Objects.requireNonNull;

/// A variable declared by a function [Signature]: a hole bound when the signature is resolved
/// against actual arguments. It is either a [type variable][TypeVariable] (e.g. the `E` of
/// `array(E)`) carrying its [constraints][TypeVariableConstraint], or a
/// [numeric variable][NumericVariable] (e.g. the `p` of `decimal(p, s)`) carrying its
/// [definition][NumericVariableConstraint].
public sealed interface VariableDeclaration
        permits VariableDeclaration.TypeVariable, VariableDeclaration.NumericVariable
{
    String name();

    record TypeVariable(TypeVariableConstraint constraint)
            implements VariableDeclaration
    {
        public TypeVariable
        {
            requireNonNull(constraint, "constraint is null");
        }

        @Override
        public String name()
        {
            return constraint.getName();
        }
    }

    record NumericVariable(NumericVariableConstraint constraint)
            implements VariableDeclaration
    {
        public NumericVariable
        {
            requireNonNull(constraint, "constraint is null");
        }

        @Override
        public String name()
        {
            return constraint.getName();
        }
    }
}
