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
package org.weakref.solver.type;

import org.weakref.solver.Constraint;

import java.util.List;

/**
 * Base class for parametric type constructors — types taking one or more arguments
 * (numeric or nested types).
 * <p>
 * Subclasses specify the name, parameter names, and constructor-wide validation constraints
 * via the protected constructor, and override {@link #newInstance} to produce their
 * concrete {@link Type} value. Examples in the Trino preset: {@code DecimalType.DecimalTypeConstructor}
 * (takes numeric {@code p} and {@code s} with {@code p ≥ s}), {@code ArrayType}'s inner
 * constructor (takes one type parameter), {@code MapType}'s (two type parameters).
 */
public abstract class ParametricTypeConstructor
        implements TypeConstructor
{
    private final String name;
    private final List<String> parameters;
    private final List<Constraint> constraints;

    protected ParametricTypeConstructor(String name, List<String> parameters, List<Constraint> constraints)
    {
        this.name = name;
        this.parameters = parameters;
        this.constraints = constraints;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public List<String> parameters()
    {
        return parameters;
    }

    @Override
    public List<Constraint> constraints()
    {
        return constraints;
    }

    @Override
    public String toString()
    {
        return name + "(" + String.join(",", parameters) + ") ∀ " + constraints;
    }
}
