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
package io.trino.metadata;

import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class FunctionMetadata
{
    private final FunctionId functionId;
    private final Signature canonicalSignature;
    private final boolean nullable;
    private final List<FunctionArgumentDefinition> argumentDefinitions;
    private final boolean hidden;
    private final boolean deterministic;
    private final String description;
    private final FunctionKind kind;
    private final boolean deprecated;
    private final String actualName;

    public FunctionMetadata(
            Signature canonicalSignature,
            boolean nullable,
            List<FunctionArgumentDefinition> argumentDefinitions,
            boolean hidden,
            boolean deterministic,
            String description,
            FunctionKind kind)
    {
        this(
                FunctionId.toFunctionId(canonicalSignature),
                canonicalSignature,
                nullable,
                argumentDefinitions,
                hidden,
                deterministic,
                description,
                kind,
                false,
                canonicalSignature.getName());
    }

    public FunctionMetadata(
            Signature canonicalSignature,
            boolean nullable,
            List<FunctionArgumentDefinition> argumentDefinitions,
            boolean hidden,
            boolean deterministic,
            String description,
            FunctionKind kind,
            boolean deprecated,
            String actualName)
    {
        this(
                FunctionId.toFunctionId(
                        new Signature(
                                actualName,
                                canonicalSignature.getTypeVariableConstraints(),
                                canonicalSignature.getLongVariableConstraints(),
                                canonicalSignature.getReturnType(),
                                canonicalSignature.getArgumentTypes(),
                                canonicalSignature.isVariableArity())),
                canonicalSignature,
                nullable,
                argumentDefinitions,
                hidden,
                deterministic,
                description,
                kind,
                deprecated,
                actualName);
    }

    public FunctionMetadata(
            FunctionId functionId,
            Signature canonicalSignature,
            boolean nullable,
            List<FunctionArgumentDefinition> argumentDefinitions,
            boolean hidden,
            boolean deterministic,
            String description,
            FunctionKind kind,
            boolean deprecated,
            String actualName)
    {
        this.functionId = requireNonNull(functionId, "functionId is null");
        this.canonicalSignature = requireNonNull(canonicalSignature, "canonicalSignature is null");
        this.nullable = nullable;
        this.argumentDefinitions = ImmutableList.copyOf(requireNonNull(argumentDefinitions, "argumentDefinitions is null"));
        this.hidden = hidden;
        this.deterministic = deterministic;
        this.description = requireNonNull(description, "description is null");
        this.kind = requireNonNull(kind, "kind is null");
        this.deprecated = deprecated;
        this.actualName = requireNonNull(actualName, "actualName is null");
    }

    public FunctionId getFunctionId()
    {
        return functionId;
    }

    public Signature getSignature()
    {
        return canonicalSignature;
    }

    public boolean isNullable()
    {
        return nullable;
    }

    public List<FunctionArgumentDefinition> getArgumentDefinitions()
    {
        return argumentDefinitions;
    }

    public boolean isHidden()
    {
        return hidden;
    }

    public boolean isDeterministic()
    {
        return deterministic;
    }

    public String getDescription()
    {
        return description;
    }

    public FunctionKind getKind()
    {
        return kind;
    }

    public boolean isDeprecated()
    {
        return deprecated;
    }

    public String getActualName()
    {
        return actualName;
    }

    @Override
    public String toString()
    {
        return canonicalSignature.toString();
    }
}
