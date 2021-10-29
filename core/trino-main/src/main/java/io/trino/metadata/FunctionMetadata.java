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
    private final Signature signature;
    private final String canonicalName;
    private final boolean nullable;
    private final List<FunctionArgumentDefinition> argumentDefinitions;
    private final boolean hidden;
    private final boolean deterministic;
    private final String description;
    private final FunctionKind kind;
    private final boolean deprecated;

    public FunctionMetadata(
            Signature signature,
            boolean nullable,
            List<FunctionArgumentDefinition> argumentDefinitions,
            boolean hidden,
            boolean deterministic,
            String description,
            FunctionKind kind)
    {
        this(
                FunctionId.toFunctionId(signature),
                signature,
                signature.getName(),
                nullable,
                argumentDefinitions,
                hidden,
                deterministic,
                description,
                kind,
                false);
    }

    public FunctionMetadata(
            Signature signature,
            String canonicalName,
            boolean nullable,
            List<FunctionArgumentDefinition> argumentDefinitions,
            boolean hidden,
            boolean deterministic,
            String description,
            FunctionKind kind,
            boolean deprecated)
    {
        this(
                FunctionId.toFunctionId(signature),
                signature,
                canonicalName,
                nullable,
                argumentDefinitions,
                hidden,
                deterministic,
                description,
                kind,
                deprecated);
    }

    public FunctionMetadata(
            FunctionId functionId,
            Signature signature,
            String canonicalName,
            boolean nullable,
            List<FunctionArgumentDefinition> argumentDefinitions,
            boolean hidden,
            boolean deterministic,
            String description,
            FunctionKind kind,
            boolean deprecated)
    {
        this.functionId = requireNonNull(functionId, "functionId is null");
        this.signature = requireNonNull(signature, "signature is null");
        this.canonicalName = requireNonNull(canonicalName, "canonicalName is null");
        this.nullable = nullable;
        this.argumentDefinitions = ImmutableList.copyOf(requireNonNull(argumentDefinitions, "argumentDefinitions is null"));
        this.hidden = hidden;
        this.deterministic = deterministic;
        this.description = requireNonNull(description, "description is null");
        this.kind = requireNonNull(kind, "kind is null");
        this.deprecated = deprecated;
    }

    /**
     * Unique id of this function.
     * For aliased functions, each alias must have a different alias.
     */
    public FunctionId getFunctionId()
    {
        return functionId;
    }

    /**
     * Signature of a matching call site.
     * For aliased functions, the signature must use the alias name.
     */
    public Signature getSignature()
    {
        return signature;
    }

    /**
     * For aliased functions, the canonical name of the function.
     */
    public String getCanonicalName()
    {
        return canonicalName;
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

    @Override
    public String toString()
    {
        return signature.toString();
    }
}
