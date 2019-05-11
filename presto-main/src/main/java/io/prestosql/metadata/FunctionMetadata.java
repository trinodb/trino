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
package io.prestosql.metadata;

import static java.util.Objects.requireNonNull;

public class FunctionMetadata
{
    private final FunctionId functionId;
    private final Signature signature;
    private final boolean hidden;
    private final boolean deterministic;
    private final String description;

    public FunctionMetadata(Signature signature, boolean hidden, boolean deterministic, String description)
    {
        this(FunctionId.toFunctionId(signature), signature, hidden, deterministic, description);
    }

    public FunctionMetadata(FunctionId functionId, Signature signature, boolean hidden, boolean deterministic, String description)
    {
        this.functionId = requireNonNull(functionId, "functionId is null");
        this.signature = requireNonNull(signature, "signature is null");
        this.hidden = hidden;
        this.deterministic = deterministic;
        this.description = requireNonNull(description, "description is null");
    }

    public FunctionId getFunctionId()
    {
        return functionId;
    }

    public Signature getSignature()
    {
        return signature;
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

    @Override
    public String toString()
    {
        return signature.toString();
    }
}
