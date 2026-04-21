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

import io.trino.spi.function.BoundSignature;
import io.trino.spi.function.FunctionId;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class FunctionBinding
{
    private final FunctionId functionId;
    private final BoundSignature boundSignature;
    private final VariableBindings variables;

    public FunctionBinding(FunctionId functionId, BoundSignature boundSignature, VariableBindings variables)
    {
        this.functionId = requireNonNull(functionId, "functionId is null");
        this.boundSignature = requireNonNull(boundSignature, "boundSignature is null");
        this.variables = requireNonNull(variables, "variables is null");
    }

    public FunctionId getFunctionId()
    {
        return functionId;
    }

    public BoundSignature getBoundSignature()
    {
        return boundSignature;
    }

    public int getArity()
    {
        return boundSignature.getArgumentTypes().size();
    }

    public VariableBindings variables()
    {
        return variables;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FunctionBinding that = (FunctionBinding) o;
        return Objects.equals(functionId, that.functionId) &&
                Objects.equals(boundSignature, that.boundSignature) &&
                Objects.equals(variables, that.variables);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionId, boundSignature, variables);
    }
}
