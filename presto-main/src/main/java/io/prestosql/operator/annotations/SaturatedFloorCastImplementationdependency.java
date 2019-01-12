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
package io.prestosql.operator.annotations;

import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.FunctionHandle;
import io.prestosql.metadata.FunctionManager;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.type.TypeSignature;

import java.util.Objects;
import java.util.Optional;

import static io.prestosql.metadata.SignatureBinder.applyBoundVariables;
import static java.util.Objects.requireNonNull;

public final class SaturatedFloorCastImplementationdependency
        extends ScalarImplementationDependency
{
    private final TypeSignature fromType;
    private final TypeSignature toType;

    public SaturatedFloorCastImplementationdependency(TypeSignature fromType, TypeSignature toType, Optional<InvocationConvention> invocationConvention)
    {
        super(invocationConvention);
        this.fromType = requireNonNull(fromType, "fromType is null");
        this.toType = requireNonNull(toType, "toType is null");
    }

    @Override
    protected FunctionHandle getFunctionHandle(BoundVariables boundVariables, FunctionManager functionManager)
    {
        return functionManager.lookupSaturatedFloorCast(
                applyBoundVariables(fromType, boundVariables),
                applyBoundVariables(toType, boundVariables));
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
        SaturatedFloorCastImplementationdependency that = (SaturatedFloorCastImplementationdependency) o;
        return Objects.equals(fromType, that.fromType) &&
                Objects.equals(toType, that.toType);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(fromType, toType);
    }
}
