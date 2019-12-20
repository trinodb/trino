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

import com.google.common.collect.ImmutableList;
import io.prestosql.metadata.BoundVariables;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.ResolvedFunction;
import io.prestosql.spi.function.InvocationConvention;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeSignature;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.metadata.SignatureBinder.applyBoundVariables;
import static io.prestosql.spi.function.OperatorType.CAST;
import static io.prestosql.spi.function.OperatorType.SATURATED_FLOOR_CAST;
import static java.util.Objects.requireNonNull;

public final class OperatorImplementationDependency
        extends ScalarImplementationDependency
{
    private final OperatorType operator;
    private final List<TypeSignature> argumentTypes;

    public OperatorImplementationDependency(OperatorType operator, List<TypeSignature> argumentTypes, Optional<InvocationConvention> invocationConvention)
    {
        super(invocationConvention);
        this.operator = requireNonNull(operator, "operator is null");
        checkArgument(operator != CAST && operator != SATURATED_FLOOR_CAST);
        this.argumentTypes = ImmutableList.copyOf(requireNonNull(argumentTypes, "argumentTypes is null"));
    }

    public OperatorType getOperator()
    {
        return operator;
    }

    public List<TypeSignature> getArgumentTypes()
    {
        return argumentTypes;
    }

    @Override
    protected ResolvedFunction getResolvedFunction(BoundVariables boundVariables, Metadata metadata)
    {
        List<Type> argumentTypes = applyBoundVariables(this.argumentTypes, boundVariables).stream()
                .map(metadata::getType)
                .collect(toImmutableList());
        return metadata.resolveOperator(operator, argumentTypes);
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
        OperatorImplementationDependency that = (OperatorImplementationDependency) o;
        return operator == that.operator &&
                Objects.equals(argumentTypes, that.argumentTypes);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(operator, argumentTypes);
    }
}
