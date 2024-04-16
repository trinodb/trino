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
package io.trino.sql.relational;

import com.google.common.collect.ImmutableList;
import io.trino.metadata.Metadata;
import io.trino.metadata.ResolvedFunction;
import io.trino.spi.function.OperatorType;
import io.trino.spi.type.Type;
import io.trino.sql.ir.Comparison;

import static io.trino.spi.function.OperatorType.EQUAL;
import static io.trino.spi.function.OperatorType.IS_DISTINCT_FROM;
import static io.trino.spi.function.OperatorType.LESS_THAN;
import static io.trino.spi.function.OperatorType.LESS_THAN_OR_EQUAL;
import static java.util.Objects.requireNonNull;

public final class StandardFunctionResolution
{
    private final Metadata metadata;

    public StandardFunctionResolution(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    public ResolvedFunction comparisonFunction(Comparison.Operator operator, Type leftType, Type rightType)
    {
        OperatorType operatorType = switch (operator) {
            case EQUAL -> EQUAL;
            case LESS_THAN -> LESS_THAN;
            case LESS_THAN_OR_EQUAL -> LESS_THAN_OR_EQUAL;
            case IS_DISTINCT_FROM -> IS_DISTINCT_FROM;
            default -> throw new IllegalStateException("Unsupported comparison operator type: " + operator);
        };

        return metadata.resolveOperator(operatorType, ImmutableList.of(leftType, rightType));
    }
}
