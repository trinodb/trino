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
package io.trino.operator.aggregation.histogram;

import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.Convention;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;
import io.trino.type.BlockTypeOperators.BlockPositionEqual;
import io.trino.type.BlockTypeOperators.BlockPositionHashCode;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static java.util.Objects.requireNonNull;

public class HistogramStateFactory
        implements AccumulatorStateFactory<HistogramState>
{
    public static final int EXPECTED_SIZE_FOR_HASHING = 10;

    private final Type type;
    private final BlockPositionEqual equalOperator;
    private final BlockPositionHashCode hashCodeOperator;

    public HistogramStateFactory(
            @TypeParameter("T") Type type,
            @OperatorDependency(
                    operator = OperatorType.EQUAL,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {BLOCK_POSITION, BLOCK_POSITION}, result = NULLABLE_RETURN))
                    BlockPositionEqual equalOperator,
            @OperatorDependency(
                    operator = OperatorType.HASH_CODE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = BLOCK_POSITION, result = FAIL_ON_NULL))
                    BlockPositionHashCode hashCodeOperator)
    {
        this.type = requireNonNull(type, "type is null");
        this.equalOperator = requireNonNull(equalOperator, "equalOperator is null");
        this.hashCodeOperator = requireNonNull(hashCodeOperator, "hashCodeOperator is null");
    }

    @Override
    public HistogramState createSingleState()
    {
        return new SingleHistogramState(type, equalOperator, hashCodeOperator, EXPECTED_SIZE_FOR_HASHING);
    }

    @Override
    public HistogramState createGroupedState()
    {
        return new GroupedHistogramState(type, equalOperator, hashCodeOperator, EXPECTED_SIZE_FOR_HASHING);
    }
}
