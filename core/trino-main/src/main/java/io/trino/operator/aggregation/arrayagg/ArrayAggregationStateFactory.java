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
package io.trino.operator.aggregation.arrayagg;

import io.trino.spi.function.AccumulatorStateFactory;
import io.trino.spi.function.Convention;
import io.trino.spi.function.OperatorDependency;
import io.trino.spi.function.OperatorType;
import io.trino.spi.function.TypeParameter;
import io.trino.spi.type.Type;

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.VALUE_BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;

public class ArrayAggregationStateFactory
        implements AccumulatorStateFactory<ArrayAggregationState>
{
    private final Type type;
    private final MethodHandle readFlat;
    private final MethodHandle writeFlat;

    public ArrayAggregationStateFactory(
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = FLAT, result = BLOCK_BUILDER))
            MethodHandle readFlat,
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = VALUE_BLOCK_POSITION_NOT_NULL, result = FLAT_RETURN))
            MethodHandle writeFlat,
            @TypeParameter("T") Type type)
    {
        this.type = type;
        this.readFlat = readFlat;
        this.writeFlat = writeFlat;
    }

    @Override
    public ArrayAggregationState createSingleState()
    {
        return new SingleArrayAggregationState(type, readFlat, writeFlat);
    }

    @Override
    public ArrayAggregationState createGroupedState()
    {
        return new GroupArrayAggregationState(type, readFlat, writeFlat);
    }
}
