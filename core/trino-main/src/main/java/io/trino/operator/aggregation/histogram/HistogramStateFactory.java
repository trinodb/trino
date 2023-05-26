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

import java.lang.invoke.MethodHandle;

import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION_NOT_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.FLAT;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.BLOCK_BUILDER;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FLAT_RETURN;
import static java.util.Objects.requireNonNull;

public class HistogramStateFactory
        implements AccumulatorStateFactory<HistogramState>
{
    private final Type type;
    private final MethodHandle readFlat;
    private final MethodHandle writeFlat;
    private final MethodHandle hashFlat;
    private final MethodHandle distinctFlatBlock;
    private final MethodHandle hashBlock;

    public HistogramStateFactory(
            @TypeParameter("T") Type type,
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = FLAT, result = BLOCK_BUILDER)) MethodHandle readFlat,
            @OperatorDependency(
                    operator = OperatorType.READ_VALUE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FLAT_RETURN)) MethodHandle writeFlat,
            @OperatorDependency(
                    operator = OperatorType.HASH_CODE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = FLAT, result = FAIL_ON_NULL)) MethodHandle hashFlat,
            @OperatorDependency(
                    operator = OperatorType.IS_DISTINCT_FROM,
                    argumentTypes = {"T", "T"},
                    convention = @Convention(arguments = {FLAT, BLOCK_POSITION_NOT_NULL}, result = FAIL_ON_NULL)) MethodHandle distinctFlatBlock,
            @OperatorDependency(
                    operator = OperatorType.HASH_CODE,
                    argumentTypes = "T",
                    convention = @Convention(arguments = BLOCK_POSITION_NOT_NULL, result = FAIL_ON_NULL)) MethodHandle hashBlock)
    {
        this.type = requireNonNull(type, "type is null");
        this.readFlat = requireNonNull(readFlat, "readFlat is null");
        this.writeFlat = requireNonNull(writeFlat, "writeFlat is null");
        this.hashFlat = requireNonNull(hashFlat, "hashFlat is null");
        this.distinctFlatBlock = requireNonNull(distinctFlatBlock, "distinctFlatBlock is null");
        this.hashBlock = requireNonNull(hashBlock, "hashBlock is null");
    }

    @Override
    public HistogramState createSingleState()
    {
        return new SingleHistogramState(type, readFlat, writeFlat, hashFlat, distinctFlatBlock, hashBlock);
    }

    @Override
    public HistogramState createGroupedState()
    {
        return new GroupedHistogramState(type, readFlat, writeFlat, hashFlat, distinctFlatBlock, hashBlock);
    }
}
