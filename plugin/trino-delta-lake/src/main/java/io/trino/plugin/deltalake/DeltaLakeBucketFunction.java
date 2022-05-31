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
package io.trino.plugin.deltalake;

import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.BucketFunction;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import io.trino.spi.type.TypeUtils;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.type.TypeUtils.NULL_HASH_CODE;

public class DeltaLakeBucketFunction
        implements BucketFunction
{
    private final List<Type> types;
    private final int bucketCount;
    private final List<MethodHandle> hashCodeInvokers;

    public DeltaLakeBucketFunction(TypeOperators typeOperators, List<DeltaLakeColumnHandle> partitioningColumns, int bucketCount)
    {
        this.types = partitioningColumns.stream()
                .map(DeltaLakeColumnHandle::getType)
                .collect(toImmutableList());
        this.hashCodeInvokers = partitioningColumns.stream()
                .map(DeltaLakeColumnHandle::getType)
                .map(type -> typeOperators.getHashCodeOperator(type, simpleConvention(FAIL_ON_NULL, NEVER_NULL)))
                .collect(toImmutableList());
        this.bucketCount = bucketCount;
    }

    @Override
    public int getBucket(Page page, int position)
    {
        long result = 0;
        for (int channel = 0; channel < page.getChannelCount(); channel++) {
            Block block = page.getBlock(channel);
            Object value = TypeUtils.readNativeValue(types.get(channel), block, position);
            result += hashValue(hashCodeInvokers.get(channel), value);
        }
        return (int) ((result & Long.MAX_VALUE) % bucketCount);
    }

    private static long hashValue(MethodHandle method, Object value)
    {
        if (value == null) {
            return NULL_HASH_CODE;
        }
        try {
            return (long) method.invoke(value);
        }
        catch (Throwable throwable) {
            if (throwable instanceof Error) {
                throw (Error) throwable;
            }
            if (throwable instanceof RuntimeException) {
                throw (RuntimeException) throwable;
            }
            throw new RuntimeException(throwable);
        }
    }
}
