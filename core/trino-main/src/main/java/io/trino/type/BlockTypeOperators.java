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
package io.trino.type;

import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.trino.plugin.base.cache.NonKeyEvictableCache;
import io.trino.spi.block.Block;
import io.trino.spi.connector.SortOrder;
import io.trino.spi.function.InvocationConvention;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.GuardedBy;
import javax.inject.Inject;

import java.lang.invoke.MethodHandle;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.trino.plugin.base.cache.SafeCaches.buildNonEvictableCacheWithWeakInvalidateAll;
import static io.trino.spi.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static io.trino.spi.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static io.trino.spi.function.InvocationConvention.simpleConvention;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_FIRST;
import static io.trino.spi.function.OperatorType.COMPARISON_UNORDERED_LAST;
import static io.trino.type.TypeUtils.NULL_HASH_CODE;
import static io.trino.util.SingleAccessMethodCompiler.compileSingleAccessMethod;
import static java.util.Objects.requireNonNull;

public final class BlockTypeOperators
{
    private static final InvocationConvention BLOCK_EQUAL_CONVENTION = simpleConvention(NULLABLE_RETURN, BLOCK_POSITION, BLOCK_POSITION);
    private static final InvocationConvention HASH_CODE_CONVENTION = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION);
    private static final InvocationConvention XX_HASH_64_CONVENTION = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION);
    private static final InvocationConvention IS_DISTINCT_FROM_CONVENTION = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION);
    private static final InvocationConvention COMPARISON_CONVENTION = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION);
    private static final InvocationConvention ORDERING_CONVENTION = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION);
    private static final InvocationConvention LESS_THAN_CONVENTION = simpleConvention(FAIL_ON_NULL, BLOCK_POSITION, BLOCK_POSITION);

    private final NonKeyEvictableCache<GeneratedBlockOperatorKey<?>, GeneratedBlockOperator<?>> generatedBlockOperatorCache;
    private final TypeOperators typeOperators;

    public BlockTypeOperators()
    {
        this(new TypeOperators());
    }

    @Inject
    public BlockTypeOperators(TypeOperators typeOperators)
    {
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
        this.generatedBlockOperatorCache = buildNonEvictableCacheWithWeakInvalidateAll(
                CacheBuilder.newBuilder()
                        .maximumSize(10_000)
                        .expireAfterWrite(2, TimeUnit.HOURS));
    }

    public BlockPositionEqual getEqualOperator(Type type)
    {
        return getBlockOperator(type, BlockPositionEqual.class, () -> typeOperators.getEqualOperator(type, BLOCK_EQUAL_CONVENTION));
    }

    public interface BlockPositionEqual
    {
        Boolean equal(Block left, int leftPosition, Block right, int rightPosition);

        default boolean equalNullSafe(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
        {
            boolean leftIsNull = leftBlock.isNull(leftPosition);
            boolean rightIsNull = rightBlock.isNull(rightPosition);
            if (leftIsNull || rightIsNull) {
                return leftIsNull && rightIsNull;
            }
            return equal(leftBlock, leftPosition, rightBlock, rightPosition);
        }
    }

    public BlockPositionHashCode getHashCodeOperator(Type type)
    {
        return getBlockOperator(type, BlockPositionHashCode.class, () -> typeOperators.getHashCodeOperator(type, HASH_CODE_CONVENTION));
    }

    public interface BlockPositionHashCode
    {
        long hashCode(Block block, int position);

        default long hashCodeNullSafe(Block block, int position)
        {
            if (block.isNull(position)) {
                return NULL_HASH_CODE;
            }
            return hashCode(block, position);
        }
    }

    public BlockPositionXxHash64 getXxHash64Operator(Type type)
    {
        return getBlockOperator(type, BlockPositionXxHash64.class, () -> typeOperators.getXxHash64Operator(type, XX_HASH_64_CONVENTION));
    }

    public interface BlockPositionXxHash64
    {
        long xxHash64(Block block, int position);
    }

    public BlockPositionIsDistinctFrom getDistinctFromOperator(Type type)
    {
        return getBlockOperator(type, BlockPositionIsDistinctFrom.class, () -> typeOperators.getDistinctFromOperator(type, IS_DISTINCT_FROM_CONVENTION));
    }

    public interface BlockPositionIsDistinctFrom
    {
        boolean isDistinctFrom(Block left, int leftPosition, Block right, int rightPosition);
    }

    public BlockPositionComparison getComparisonUnorderedLastOperator(Type type)
    {
        return getBlockOperator(type, BlockPositionComparison.class, () -> typeOperators.getComparisonUnorderedLastOperator(type, COMPARISON_CONVENTION), Optional.of(COMPARISON_UNORDERED_LAST));
    }

    public BlockPositionComparison getComparisonUnorderedFirstOperator(Type type)
    {
        return getBlockOperator(type, BlockPositionComparison.class, () -> typeOperators.getComparisonUnorderedFirstOperator(type, COMPARISON_CONVENTION), Optional.of(COMPARISON_UNORDERED_FIRST));
    }

    public interface BlockPositionComparison
    {
        long compare(Block left, int leftPosition, Block right, int rightPosition);
    }

    public BlockPositionOrdering generateBlockPositionOrdering(Type type, SortOrder sortOrder)
    {
        return getBlockOperator(type, BlockPositionOrdering.class, () -> typeOperators.getOrderingOperator(type, sortOrder, ORDERING_CONVENTION), Optional.of(sortOrder));
    }

    public interface BlockPositionOrdering
    {
        int order(Block left, int leftPosition, Block right, int rightPosition);
    }

    public BlockPositionLessThan generateBlockPositionLessThan(Type type)
    {
        return getBlockOperator(type, BlockPositionLessThan.class, () -> typeOperators.getLessThanOperator(type, LESS_THAN_CONVENTION));
    }

    public interface BlockPositionLessThan
    {
        boolean lessThan(Block left, int leftPosition, Block right, int rightPosition);
    }

    private <T> T getBlockOperator(Type type, Class<T> operatorInterface, Supplier<MethodHandle> methodHandleSupplier)
    {
        return getBlockOperator(type, operatorInterface, methodHandleSupplier, Optional.empty());
    }

    private <T> T getBlockOperator(Type type, Class<T> operatorInterface, Supplier<MethodHandle> methodHandleSupplier, Optional<Object> additionalKey)
    {
        try {
            @SuppressWarnings("unchecked")
            GeneratedBlockOperator<T> generatedBlockOperator = (GeneratedBlockOperator<T>) generatedBlockOperatorCache.get(
                    new GeneratedBlockOperatorKey<>(type, operatorInterface, additionalKey),
                    () -> new GeneratedBlockOperator<>(type, operatorInterface, methodHandleSupplier.get()));
            return generatedBlockOperator.get();
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    private static class GeneratedBlockOperatorKey<T>
    {
        private final Type type;
        private final Class<T> operatorInterface;
        private final Optional<Object> additionalKey;

        public GeneratedBlockOperatorKey(Type type, Class<T> operatorInterface, Optional<Object> additionalKey)
        {
            this.type = requireNonNull(type, "type is null");
            this.operatorInterface = requireNonNull(operatorInterface, "operatorInterface is null");
            this.additionalKey = requireNonNull(additionalKey, "additionalKey is null");
        }

        public Type getType()
        {
            return type;
        }

        public Class<T> getOperatorInterface()
        {
            return operatorInterface;
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
            GeneratedBlockOperatorKey<?> that = (GeneratedBlockOperatorKey<?>) o;
            return type.equals(that.type) &&
                    operatorInterface.equals(that.operatorInterface) &&
                    additionalKey.equals(that.additionalKey);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(type, operatorInterface, additionalKey);
        }
    }

    private static class GeneratedBlockOperator<T>
    {
        private final Type type;
        private final Class<T> operatorInterface;
        private final MethodHandle methodHandle;
        @GuardedBy("this")
        private T operator;

        public GeneratedBlockOperator(Type type, Class<T> operatorInterface, MethodHandle methodHandle)
        {
            this.type = requireNonNull(type, "type is null");
            this.operatorInterface = requireNonNull(operatorInterface, "operatorInterface is null");
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
        }

        public synchronized T get()
        {
            if (operator != null) {
                return operator;
            }
            String suggestedClassName = operatorInterface.getSimpleName() + "_" + type.getDisplayName();
            operator = compileSingleAccessMethod(suggestedClassName, operatorInterface, methodHandle);
            return operator;
        }
    }

    // stats
    @Managed
    public long cacheSize()
    {
        return generatedBlockOperatorCache.size();
    }

    @Managed
    public Double getCacheHitRate()
    {
        return generatedBlockOperatorCache.stats().hitRate();
    }

    @Managed
    public Double getCacheMissRate()
    {
        return generatedBlockOperatorCache.stats().missRate();
    }

    @Managed
    public long getCacheRequestCount()
    {
        return generatedBlockOperatorCache.stats().requestCount();
    }

    @Managed
    public void cacheReset()
    {
        // Note: this may not invalidate ongoing loads (https://github.com/trinodb/trino/issues/10512, https://github.com/google/guava/issues/1881)
        // This is acceptable, since this operation is invoked manually, and not relied upon for correctness.
        generatedBlockOperatorCache.invalidateAll();
    }
}
