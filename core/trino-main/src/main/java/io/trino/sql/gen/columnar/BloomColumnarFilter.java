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
package io.trino.sql.gen.columnar;

import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.cache.NonEvictableCache;
import io.trino.operator.project.InputChannels;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.type.Type;
import io.trino.sql.planner.BloomFilterWithRange;

import java.lang.reflect.Constructor;
import java.util.concurrent.TimeUnit;

import static io.trino.cache.CacheUtils.uncheckedCacheGet;
import static io.trino.cache.SafeCaches.buildNonEvictableCache;
import static io.trino.spi.StandardErrorCode.COMPILER_ERROR;
import static io.trino.sql.gen.IsolatedClass.isolateClass;
import static java.util.Objects.requireNonNull;

public final class BloomColumnarFilter
        implements ColumnarFilter
{
    // Perform classloader isolation for BloomColumnarFilter class per Type to avoid mega-morphic call site when reading a position from input block
    private static final NonEvictableCache<Type, Constructor<? extends ColumnarFilter>> COLUMNAR_BLOOM_FILTER_CACHE = buildNonEvictableCache(
            CacheBuilder.newBuilder()
                    .maximumSize(100)
                    .expireAfterWrite(2, TimeUnit.HOURS));

    private final BloomFilterWithRange filter;
    private final InputChannels inputChannels;

    public static Supplier<FilterEvaluator> createBloomFilterEvaluator(BloomFilterWithRange bloomFilterWithRange, int inputChannel)
    {
        return () -> new ColumnarFilterEvaluator(
                new DictionaryAwareColumnarFilter(
                        createColumnarBloomFilter(bloomFilterWithRange.type(), inputChannel, bloomFilterWithRange).get()));
    }

    public BloomColumnarFilter(BloomFilterWithRange filter, InputChannels inputChannels)
    {
        this.filter = requireNonNull(filter, "filter is null");
        this.inputChannels = requireNonNull(inputChannels, "inputChannels is null");
    }

    @Override
    public int filterPositionsRange(ConnectorSession session, int[] outputPositions, int offset, int size, SourcePage loadedPage)
    {
        int outputPositionsCount = 0;
        Block block = loadedPage.getBlock(0);
        for (int position = offset; position < offset + size; position++) {
            boolean result = this.filter.test(block, position);
            outputPositions[outputPositionsCount] = position;
            outputPositionsCount += result ? 1 : 0;
        }
        return outputPositionsCount;
    }

    @Override
    public int filterPositionsList(ConnectorSession session, int[] outputPositions, int[] activePositions, int offset, int size, SourcePage loadedPage)
    {
        int outputPositionsCount = 0;
        Block block = loadedPage.getBlock(0);
        for (int index = offset; index < offset + size; index++) {
            int position = activePositions[index];
            boolean result = this.filter.test(block, position);
            outputPositions[outputPositionsCount] = position;
            outputPositionsCount += result ? 1 : 0;
        }
        return outputPositionsCount;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    private static Supplier<ColumnarFilter> createColumnarBloomFilter(Type type, int inputChannel, BloomFilterWithRange bloomFilterWithRange)
    {
        Constructor<? extends ColumnarFilter> filterConstructor = uncheckedCacheGet(
                COLUMNAR_BLOOM_FILTER_CACHE,
                type,
                BloomColumnarFilter::isolateColumnarBloomFilterClass);

        return () -> {
            try {
                InputChannels inputChannels = new InputChannels(ImmutableList.of(inputChannel), ImmutableSet.of(inputChannel));
                return filterConstructor.newInstance(bloomFilterWithRange, inputChannels);
            }
            catch (ReflectiveOperationException e) {
                throw new TrinoException(COMPILER_ERROR, e);
            }
        };
    }

    private static Constructor<? extends ColumnarFilter> isolateColumnarBloomFilterClass()
    {
        Class<? extends ColumnarFilter> isolatedColumnarBloomFilter = isolateClass(
                new DynamicClassLoader(DynamicPageFilter.class.getClassLoader()),
                ColumnarFilter.class,
                BloomColumnarFilter.class);
        try {
            return isolatedColumnarBloomFilter.getConstructor(BloomFilterWithRange.class, InputChannels.class);
        }
        catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }
}
