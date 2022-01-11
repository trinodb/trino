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
package io.trino.operator.hash;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.operator.hash.fixed.FixedOffsetHashTableRowFormat;
import io.trino.operator.hash.fixed.FixedOffsetHashTableRowFormat2Channels;
import io.trino.operator.hash.fixed.FixedOffsetHashTableRowFormat4Channels;
import io.trino.spi.type.Type;
import io.trino.sql.gen.IsolatedClass;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class IsolatedHashTableRowFormatFactory
{
    public static volatile boolean useDedicatedExtractor = true;
    private final LoadingCache<CacheKey, HashTableRowFormat> cache;

    public IsolatedHashTableRowFormatFactory()
    {
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(1024 * 1024)
                .build(CacheLoader.from(key -> createInstance(key.hashChannelTypes, key.maxVarWidthBufferSize)));
    }

    public HashTableRowFormat create(List<? extends Type> hashTypes, int maxVarWidthBufferSize)
    {
        return cache.getUnchecked(new CacheKey(hashTypes, maxVarWidthBufferSize));
    }

    private HashTableRowFormat createInstance(List<? extends Type> hashTypes, int maxVarWidthBufferSize)
    {
        ColumnValueExtractor[] columnValueExtractors = toColumnValueExtractor(hashTypes);

        Class<? extends HashTableRowFormat> rowFormatClass = FixedOffsetHashTableRowFormat.class;
        if (useDedicatedExtractor) {
            if (columnValueExtractors.length == 2) {
                rowFormatClass = FixedOffsetHashTableRowFormat2Channels.class;
            }
            else if (columnValueExtractors.length == 4) {
                rowFormatClass = FixedOffsetHashTableRowFormat4Channels.class;
            }
        }

        Class<? extends HashTableRowFormat> isolatedClass = isolateClass(rowFormatClass);

        try {
            return isolatedClass.getConstructor(int.class, ColumnValueExtractor[].class).newInstance(maxVarWidthBufferSize, columnValueExtractors);
        }
        catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private static ColumnValueExtractor[] toColumnValueExtractor(List<? extends Type> hashTypes)
    {
        return hashTypes.stream()
                .map(ColumnValueExtractor::columnValueExtractor)
                .map(columnValueExtractor -> columnValueExtractor.orElseThrow(() -> new RuntimeException("unsupported type " + hashTypes)))
                .toArray(ColumnValueExtractor[]::new);
    }

    private Class<? extends HashTableRowFormat> isolateClass(Class<? extends HashTableRowFormat> rowFormatClass)
    {
        DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(HashTableRowFormat.class.getClassLoader());

        return IsolatedClass.isolateClass(
                dynamicClassLoader,
                HashTableRowFormat.class,
                rowFormatClass);
    }

    private static class CacheKey
    {
        private final List<? extends Type> hashChannelTypes;
        private final int maxVarWidthBufferSize;

        private CacheKey(List<? extends Type> hashChannelTypes, int maxVarWidthBufferSize)
        {
            this.hashChannelTypes = requireNonNull(hashChannelTypes, "hashChannelTypes is null");
            this.maxVarWidthBufferSize = maxVarWidthBufferSize;
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
            CacheKey cacheKey = (CacheKey) o;
            return maxVarWidthBufferSize == cacheKey.maxVarWidthBufferSize && hashChannelTypes.equals(cacheKey.hashChannelTypes);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(hashChannelTypes, maxVarWidthBufferSize);
        }
    }
}
