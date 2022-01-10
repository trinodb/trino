package io.trino.operator.hash;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import io.airlift.bytecode.DynamicClassLoader;
import io.trino.operator.hash.fixed.FixedOffsetRowExtractor;
import io.trino.operator.hash.fixed.FixedOffsetRowExtractor2Channels;
import io.trino.operator.hash.fixed.FixedOffsetRowExtractor4Channels;
import io.trino.spi.type.Type;
import io.trino.sql.gen.IsolatedClass;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class IsolatedRowExtractorFactory
{
    public static volatile boolean USE_DEDICATED_EXTRACTOR = true;
    private final LoadingCache<CacheKey, RowExtractor> cache;

    public IsolatedRowExtractorFactory()
    {
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(1024 * 1024)
                .build(CacheLoader.from(key -> createRowExtractor(key.hashChannelTypes, key.maxVarWidthBufferSize)));
    }

    public RowExtractor create(List<? extends Type> hashTypes, int maxVarWidthBufferSize)
    {
        return cache.getUnchecked(new CacheKey(hashTypes, maxVarWidthBufferSize));
    }

    private RowExtractor createRowExtractor(List<? extends Type> hashTypes, int maxVarWidthBufferSize)
    {
        ColumnValueExtractor[] columnValueExtractors = toColumnValueExtractor(hashTypes);

        Class<? extends RowExtractor> rowExtractorClass = FixedOffsetRowExtractor.class;
        if (USE_DEDICATED_EXTRACTOR) {
            if (columnValueExtractors.length == 2) {
                rowExtractorClass = FixedOffsetRowExtractor2Channels.class;
            }
            else if (columnValueExtractors.length == 4) {
                rowExtractorClass = FixedOffsetRowExtractor4Channels.class;
            }
        }

        Class<? extends RowExtractor> isolatedClass = isolateClass(rowExtractorClass);

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

    private Class<? extends RowExtractor> isolateClass(Class<? extends RowExtractor> rowExtractorClass)
    {
        DynamicClassLoader dynamicClassLoader = new DynamicClassLoader(RowExtractor.class.getClassLoader());

        return IsolatedClass.isolateClass(
                dynamicClassLoader,
                RowExtractor.class,
                rowExtractorClass);
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
