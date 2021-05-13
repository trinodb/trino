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
package io.trino.parquet.reader.cache;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.jmx.CacheStatsMBean;
import io.trino.parquet.ParquetDataSource;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.reader.MetadataSource;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Throwables.throwIfInstanceOf;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CachingMetadataSource
        implements MetadataSource
{
    private final Cache<ParquetDataSourceId, ParquetMetadata> cache;
    private final MetadataSource delegate;

    public CachingMetadataSource(MetadataCacheConfig config, MetadataSource delegate)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        cache = CacheBuilder.newBuilder()
                .maximumSize(config.getMetadataCacheMaxEntries())
                .expireAfterAccess(config.getMetadataCacheTtl().toMillis(), MILLISECONDS)
                .recordStats()
                .build();
    }

    @Override
    public ParquetMetadata readMetadata(ParquetDataSource dataSource)
            throws IOException
    {
        try {
            return cache.get(dataSource.getId(), () -> delegate.readMetadata(dataSource));
        }
        catch (ExecutionException | UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), IOException.class);
            throw new IOException("Error in parquet metadata reader", e.getCause());
        }
    }

    @Managed
    @Flatten
    public CacheStatsMBean getStats()
    {
        return new CacheStatsMBean(cache);
    }
}
