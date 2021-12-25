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
package io.trino.plugin.hive.metastore.procedure;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.hive.HiveErrorCode;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.procedure.Procedure;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static java.util.Objects.requireNonNull;

public class FlushHiveMetastoreCacheProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle FLUSH_HIVE_METASTORE_CACHE = methodHandle(
            FlushHiveMetastoreCacheProcedure.class,
            "flushMetadataCache");

    private final Optional<CachingHiveMetastore> cachingHiveMetastore;

    @Inject
    public FlushHiveMetastoreCacheProcedure(Optional<CachingHiveMetastore> cachingHiveMetastore)
    {
        this.cachingHiveMetastore = requireNonNull(cachingHiveMetastore, "cachingHiveMetastore is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "flush_metadata_cache",
                ImmutableList.of(),
                FLUSH_HIVE_METASTORE_CACHE.bindTo(this));
    }

    public void flushMetadataCache()
    {
        cachingHiveMetastore
                .orElseThrow(() -> new TrinoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Cannot flush, metastore cache is not enabled"))
                .flushCache();
    }
}
