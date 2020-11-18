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
package io.prestosql.plugin.jdbc.procedures;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.prestosql.plugin.jdbc.CachingJdbcClient;
import io.prestosql.spi.procedure.Procedure;

import javax.inject.Provider;

import java.lang.invoke.MethodHandle;

import static io.prestosql.spi.block.MethodHandleUtil.methodHandle;
import static java.util.Objects.requireNonNull;

public class FlushCachesProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle FLUSH_METADATA_CACHE = methodHandle(FlushCachesProcedure.class, "flush");

    private final CachingJdbcClient cachingJdbcClient;

    @Inject
    public FlushCachesProcedure(CachingJdbcClient cachingJdbcClient)
    {
        this.cachingJdbcClient = requireNonNull(cachingJdbcClient, "cachingJdbcClient is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "flush_metadata_cache",
                ImmutableList.of(),
                FLUSH_METADATA_CACHE.bindTo(this));
    }

    @SuppressWarnings("unused")
    public void flush()
    {
        cachingJdbcClient.invalidateCache();
    }
}
