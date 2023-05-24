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
package io.trino.plugin.deltalake.procedure;

import com.google.common.collect.ImmutableList;
import io.trino.plugin.deltalake.DeltaLakeMetadata;
import io.trino.plugin.deltalake.DeltaLakeMetadataFactory;
import io.trino.plugin.deltalake.LocatedTableHandle;
import io.trino.plugin.deltalake.statistics.CachingExtendedStatisticsAccess;
import io.trino.plugin.deltalake.transactionlog.TransactionLogAccess;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.spi.StandardErrorCode.INVALID_PROCEDURE_ARGUMENT;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class FlushMetadataCacheProcedure
        implements Provider<Procedure>
{
    private static final String PROCEDURE_NAME = "flush_metadata_cache";

    private static final String PARAM_SCHEMA_NAME = "SCHEMA_NAME";
    private static final String PARAM_TABLE_NAME = "TABLE_NAME";

    private static final MethodHandle FLUSH_METADATA_CACHE;

    static {
        try {
            FLUSH_METADATA_CACHE = lookup().unreflect(FlushMetadataCacheProcedure.class.getMethod("flushMetadataCache", ConnectorSession.class, String.class, String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final DeltaLakeMetadataFactory metadataFactory;
    private final Optional<CachingHiveMetastore> cachingHiveMetastore;
    private final TransactionLogAccess transactionLogAccess;
    private final CachingExtendedStatisticsAccess extendedStatisticsAccess;

    @Inject
    public FlushMetadataCacheProcedure(
            DeltaLakeMetadataFactory metadataFactory,
            Optional<CachingHiveMetastore> cachingHiveMetastore,
            TransactionLogAccess transactionLogAccess,
            CachingExtendedStatisticsAccess extendedStatisticsAccess)
    {
        this.metadataFactory = requireNonNull(metadataFactory, "metadataFactory is null");
        this.cachingHiveMetastore = requireNonNull(cachingHiveMetastore, "cachingHiveMetastore is null");
        this.transactionLogAccess = requireNonNull(transactionLogAccess, "transactionLogAccess is null");
        this.extendedStatisticsAccess = requireNonNull(extendedStatisticsAccess, "extendedStatisticsAccess is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Procedure.Argument(PARAM_SCHEMA_NAME, VARCHAR, false, null),
                        new Procedure.Argument(PARAM_TABLE_NAME, VARCHAR, false, null)),
                FLUSH_METADATA_CACHE.bindTo(this),
                true);
    }

    public void flushMetadataCache(ConnectorSession session, String schemaName, String tableName)
    {
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doFlushMetadataCache(session, Optional.ofNullable(schemaName), Optional.ofNullable(tableName));
        }
    }

    private void doFlushMetadataCache(ConnectorSession session, Optional<String> schemaName, Optional<String> tableName)
    {
        if (schemaName.isEmpty() && tableName.isEmpty()) {
            cachingHiveMetastore.ifPresent(CachingHiveMetastore::flushCache);
            transactionLogAccess.flushCache();
            extendedStatisticsAccess.invalidateCache();
        }
        else if (schemaName.isPresent() && tableName.isPresent()) {
            DeltaLakeMetadata metadata = metadataFactory.create(session.getIdentity());
            SchemaTableName schemaTableName = new SchemaTableName(schemaName.get(), tableName.get());
            // This may insert into a cache, but this will get invalidated below. TODO fix Delta so that flush_metadata_cache doesn't have to read from metastore
            LocatedTableHandle tableHandle = metadata.getTableHandle(session, schemaTableName);
            cachingHiveMetastore.ifPresent(caching -> caching.invalidateTable(schemaName.get(), tableName.get()));

            Optional<String> tableLocation = Optional.ofNullable(tableHandle).map(LocatedTableHandle::location);
            tableLocation.ifPresent(transactionLogAccess::invalidateCaches);
            tableLocation.ifPresent(extendedStatisticsAccess::invalidateCache);
        }
        else {
            throw new TrinoException(INVALID_PROCEDURE_ARGUMENT, "Illegal parameter set passed");
        }
    }
}
