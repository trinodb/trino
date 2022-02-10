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
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;

import javax.inject.Inject;
import javax.inject.Provider;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.block.MethodHandleUtil.methodHandle;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class FlushHiveMetastoreCacheProcedure
        implements Provider<Procedure>
{
    private static final String PROCEDURE_NAME = "flush_metadata_cache";

    private static final String PARAM_SCHEMA_NAME = "schema_name";
    private static final String PARAM_TABLE_NAME = "table_name";
    private static final String PARAM_PARTITION_COLUMN = "partition_column";
    private static final String PARAM_PARTITION_VALUE = "partition_value";

    private static final String PROCEDURE_USAGE_EXAMPLES = format(
            "Valid usages:%n" +
                    " - '%1$s()'%n" +
                    " - %1$s(%2$s => ..., %3$s => ..., %4$s => ARRAY['...'], %5$s => ARRAY['...'])",
            PROCEDURE_NAME,
            PARAM_SCHEMA_NAME,
            PARAM_TABLE_NAME,
            PARAM_PARTITION_COLUMN,
            PARAM_PARTITION_VALUE);

    private static final MethodHandle FLUSH_HIVE_METASTORE_CACHE = methodHandle(
            FlushHiveMetastoreCacheProcedure.class,
            "flushMetadataCache",
            String.class,
            String.class,
            String.class,
            List.class,
            List.class);
    private static final String FAKE_PARAM_DEFAULT_VALUE = "procedure should only be invoked with named parameters";

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
                PROCEDURE_NAME,
                ImmutableList.of(
                        new Procedure.Argument("$fake_first_parameter", VARCHAR, false, FAKE_PARAM_DEFAULT_VALUE),
                        new Procedure.Argument(PARAM_SCHEMA_NAME, VARCHAR, false, null),
                        new Procedure.Argument(PARAM_TABLE_NAME, VARCHAR, false, null),
                        new Procedure.Argument(PARAM_PARTITION_COLUMN, new ArrayType(VARCHAR), false, null),
                        new Procedure.Argument(PARAM_PARTITION_VALUE, new ArrayType(VARCHAR), false, null)),
                FLUSH_HIVE_METASTORE_CACHE.bindTo(this));
    }

    public void flushMetadataCache(String fakeParam, String schemaName, String tableName, List<String> partitionColumn, List<String> partitionValue)
    {
        checkState(FAKE_PARAM_DEFAULT_VALUE.equals(fakeParam), "Procedure should only be invoked with named parameters. " + PROCEDURE_USAGE_EXAMPLES);
        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doFlushMetadataCache(
                    Optional.ofNullable(schemaName),
                    Optional.ofNullable(tableName),
                    Optional.ofNullable(partitionColumn).orElse(ImmutableList.of()),
                    Optional.ofNullable(partitionValue).orElse(ImmutableList.of()));
        }
    }

    private void doFlushMetadataCache(Optional<String> schemaName, Optional<String> tableName, List<String> partitionColumns, List<String> partitionValues)
    {
        CachingHiveMetastore cachingHiveMetastore = this.cachingHiveMetastore
                .orElseThrow(() -> new TrinoException(HiveErrorCode.HIVE_METASTORE_ERROR, "Cannot flush, metastore cache is not enabled"));

        checkState(
                partitionColumns.size() == partitionValues.size(),
                "Parameters partition_column and partition_value should have same length");

        if (schemaName.isEmpty() && tableName.isEmpty() && partitionColumns.isEmpty()) {
            cachingHiveMetastore.flushCache();
        }
        else if (schemaName.isPresent() && tableName.isPresent() && !partitionColumns.isEmpty()) {
            cachingHiveMetastore.flushPartitionCache(schemaName.get(), tableName.get(), partitionColumns, partitionValues);
        }
        else {
            throw new TrinoException(
                    HiveErrorCode.HIVE_METASTORE_ERROR,
                    "Illegal parameter set passed. " + PROCEDURE_USAGE_EXAMPLES);
        }
    }
}
