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
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.plugin.hive.HiveErrorCode;
import io.trino.plugin.hive.metastore.cache.CachingHiveMetastore;
import io.trino.spi.StandardErrorCode;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.procedure.Procedure;
import io.trino.spi.type.ArrayType;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class FlushMetadataCacheProcedure
        implements Provider<Procedure>
{
    private static final String PROCEDURE_NAME = "flush_metadata_cache";

    private static final String PARAM_SCHEMA_NAME = "SCHEMA_NAME";
    private static final String PARAM_TABLE_NAME = "TABLE_NAME";
    // Other procedures use plural naming, but it's kept for backward compatibility
    @Deprecated
    private static final String PARAM_PARTITION_COLUMN = "PARTITION_COLUMN";
    @Deprecated
    private static final String PARAM_PARTITION_VALUE = "PARTITION_VALUE";
    private static final String PARAM_PARTITION_COLUMNS = "PARTITION_COLUMNS";
    private static final String PARAM_PARTITION_VALUES = "PARTITION_VALUES";

    private static final String PROCEDURE_USAGE_EXAMPLES = format(
            "Valid usages:%n" +
                    " - '%1$s()'%n" +
                    " - %1$s(%2$s => ..., %3$s => ...)" +
                    " - %1$s(%2$s => ..., %3$s => ..., %4$s => ARRAY['...'], %5$s => ARRAY['...'])",
            PROCEDURE_NAME,
            // Use lowercase parameter names per convention. In the usage example the names are not delimited.
            PARAM_SCHEMA_NAME.toLowerCase(ENGLISH),
            PARAM_TABLE_NAME.toLowerCase(ENGLISH),
            PARAM_PARTITION_COLUMNS.toLowerCase(ENGLISH),
            PARAM_PARTITION_VALUES.toLowerCase(ENGLISH));

    private static final String INVALID_PARTITION_PARAMS_ERROR_MESSAGE = format(
            "Procedure should only be invoked with single pair of partition definition named params: %1$s and %2$s or %3$s and %4$s",
            PARAM_PARTITION_COLUMNS.toLowerCase(ENGLISH),
            PARAM_PARTITION_VALUES.toLowerCase(ENGLISH),
            PARAM_PARTITION_COLUMN.toLowerCase(ENGLISH),
            PARAM_PARTITION_VALUE.toLowerCase(ENGLISH));

    private static final MethodHandle FLUSH_HIVE_METASTORE_CACHE;

    static {
        try {
            FLUSH_HIVE_METASTORE_CACHE = lookup().unreflect(FlushMetadataCacheProcedure.class.getMethod(
                    "flushMetadataCache", String.class, String.class, List.class, List.class, List.class, List.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final Optional<CachingHiveMetastore> cachingHiveMetastore;

    @Inject
    public FlushMetadataCacheProcedure(Optional<CachingHiveMetastore> cachingHiveMetastore)
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
                        new Procedure.Argument(PARAM_SCHEMA_NAME, VARCHAR, false, null),
                        new Procedure.Argument(PARAM_TABLE_NAME, VARCHAR, false, null),
                        new Procedure.Argument(PARAM_PARTITION_COLUMNS, new ArrayType(VARCHAR), false, null),
                        new Procedure.Argument(PARAM_PARTITION_VALUES, new ArrayType(VARCHAR), false, null),
                        new Procedure.Argument(PARAM_PARTITION_COLUMN, new ArrayType(VARCHAR), false, null),
                        new Procedure.Argument(PARAM_PARTITION_VALUE, new ArrayType(VARCHAR), false, null)),
                FLUSH_HIVE_METASTORE_CACHE.bindTo(this),
                true);
    }

    public void flushMetadataCache(
            String schemaName,
            String tableName,
            List<String> partitionColumns,
            List<String> partitionValues,
            List<String> partitionColumn,
            List<String> partitionValue)
    {
        Optional<List<String>> optionalPartitionColumns = Optional.ofNullable(partitionColumns);
        Optional<List<String>> optionalPartitionValues = Optional.ofNullable(partitionValues);
        Optional<List<String>> optionalPartitionColumn = Optional.ofNullable(partitionColumn);
        Optional<List<String>> optionalPartitionValue = Optional.ofNullable(partitionValue);
        checkState(partitionParamsUsed(optionalPartitionColumns, optionalPartitionValues, optionalPartitionColumn, optionalPartitionValue)
                        || deprecatedPartitionParamsUsed(optionalPartitionColumns, optionalPartitionValues, optionalPartitionColumn, optionalPartitionValue)
                        || partitionParamsNotUsed(optionalPartitionColumns, optionalPartitionValues, optionalPartitionColumn, optionalPartitionValue),
                INVALID_PARTITION_PARAMS_ERROR_MESSAGE);

        try (ThreadContextClassLoader ignored = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doFlushMetadataCache(
                    Optional.ofNullable(schemaName),
                    Optional.ofNullable(tableName),
                    optionalPartitionColumns.or(() -> optionalPartitionColumn).orElse(ImmutableList.of()),
                    optionalPartitionValues.or(() -> optionalPartitionValue).orElse(ImmutableList.of()));
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
        else if (schemaName.isPresent() && tableName.isPresent()) {
            if (!partitionColumns.isEmpty()) {
                cachingHiveMetastore.flushPartitionCache(schemaName.get(), tableName.get(), partitionColumns, partitionValues);
            }
            else {
                cachingHiveMetastore.invalidateTable(schemaName.get(), tableName.get());
            }
        }
        else {
            throw new TrinoException(StandardErrorCode.INVALID_PROCEDURE_ARGUMENT, "Illegal parameter set passed. " + PROCEDURE_USAGE_EXAMPLES);
        }
    }

    private boolean partitionParamsNotUsed(
            Optional<List<String>> partitionColumns,
            Optional<List<String>> partitionValues,
            Optional<List<String>> partitionColumn,
            Optional<List<String>> partitionValue)
    {
        return partitionColumns.isEmpty() && partitionValues.isEmpty()
                && partitionColumn.isEmpty() && partitionValue.isEmpty();
    }

    private boolean partitionParamsUsed(
            Optional<List<String>> partitionColumns,
            Optional<List<String>> partitionValues,
            Optional<List<String>> partitionColumn,
            Optional<List<String>> partitionValue)
    {
        return (partitionColumns.isPresent() || partitionValues.isPresent())
                && partitionColumn.isEmpty() && partitionValue.isEmpty();
    }

    private boolean deprecatedPartitionParamsUsed(
            Optional<List<String>> partitionColumns,
            Optional<List<String>> partitionValues,
            Optional<List<String>> partitionColumn,
            Optional<List<String>> partitionValue)
    {
        return (partitionColumn.isPresent() || partitionValue.isPresent())
                && partitionColumns.isEmpty() && partitionValues.isEmpty();
    }
}
