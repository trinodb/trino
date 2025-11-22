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
package io.trino.plugin.hive.metastore.dynamic;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.protobuf.Descriptors;
import io.airlift.units.Duration;
import io.trino.cache.EvictableCacheBuilder;
import io.trino.hive.formats.line.protobuf.ProtobufDeserializerFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreWrapper;
import io.trino.metastore.Partition;
import io.trino.metastore.Table;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static io.trino.hive.formats.line.protobuf.ProtobufDeserializerFactory.SERIALIZATION_CLASS;
import static io.trino.plugin.hive.HiveStorageFormat.SEQUENCEFILE_PROTOBUF;
import static java.util.stream.Collectors.toMap;

public class DynamicSchemaHiveMetastore
        extends HiveMetastoreWrapper
{
    private final LoadingCache<TableReference, List<Column>> dynamicSchemaCache;

    public DynamicSchemaHiveMetastore(HiveMetastore delegate, ProtobufDeserializerFactory protobufDeserializerFactory, Duration dynamicSchemaCacheExpiration)
    {
        super(delegate);
        dynamicSchemaCache = EvictableCacheBuilder.newBuilder()
                .expireAfterWrite(dynamicSchemaCacheExpiration.toJavaTime())
                .build(new CacheLoader<>() {
                    @Override
                    public List<Column> load(TableReference tableName)
                    {
                        Descriptors.Descriptor descriptor = protobufDeserializerFactory.getDescriptor(tableName.getSerializationClass());
                        return descriptor.getFields().stream().map(DynamicSchemaLoader::fieldToColumn).toList();
                    }
                });
    }

    private static boolean isTableWithDynamicSchema(Table table)
    {
        return table.getStorage() != null && table.getStorage().getStorageFormat() != null && SEQUENCEFILE_PROTOBUF.getSerde().equals(table.getStorage().getStorageFormat().getSerDeNullable());
    }

    private List<Column> getDynamicColumns(Table table)
    {
        return dynamicSchemaCache.getUnchecked(new TableReference(table));
    }

    private Partition getDynamicPartition(Table table, Partition partition)
    {
        return Partition.builder(partition)
                .setColumns(getDynamicColumns(table))
                .build();
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        return delegate.getTable(databaseName, tableName).map(table -> {
            if (isTableWithDynamicSchema(table)) {
                return Table.builder(table)
                        .setDataColumns(getDynamicColumns(table))
                        .build();
            }
            return table;
        });
    }

    @Override
    public Optional<Partition> getPartition(Table table, List<String> partitionValues)
    {
        return delegate.getPartition(table, partitionValues).map(partition -> {
            if (isTableWithDynamicSchema(table)) {
                return getDynamicPartition(table, partition);
            }
            return partition;
        });
    }

    @Override
    public Map<String, Optional<Partition>> getPartitionsByNames(Table table, List<String> partitionNames)
    {
        Map<String, Optional<Partition>> partitionsByNames = delegate.getPartitionsByNames(table, partitionNames);
        if (isTableWithDynamicSchema(table)) {
            return partitionsByNames.entrySet().stream()
                    .collect(toMap(Map.Entry::getKey, e -> e.getValue()
                            .map(partition -> getDynamicPartition(table, partition))));
        }
        return partitionsByNames;
    }

    // Class to use as cache key, using the storage and table name
    private record TableReference(Table table)
    {
        private String getFullTableName()
        {
            return String.format("%s.%s", table.getDatabaseName(), table.getTableName());
        }

        private String getSerializationClass()
        {
            String serializationClass = table.getStorage().getSerdeParameters().get(SERIALIZATION_CLASS);
            if (serializationClass != null) {
                return serializationClass;
            }
            throw new IllegalStateException(SERIALIZATION_CLASS + " missing in table " + getFullTableName());
        }

        @Override
        public boolean equals(Object o)
        {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TableReference tableName = (TableReference) o;
            return Objects.equals(getFullTableName(), tableName.getFullTableName());
        }

        @Override
        public int hashCode()
        {
            return Objects.hashCode(getFullTableName());
        }
    }
}
