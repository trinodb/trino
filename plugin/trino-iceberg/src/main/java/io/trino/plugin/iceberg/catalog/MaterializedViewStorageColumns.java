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
package io.trino.plugin.iceberg.catalog;

import io.trino.plugin.iceberg.PartitionTransforms.ColumnTransform;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorMaterializedViewDefinition;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static io.trino.plugin.iceberg.IcebergTableProperties.getPartitioning;
import static io.trino.plugin.iceberg.IcebergUtil.schemaFromMetadata;
import static io.trino.plugin.iceberg.PartitionFields.parsePartitionFields;
import static io.trino.plugin.iceberg.PartitionTransforms.getColumnTransform;
import static io.trino.plugin.iceberg.TypeConverter.toTrinoType;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.NumberType.NUMBER;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimeType.TIME_MICROS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MICROS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MICROS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.VarcharType.VARCHAR;

/**
 * Column type coercion for materialized view storage tables, shared by every {@link TrinoCatalog}
 * implementation regardless of how it persists the materialized view itself.
 */
public final class MaterializedViewStorageColumns
{
    private MaterializedViewStorageColumns() {}

    public static List<ColumnMetadata> columnsForMaterializedView(
            TypeManager typeManager,
            ConnectorMaterializedViewDefinition definition,
            Map<String, Object> materializedViewProperties)
    {
        Schema schemaWithTimestampTzPreserved = schemaFromMetadata(definition.getColumns().stream()
                .map(column -> {
                    Type type = typeManager.getType(column.getType());
                    if (type instanceof TimestampWithTimeZoneType timestampTzType && timestampTzType.getPrecision() <= 6) {
                        // For now preserve timestamptz columns so that we can parse partitioning
                        type = TIMESTAMP_TZ_MICROS;
                    }
                    else {
                        type = typeForMaterializedViewStorageTable(typeManager, type);
                    }
                    return new ColumnMetadata(column.getName(), type);
                })
                .collect(toImmutableList()));
        PartitionSpec partitionSpec = parsePartitionFields(schemaWithTimestampTzPreserved, getPartitioning(materializedViewProperties));
        Set<String> temporalPartitioningSources = partitionSpec.fields().stream()
                .flatMap(partitionField -> {
                    Types.NestedField sourceField = schemaWithTimestampTzPreserved.findField(partitionField.sourceId());
                    Type sourceType = toTrinoType(sourceField.type(), typeManager);
                    ColumnTransform columnTransform = getColumnTransform(partitionField, sourceType);
                    if (!columnTransform.temporal()) {
                        return Stream.of();
                    }
                    return Stream.of(sourceField.name());
                })
                .collect(toImmutableSet());

        return definition.getColumns().stream()
                .map(column -> {
                    Type type = typeManager.getType(column.getType());
                    if (type instanceof TimestampWithTimeZoneType timestampTzType && timestampTzType.getPrecision() <= 6 && temporalPartitioningSources.contains(column.getName())) {
                        // Apply point-in-time semantics to maintain partitioning capabilities
                        type = TIMESTAMP_TZ_MICROS;
                    }
                    else {
                        type = typeForMaterializedViewStorageTable(typeManager, type);
                    }
                    return new ColumnMetadata(column.getName(), type);
                })
                .collect(toImmutableList());
    }

    /**
     * Substitutes type not supported by Iceberg with a type that is supported.
     * Upon reading from a materialized view, the types will be coerced back to the original ones,
     * stored in the materialized view definition.
     */
    private static Type typeForMaterializedViewStorageTable(TypeManager typeManager, Type type)
    {
        if (type == TINYINT || type == SMALLINT) {
            return INTEGER;
        }
        if (type == NUMBER) {
            return VARCHAR;
        }
        if (type instanceof CharType) {
            return VARCHAR;
        }
        if (type instanceof TimeType timeType) {
            // Iceberg supports microsecond precision only
            return timeType.getPrecision() <= 6
                    ? TIME_MICROS
                    : VARCHAR;
        }
        if (type instanceof TimeWithTimeZoneType) {
            return VARCHAR;
        }
        if (type instanceof TimestampType timestampType) {
            // Iceberg supports microsecond precision only
            return timestampType.getPrecision() <= 6
                    ? TIMESTAMP_MICROS
                    : VARCHAR;
        }
        if (type instanceof TimestampWithTimeZoneType) {
            // Iceberg does not store the time zone.
            return VARCHAR;
        }
        if (type instanceof ArrayType arrayType) {
            return new ArrayType(typeForMaterializedViewStorageTable(typeManager, arrayType.getElementType()));
        }
        if (type instanceof MapType mapType) {
            return new MapType(
                    typeForMaterializedViewStorageTable(typeManager, mapType.getKeyType()),
                    typeForMaterializedViewStorageTable(typeManager, mapType.getValueType()),
                    typeManager.getTypeOperators());
        }
        if (type instanceof RowType rowType) {
            return RowType.rowType(
                    rowType.getFields().stream()
                            .map(field -> new RowType.Field(field.getName(), typeForMaterializedViewStorageTable(typeManager, field.getType())))
                            .toArray(RowType.Field[]::new));
        }

        // Pass through all the types not explicitly handled above. If a type is not accepted by the connector,
        // creation of the storage table will fail anyway.
        return type;
    }
}
