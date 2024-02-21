/*
 * Copyright Starburst Data, Inc. All rights reserved.
 *
 * THIS IS UNPUBLISHED PROPRIETARY SOURCE CODE OF STARBURST DATA.
 * The copyright notice above does not evidence any
 * actual or intended publication of such source code.
 *
 * Redistribution of this material is strictly prohibited.
 */
package io.starburst.schema.discovery.formats.parquet;

import io.starburst.schema.discovery.internal.HiveTypes;
import io.trino.plugin.hive.type.DecimalTypeInfo;
import io.trino.plugin.hive.type.TypeInfo;
import io.trino.spi.TrinoException;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation.DateLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.EnumLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.IntervalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.ListLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.LogicalTypeAnnotationVisitor;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapKeyValueTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.MapLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.StringLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.UUIDLogicalTypeAnnotation;
import org.apache.parquet.schema.Type;

import java.util.Optional;

import static io.starburst.schema.discovery.SchemaDiscoveryErrorCode.UNEXPECTED_DATA_TYPE;
import static io.starburst.schema.discovery.formats.parquet.Converter.fromParquetType;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_BYTE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_DATE;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_INT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_LONG;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_SHORT;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_STRING;
import static io.starburst.schema.discovery.internal.HiveTypes.HIVE_TIMESTAMP;
import static io.starburst.schema.discovery.internal.HiveTypes.STRING_TYPE;

// TODO for https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#json and https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#bson
// visitors are missing, but they need to be checked with trino types, to pick one with best compatibility
class TypeVisitor
        implements LogicalTypeAnnotationVisitor<TypeInfo>
{
    private final org.apache.parquet.schema.Type type;

    TypeVisitor(org.apache.parquet.schema.Type type)
    {
        this.type = type;
    }

    @Override
    public Optional<TypeInfo> visit(StringLogicalTypeAnnotation stringLogicalType)
    {
        return Optional.of(STRING_TYPE);
    }

    @Override
    public Optional<TypeInfo> visit(MapLogicalTypeAnnotation mapLogicalType)
    {
        return Optional.of(handleMapType().orElseThrow(() -> new TrinoException(UNEXPECTED_DATA_TYPE, "Unsupported map type: " + type)));
    }

    @Override
    public Optional<TypeInfo> visit(ListLogicalTypeAnnotation listLogicalType)
    {
        if (type instanceof GroupType group) {
            if (group.getFieldCount() == 1) {
                Type field = group.getFields().get(0);
                if (field instanceof GroupType nestedGroup) {
                    if (nestedGroup.getFieldCount() == 1) {
                        return Optional.of(HiveTypes.arrayType(fromParquetType(nestedGroup.getType(0))));
                    }
                }
            }
        }
        throw new TrinoException(UNEXPECTED_DATA_TYPE, "Unsupported list type: " + type);
    }

    @Override
    public Optional<TypeInfo> visit(IntLogicalTypeAnnotation intLogicalType)
    {
        return switch (intLogicalType.getBitWidth()) {
            case 8 -> Optional.of(HIVE_BYTE);
            case 16 -> Optional.of(HIVE_SHORT);
            case 64 -> Optional.of(HIVE_LONG);
            default -> Optional.of(HIVE_INT);
        };
    }

    @Override
    public Optional<TypeInfo> visit(DecimalLogicalTypeAnnotation decimalLogicalType)
    {
        DecimalTypeInfo typeInfo = new DecimalTypeInfo(decimalLogicalType.getPrecision(), decimalLogicalType.getScale());
        return Optional.of(typeInfo);
    }

    @Override
    public Optional<TypeInfo> visit(DateLogicalTypeAnnotation dateLogicalType)
    {
        return Optional.of(HIVE_DATE);
    }

    @Override
    public Optional<TypeInfo> visit(TimeLogicalTypeAnnotation timeLogicalType)
    {
        // maps to trino's bigint, as time is not supported in hive, and bigint has implicit cast
        return Optional.of(HIVE_LONG);
    }

    @Override
    public Optional<TypeInfo> visit(TimestampLogicalTypeAnnotation timestampLogicalType)
    {
        return Optional.of(HIVE_TIMESTAMP);
    }

    @Override
    public Optional<TypeInfo> visit(UUIDLogicalTypeAnnotation uuidLogicalType)
    {
        // uuid is not supported by type='hive' tables
        return Optional.of(HIVE_STRING);
    }

    @Override
    public Optional<TypeInfo> visit(IntervalLogicalTypeAnnotation intervalLogicalType)
    {
        // INTERVAL YEAR TO MONTH and INTERVAL DAY TO SECOND are not supported by type='hive' tables
        return Optional.of(HIVE_STRING);
    }

    @Override
    public Optional<TypeInfo> visit(EnumLogicalTypeAnnotation enumLogicalType)
    {
        // there is no enum in trino
        return Optional.of(HIVE_STRING);
    }

    @Override
    public Optional<TypeInfo> visit(MapKeyValueTypeAnnotation mapKeyValueLogicalType)
    {
        // handle as map, but do not throw if it's not compatible, just silently skip it as it might be some legacy data
        return handleMapType();
    }

    private Optional<TypeInfo> handleMapType()
    {
        if (type instanceof GroupType group) {
            if (group.getFieldCount() == 1) {
                Type field = group.getFields().get(0);
                if (field instanceof GroupType nestedGroup) {
                    if (nestedGroup.getFieldCount() == 2) {
                        return Optional.of(HiveTypes.mapType(fromParquetType(nestedGroup.getType(0)), fromParquetType(nestedGroup.getType(1))));
                    }
                }
            }
        }
        return Optional.empty();
    }
}
