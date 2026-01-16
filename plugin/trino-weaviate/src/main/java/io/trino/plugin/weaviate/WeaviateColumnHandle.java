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
package io.trino.plugin.weaviate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.spi.TrinoException;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowBlockBuilder;
import io.trino.spi.block.SqlRow;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.BooleanType;
import io.trino.spi.type.DoubleType;
import io.trino.spi.type.IntegerType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import io.weaviate.client6.v1.api.collections.DataType;
import io.weaviate.client6.v1.api.collections.GeoCoordinates;
import io.weaviate.client6.v1.api.collections.PhoneNumber;
import io.weaviate.client6.v1.api.collections.Property;
import io.weaviate.client6.v1.api.collections.VectorConfig;
import io.weaviate.client6.v1.api.collections.VectorIndex;
import io.weaviate.client6.v1.api.collections.vectorindex.MultiVector;

import java.time.OffsetDateTime;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static io.trino.plugin.weaviate.WeaviateErrorCode.WEAVIATE_MULTIPLE_DATA_TYPES;
import static io.trino.plugin.weaviate.WeaviateErrorCode.WEAVIATE_UNSUPPORTED_DATA_TYPE;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

public record WeaviateColumnHandle(String name, Type trinoType)
        implements ColumnHandle
{
    static final WeaviateColumnHandle UUID = new WeaviateColumnHandle(Property.text("__id__"));
    static final WeaviateColumnHandle CREATED_AT = new WeaviateColumnHandle(Property.date("__creationTimeUnix__"));
    static final WeaviateColumnHandle LAST_UPDATED_AT = new WeaviateColumnHandle(Property.date("__lastUpdateTimeUnix__"));
    static final List<WeaviateColumnHandle> METADATA_COLUMNS = ImmutableList.of(UUID, CREATED_AT, LAST_UPDATED_AT);
    static final String VECTORS_COLUMN_NAME = "vectors";

    @VisibleForTesting static final Type TEXT_ARRAY = new ArrayType(VARCHAR);
    @VisibleForTesting static final Type BOOL_ARRAY = new ArrayType(BOOLEAN);
    @VisibleForTesting static final Type INT_ARRAY = new ArrayType(INTEGER);
    @VisibleForTesting static final Type NUMBER_ARRAY = new ArrayType(DOUBLE);
    @VisibleForTesting static final Type DATE_ARRAY = new ArrayType(TIMESTAMP_TZ_MILLIS);
    @VisibleForTesting static final Type GEO_COORDINATES = RowType.rowType(
            RowType.field("latitude", DOUBLE),
            RowType.field("longitude", DOUBLE));
    @VisibleForTesting static final Type PHONE_NUMBER = RowType.rowType(
            RowType.field("defaultCountry", VARCHAR),
            RowType.field("countryCode", INTEGER),
            RowType.field("internationalFormatted", VARCHAR),
            RowType.field("national", INTEGER),
            RowType.field("nationalFormatted", VARCHAR),
            RowType.field("valid", BOOLEAN));

    @VisibleForTesting static final Type SINGLE_VECTOR = NUMBER_ARRAY;
    @VisibleForTesting static final Type MULTI_VECTOR = new ArrayType(NUMBER_ARRAY);

    public WeaviateColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(trinoType, "trinoType is null");
    }

    public WeaviateColumnHandle(Property property)
    {
        this(property.propertyName(), toTrinoType(property));
    }

    public WeaviateColumnHandle(Map<String, VectorConfig> vectors)
    {
        this(VECTORS_COLUMN_NAME, toTrinoType(vectors));
    }

    private static Type toTrinoType(Property property)
    {
        requireNonNull(property, "property is null");

        List<String> dataTypes = property.dataTypes();
        if (dataTypes.size() > 1) {
            throw new TrinoException(WEAVIATE_MULTIPLE_DATA_TYPES, "Property %s has %d data types: %s".formatted(property.propertyName(), dataTypes.size(), property.dataTypes()));
        }
        String dataType = dataTypes.getFirst();
        return switch (dataType) {
            case DataType.TEXT -> VARCHAR;
            case DataType.BOOL -> BOOLEAN;
            case DataType.INT -> INTEGER;
            case DataType.NUMBER -> DOUBLE;
            case DataType.DATE -> TIMESTAMP_TZ_MILLIS;
            case DataType.GEO_COORDINATES -> GEO_COORDINATES;
            case DataType.PHONE_NUMBER -> PHONE_NUMBER;
            case DataType.TEXT_ARRAY -> TEXT_ARRAY;
            case DataType.BOOL_ARRAY -> BOOL_ARRAY;
            case DataType.INT_ARRAY -> INT_ARRAY;
            case DataType.NUMBER_ARRAY -> NUMBER_ARRAY;
            case DataType.DATE_ARRAY -> DATE_ARRAY;
            case DataType.OBJECT -> nestedObjectType(property.nestedProperties());
            case DataType.OBJECT_ARRAY -> new ArrayType(nestedObjectType(property.nestedProperties()));
            default -> throw new TrinoException(WEAVIATE_UNSUPPORTED_DATA_TYPE, dataType + " is not supported");
        };
    }

    private static Type toTrinoType(Map<String, VectorConfig> vectors)
    {
        requireNonNull(vectors, "vectors is null");

        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        vectors.forEach((vectorName, vectorConfig) -> {
            VectorIndex vectorIndex = vectorConfig.vectorIndex();
            MultiVector multiVector = switch (vectorIndex._kind()) {
                case HNSW -> vectorIndex.asHnsw().multiVector();
                case DYNAMIC -> vectorIndex.asDynamic().hnsw().multiVector();
                case FLAT -> null;
            };

            Type type = (multiVector == null || !multiVector.enabled())
                    ? SINGLE_VECTOR
                    : MULTI_VECTOR;

            fields.add(RowType.field(vectorName, type));
        });
        return RowType.from(fields.build());
    }

    private static RowType nestedObjectType(List<Property> properties)
    {
        requireNonNull(properties, "properties is null");

        ImmutableList.Builder<RowType.Field> fields = ImmutableList.builder();
        properties.forEach(property -> fields.add(
                RowType.field(property.propertyName(), toTrinoType(property))));
        return RowType.from(fields.build());
    }

    static Object toTrinoValue(Object raw, Type type)
    {
        return switch (type) {
            case VarcharType _, BooleanType _, DoubleType _, IntegerType _ -> raw;
            case TimestampWithTimeZoneType _ -> ((OffsetDateTime) raw).toInstant().toEpochMilli();
            default -> decodeObject(null, raw, type);
        };
    }

    static Object decodeObject(BlockBuilder blockBuilder, Object raw, Type type)
    {
        return switch (type) {
            case ArrayType arrayType -> decodeArray(blockBuilder, raw, arrayType);
            case RowType rowType -> decodeRow(blockBuilder, raw, rowType);
            default -> {
                decodePrimitive(blockBuilder, type, raw);
                yield null;
            }
        };
    }

    private static void decodePrimitive(BlockBuilder blockBuilder, Type type, Object raw)
    {
        if (raw == null) {
            requireNonNull(blockBuilder, "blockBuilder is null");
            blockBuilder.appendNull();
            return;
        }
        switch (type) {
            case VarcharType t -> t.writeString(blockBuilder, (String) raw);
            case BooleanType t -> t.writeBoolean(blockBuilder, (Boolean) raw);
            case DoubleType t -> t.writeDouble(blockBuilder, ((Number) raw).doubleValue());
            case IntegerType t -> t.writeLong(blockBuilder, ((Number) raw).longValue());
            case TimestampWithTimeZoneType t -> TIMESTAMP_TZ_MILLIS.writeLong(blockBuilder, ((OffsetDateTime) raw).toInstant().toEpochMilli());
            default -> throw new TrinoException(WEAVIATE_UNSUPPORTED_DATA_TYPE, type + " is not supported");
        }
    }

    private static Block decodeArray(BlockBuilder parentBlockBuilder, Object raw, ArrayType arrayType)
    {
        if (raw == null) {
            requireNonNull(parentBlockBuilder, "parentBlockBuilder is null");
            parentBlockBuilder.appendNull();
            return null;
        }

        List<?> list = (List<?>) raw;
        Type elementType = arrayType.getElementType();

        BlockBuilder blockBuilder = elementType.createBlockBuilder(null, list.size());
        for (Object element : list) {
            decodeObject(blockBuilder, element, elementType);
        }

        Block block = blockBuilder.build();
        if (parentBlockBuilder != null) {
            arrayType.writeObject(parentBlockBuilder, block);
            return null;
        }
        return block;
    }

    private static SqlRow decodeRow(BlockBuilder blockBuilder, Object raw, RowType rowType)
    {
        if (raw == null) {
            requireNonNull(blockBuilder, "blockBuilder is null");
            blockBuilder.appendNull();
            return null;
        }

        Map<String, Object> row;
        if (rowType.equals(GEO_COORDINATES)) {
            row = geoCoordinatesMap((GeoCoordinates) raw);
        }
        else if (rowType.equals(PHONE_NUMBER)) {
            row = phoneNumberMap((PhoneNumber) raw);
        }
        else {
            row = (Map<String, Object>) raw;
        }

        if (blockBuilder == null) {
            return buildRowValue(rowType, fieldBuilders -> buildRow(rowType, row, fieldBuilders));
        }

        RowBlockBuilder rowBlockBuilder = (RowBlockBuilder) blockBuilder;
        rowBlockBuilder.buildEntry(fieldBuilders -> buildRow(rowType, row, fieldBuilders));
        return null;
    }

    private static void buildRow(RowType type, Map<String, Object> row, List<BlockBuilder> fieldBuilders)
    {
        List<RowType.Field> fields = type.getFields();
        for (int i = 0; i < fields.size(); i++) {
            RowType.Field field = fields.get(i);
            String fieldName = field.getName()
                    .orElseThrow(() -> WeaviateErrorCode.typeNotSupported("unnamed ROW values"));
            decodeObject(fieldBuilders.get(i), row.get(fieldName), field.getType());
        }
    }

    private static Map<String, Object> geoCoordinatesMap(GeoCoordinates geoCoordinates)
    {
        List<RowType.Field> fields = ((RowType) GEO_COORDINATES).getFields();

        String latitude = fields.getFirst().getName().orElseThrow();
        String longitude = fields.getLast().getName().orElseThrow();

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        appendMapEntry(builder, latitude, geoCoordinates::latitude);
        appendMapEntry(builder, longitude, geoCoordinates::longitude);
        return builder.buildOrThrow();
    }

    private static Map<String, Object> phoneNumberMap(PhoneNumber phoneNumber)
    {
        List<RowType.Field> fields = ((RowType) PHONE_NUMBER).getFields();

        String defaultCountry = fields.get(0).getName().orElseThrow();
        String countryCode = fields.get(1).getName().orElseThrow();
        String internationalFormatted = fields.get(2).getName().orElseThrow();
        String national = fields.get(3).getName().orElseThrow();
        String nationalFormatted = fields.get(4).getName().orElseThrow();
        String valid = fields.get(5).getName().orElseThrow();

        ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
        appendMapEntry(builder, defaultCountry, phoneNumber::defaultCountry);
        appendMapEntry(builder, countryCode, phoneNumber::countryCode);
        appendMapEntry(builder, internationalFormatted, phoneNumber::internationalFormatted);
        appendMapEntry(builder, national, phoneNumber::national);
        appendMapEntry(builder, nationalFormatted, phoneNumber::nationalFormatted);
        appendMapEntry(builder, valid, phoneNumber::valid);
        return builder.buildOrThrow();
    }

    private static void appendMapEntry(ImmutableMap.Builder<String, Object> builder, String key, Supplier<Object> supplier)
    {
        Object value = supplier.get();
        if (value == null) {
            return;
        }
        builder.put(key, value);
    }

    public ColumnMetadata columnMetadata()
    {
        return new ColumnMetadata(name, trinoType);
    }
}
