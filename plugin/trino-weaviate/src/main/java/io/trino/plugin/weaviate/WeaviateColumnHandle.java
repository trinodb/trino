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

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.block.ArrayBlockBuilder;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.block.RowValueBuilder;
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
import java.util.function.BiConsumer;

import static io.trino.plugin.weaviate.WeaviateErrorCode.WEAVIATE_MULTIPLE_DATA_TYPES;
import static io.trino.plugin.weaviate.WeaviateErrorCode.WEAVIATE_UNSUPPORTED_DATA_TYPE;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_MILLIS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

// TODO(dyma): make it a "path" to represent nested objects
public record WeaviateColumnHandle(String name, Type trinoType)
        implements ColumnHandle
{
    static final WeaviateColumnHandle UUID = new WeaviateColumnHandle(Property.text("__id__"));
    static final WeaviateColumnHandle CREATED_AT = new WeaviateColumnHandle(Property.date("__creationTimeUnix__"));
    static final WeaviateColumnHandle LAST_UPDATED_AT = new WeaviateColumnHandle(Property.date("__lastUpdateTimeUnix__"));
    static final List<WeaviateColumnHandle> METADATA_COLUMNS = ImmutableList.of(UUID, CREATED_AT, LAST_UPDATED_AT);

    private static final Type TEXT_ARRAY = new ArrayType(VARCHAR);
    private static final Type BOOL_ARRAY = new ArrayType(BOOLEAN);
    private static final Type INT_ARRAY = new ArrayType(INTEGER);
    private static final Type NUMBER_ARRAY = new ArrayType(DOUBLE);
    private static final Type DATE_ARRAY = new ArrayType(TIMESTAMP_TZ_MILLIS);
    private static final Type GEO_COORDINATES = RowType.rowType(
            RowType.field("latitude", DOUBLE),
            RowType.field("longitude", DOUBLE));
    private static final Type PHONE_NUMBER = RowType.rowType(
            RowType.field("defaultCountry", VARCHAR),
            RowType.field("countryCode", INTEGER),
            RowType.field("internationalFormatted", VARCHAR),
            RowType.field("national", INTEGER),
            RowType.field("nationalFormatted", VARCHAR),
            RowType.field("valid", BOOLEAN));

    private static final Type SINGLE_VECTOR = NUMBER_ARRAY;
    private static final Type MULTI_VECTOR = new ArrayType(NUMBER_ARRAY);

    public WeaviateColumnHandle
    {
        requireNonNull(name, "name is null");
        requireNonNull(trinoType, "trinoType is null");
    }

    public WeaviateColumnHandle(Property property)
    {
        this(property.propertyName(), toTrinoType(property));
    }

    public WeaviateColumnHandle(String vectorName, VectorConfig vectorConfig)
    {
        this(makeVectorName(vectorName), toTrinoType(vectorConfig));
    }

    private static Type toTrinoType(Property property)
    {
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

    private static Type toTrinoType(VectorConfig vectorConfig)
    {
        VectorIndex vectorIndex = vectorConfig.vectorIndex();
        MultiVector multiVector = switch (vectorIndex._kind()) {
            case HNSW -> vectorIndex.asHnsw().multiVector();
            case DYNAMIC -> vectorIndex.asDynamic().hnsw().multiVector();
            case FLAT -> null;
        };
        if (multiVector == null || !multiVector.enabled()) {
            return SINGLE_VECTOR;
        }
        return MULTI_VECTOR;
    }

    private static RowType nestedObjectType(List<Property> properties)
    {
        List<RowType.Field> fields = properties.stream().map(property -> RowType.field(property.propertyName(), toTrinoType(property))).toList();
        return RowType.from(fields);
    }

    @SuppressWarnings("unchecked")
    static Object toTrinoValue(Type trinoType, Object raw)
    {
        if (raw == null) {
            return null;
        }
        TrinoException unsupportedType = new TrinoException(WEAVIATE_UNSUPPORTED_DATA_TYPE, trinoType + " is not supported");

        return switch (trinoType) {
            case VarcharType _, BooleanType _, DoubleType _, IntegerType _ -> raw;
            case TimestampWithTimeZoneType _ -> convertOffsetDateTime(((OffsetDateTime) raw));
            case RowType rowType -> {
                if (rowType.equals(GEO_COORDINATES)) {
                    yield convertGeoCoordinates((GeoCoordinates) raw);
                }
                else if (rowType.equals(PHONE_NUMBER)) {
                    yield convertPhoneNumber((PhoneNumber) raw);
                }
                else {
                    yield convertNestedObject((RowType) trinoType, (Map<String, Object>) raw);
                }
            }
            case ArrayType arrayType -> switch (arrayType.getElementType()) {
                case VarcharType t -> packArray(t, (List<String>) raw, t::writeString);
                case BooleanType t -> packArray(t, (List<Boolean>) raw, t::writeBoolean);
                case IntegerType t -> packArray(t, (List<Integer>) raw, t::writeInt);
                case DoubleType t -> packArray(t, (List<Double>) raw, t::writeDouble);
                case TimestampWithTimeZoneType _ -> packArray(
                        TIMESTAMP_TZ_MILLIS, (List<OffsetDateTime>) raw,
                        (array, v) -> TIMESTAMP_TZ_MILLIS.writeLong(array, convertOffsetDateTime(v)));
                case RowType t -> packArray(t, (List<Map<String, Object>>) raw, (array, v) -> {});
                default -> throw unsupportedType;
            };
            default -> throw unsupportedType;
        };
    }

    private static Long convertOffsetDateTime(OffsetDateTime offsetDateTime)
    {
        return offsetDateTime.toInstant().toEpochMilli();
    }

    private static SqlRow convertGeoCoordinates(GeoCoordinates geoCoordinates)
    {
        return RowValueBuilder.buildRowValue((RowType) GEO_COORDINATES, fieldBuilders -> {
            BlockBuilder lat = fieldBuilders.get(0);
            BlockBuilder lon = fieldBuilders.get(1);

            DOUBLE.writeDouble(lat, geoCoordinates.latitude());
            DOUBLE.writeDouble(lon, geoCoordinates.longitude());
        });
    }

    private static SqlRow convertPhoneNumber(PhoneNumber phoneNumber)
    {
        return RowValueBuilder.buildRowValue((RowType) PHONE_NUMBER, fieldBuilders -> {
            BlockBuilder defaultCountry = fieldBuilders.get(0);
            BlockBuilder countryCode = fieldBuilders.get(1);
            BlockBuilder internationalFormatted = fieldBuilders.get(2);
            BlockBuilder national = fieldBuilders.get(3);
            BlockBuilder nationalFormatted = fieldBuilders.get(4);
            BlockBuilder valid = fieldBuilders.get(5);

            VARCHAR.writeString(defaultCountry, phoneNumber.defaultCountry());
            INTEGER.writeInt(countryCode, phoneNumber.countryCode());
            VARCHAR.writeString(internationalFormatted, phoneNumber.internationalFormatted());
            INTEGER.writeInt(national, phoneNumber.national());
            VARCHAR.writeString(nationalFormatted, phoneNumber.nationalFormatted());
            BOOLEAN.writeBoolean(valid, phoneNumber.valid());
        });
    }

    private static SqlRow convertNestedObject(RowType rowType, Map<String, Object> nested)
    {
        return RowValueBuilder.buildRowValue(rowType, fieldBuilders -> {
            int idx = 0;
            for (var field : rowType.getFields()) {
                if (field.getName().isEmpty()) {
                    idx++;
                    continue;
                }

                String name = field.getName().get();
                Type trinoType = field.getType();

                Object rawValue = nested.get(name);
                Object trinoValue = toTrinoValue(trinoType, rawValue);

                BlockBuilder rowBuilder = fieldBuilders.get(idx);
                trinoType.writeObject(rowBuilder, trinoValue);
                idx++;
            }
        });
    }

    private static <T> Block packArray(Type type, List<T> rawList, BiConsumer<ArrayBlockBuilder, T> packer)
    {
        ArrayType arrayType = new ArrayType(type);
        ArrayBlockBuilder array = arrayType.createBlockBuilder(null, rawList.size());
        for (T raw : rawList) {
            if (raw == null) {
                array.appendNull();
            }
            else {
                packer.accept(array, raw);
            }
        }
        return array.build();
    }

    public ColumnMetadata columnMetadata()
    {
        return new ColumnMetadata(name, trinoType);
    }

    private static final String SEPARATOR = "__";
    private static final String VECTORS_PREFIX = "vectors";

    static String makeName(String... parts)
    {
        return String.join(SEPARATOR, parts);
    }

    static String makeVectorName(String name)
    {
        return makeName(VECTORS_PREFIX, name);
    }
}
