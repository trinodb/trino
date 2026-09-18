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
package io.trino.plugin.iceberg.util;

import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.EdgeAlgorithm;
import org.apache.iceberg.types.Type.NestedType;
import org.apache.iceberg.types.Type.PrimitiveType;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.TypeUtil;
import org.apache.iceberg.types.Types.DecimalType;
import org.apache.iceberg.types.Types.FixedType;
import org.apache.iceberg.types.Types.GeographyType;
import org.apache.iceberg.types.Types.GeometryType;
import org.apache.iceberg.types.Types.ListType;
import org.apache.iceberg.types.Types.MapType;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.StructType;
import org.apache.iceberg.types.Types.TimestampNanoType;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.variants.Variant;
import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

// TODO: Remove this class once upstream Iceberg supports geometry/geography Parquet logical annotations.
public final class IcebergParquetSchemaConverter
{
    private static final LogicalTypeAnnotation STRING = LogicalTypeAnnotation.stringType();
    private static final LogicalTypeAnnotation DATE = LogicalTypeAnnotation.dateType();
    private static final LogicalTypeAnnotation TIME_MICROS = LogicalTypeAnnotation.timeType(false, TimeUnit.MICROS);
    private static final LogicalTypeAnnotation TIMESTAMP_MICROS = LogicalTypeAnnotation.timestampType(false, TimeUnit.MICROS);
    private static final LogicalTypeAnnotation TIMESTAMPTZ_MICROS = LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS);
    private static final LogicalTypeAnnotation TIMESTAMP_NANOS = LogicalTypeAnnotation.timestampType(false, TimeUnit.NANOS);
    private static final LogicalTypeAnnotation TIMESTAMPTZ_NANOS = LogicalTypeAnnotation.timestampType(true, TimeUnit.NANOS);
    private static final String METADATA = "metadata";
    private static final String VALUE = "value";

    private IcebergParquetSchemaConverter() {}

    public static MessageType convert(Schema schema, String name)
    {
        Types.MessageTypeBuilder builder = Types.buildMessage();
        for (NestedField field : schema.columns()) {
            Type fieldType = field(field);
            if (fieldType != null) {
                builder.addField(fieldType);
            }
        }
        return builder.named(AvroSchemaUtil.makeCompatibleName(name));
    }

    private static GroupType struct(StructType struct, Type.Repetition repetition, int id, String name)
    {
        Types.GroupBuilder<GroupType> builder = Types.buildGroup(repetition);
        for (NestedField field : struct.fields()) {
            Type fieldType = field(field);
            if (fieldType != null) {
                builder.addField(fieldType);
            }
        }
        return builder.id(id).named(AvroSchemaUtil.makeCompatibleName(name));
    }

    private static Type field(NestedField field)
    {
        Type.Repetition repetition = field.isOptional() ? Type.Repetition.OPTIONAL : Type.Repetition.REQUIRED;
        int id = field.fieldId();
        String name = field.name();

        if (field.type().typeId() == TypeID.UNKNOWN) {
            return null;
        }
        if (field.type().isPrimitiveType()) {
            return primitive(field.type().asPrimitiveType(), repetition, id, name);
        }
        if (field.type().isVariantType()) {
            return variant(repetition, id, name);
        }

        NestedType nested = field.type().asNestedType();
        if (nested.isStructType()) {
            return struct(nested.asStructType(), repetition, id, name);
        }
        if (nested.isMapType()) {
            return map(nested.asMapType(), repetition, id, name);
        }
        if (nested.isListType()) {
            return list(nested.asListType(), repetition, id, name);
        }
        throw new UnsupportedOperationException("Can't convert unknown type: " + nested);
    }

    private static GroupType list(ListType list, Type.Repetition repetition, int id, String name)
    {
        NestedField elementField = list.fields().get(0);
        Type elementType = field(elementField);
        checkArgument(elementType != null, "Cannot convert element Parquet: %s", elementField.type());

        return Types.list(repetition)
                .element(elementType)
                .id(id)
                .named(AvroSchemaUtil.makeCompatibleName(name));
    }

    private static GroupType map(MapType map, Type.Repetition repetition, int id, String name)
    {
        NestedField keyField = map.fields().get(0);
        NestedField valueField = map.fields().get(1);
        Type keyType = field(keyField);
        checkArgument(keyType != null, "Cannot convert key Parquet: %s", keyField.type());
        Type valueType = field(valueField);
        checkArgument(valueType != null, "Cannot convert value Parquet: %s", valueField.type());

        return Types.map(repetition)
                .key(keyType)
                .value(valueType)
                .id(id)
                .named(AvroSchemaUtil.makeCompatibleName(name));
    }

    private static Type variant(Type.Repetition repetition, int id, String originalName)
    {
        String name = AvroSchemaUtil.makeCompatibleName(originalName);
        return Types.buildGroup(repetition)
                .as(LogicalTypeAnnotation.variantType(Variant.VARIANT_SPEC_VERSION))
                .id(id)
                .required(BINARY)
                .named(METADATA)
                .required(BINARY)
                .named(VALUE)
                .named(name);
    }

    private static Type primitive(PrimitiveType primitive, Type.Repetition repetition, int id, String originalName)
    {
        String name = AvroSchemaUtil.makeCompatibleName(originalName);
        return switch (primitive.typeId()) {
            case BOOLEAN -> Types.primitive(BOOLEAN, repetition).id(id).named(name);
            case INTEGER -> Types.primitive(INT32, repetition).id(id).named(name);
            case LONG -> Types.primitive(INT64, repetition).id(id).named(name);
            case FLOAT -> Types.primitive(FLOAT, repetition).id(id).named(name);
            case DOUBLE -> Types.primitive(DOUBLE, repetition).id(id).named(name);
            case DATE -> Types.primitive(INT32, repetition).as(DATE).id(id).named(name);
            case TIME -> Types.primitive(INT64, repetition).as(TIME_MICROS).id(id).named(name);
            case TIMESTAMP -> ((TimestampType) primitive).shouldAdjustToUTC() ?
                    Types.primitive(INT64, repetition).as(TIMESTAMPTZ_MICROS).id(id).named(name) :
                    Types.primitive(INT64, repetition).as(TIMESTAMP_MICROS).id(id).named(name);
            case TIMESTAMP_NANO -> ((TimestampNanoType) primitive).shouldAdjustToUTC() ?
                    Types.primitive(INT64, repetition).as(TIMESTAMPTZ_NANOS).id(id).named(name) :
                    Types.primitive(INT64, repetition).as(TIMESTAMP_NANOS).id(id).named(name);
            case STRING -> Types.primitive(BINARY, repetition).as(STRING).id(id).named(name);
            case BINARY -> Types.primitive(BINARY, repetition).id(id).named(name);
            case FIXED -> {
                FixedType fixed = (FixedType) primitive;
                yield Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
                        .length(fixed.length())
                        .id(id)
                        .named(name);
            }
            case DECIMAL -> {
                DecimalType decimal = (DecimalType) primitive;
                if (decimal.precision() <= 9) {
                    yield Types.primitive(INT32, repetition)
                            .as(LogicalTypeAnnotation.decimalType(decimal.scale(), decimal.precision()))
                            .id(id)
                            .named(name);
                }
                if (decimal.precision() <= 18) {
                    yield Types.primitive(INT64, repetition)
                            .as(LogicalTypeAnnotation.decimalType(decimal.scale(), decimal.precision()))
                            .id(id)
                            .named(name);
                }
                int minLength = TypeUtil.decimalRequiredBytes(decimal.precision());
                yield Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
                        .length(minLength)
                        .as(LogicalTypeAnnotation.decimalType(decimal.scale(), decimal.precision()))
                        .id(id)
                        .named(name);
            }
            case UUID -> Types.primitive(FIXED_LEN_BYTE_ARRAY, repetition)
                    .length(16)
                    .as(LogicalTypeAnnotation.uuidType())
                    .id(id)
                    .named(name);
            case GEOMETRY -> {
                GeometryType geometryType = (GeometryType) primitive;
                yield Types.primitive(BINARY, repetition)
                        .as(LogicalTypeAnnotation.geometryType(geometryType.crs() == null ? GeometryType.DEFAULT_CRS : geometryType.crs()))
                        .id(id)
                        .named(name);
            }
            case GEOGRAPHY -> {
                GeographyType geographyType = (GeographyType) primitive;
                yield Types.primitive(BINARY, repetition)
                        .as(toParquetGeographyType(geographyType))
                        .id(id)
                        .named(name);
            }
            default -> throw new UnsupportedOperationException("Unsupported type for Parquet: " + primitive);
        };
    }

    private static LogicalTypeAnnotation toParquetGeographyType(GeographyType geographyType)
    {
        String crs = geographyType.crs();
        EdgeAlgorithm algorithm = geographyType.algorithm();
        if (crs == null && algorithm == null) {
            return LogicalTypeAnnotation.geographyType();
        }
        return LogicalTypeAnnotation.geographyType(
                crs == null ? GeographyType.DEFAULT_CRS : crs,
                algorithm == null ? LogicalTypeAnnotation.DEFAULT_ALGO : EdgeInterpolationAlgorithm.valueOf(algorithm.name()));
    }
}
