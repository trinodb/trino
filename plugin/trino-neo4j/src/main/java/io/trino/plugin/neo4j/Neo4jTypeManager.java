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
package io.trino.plugin.neo4j;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.plugin.neo4j.encoding.Encodings;
import io.trino.spi.TrinoException;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.function.table.Descriptor;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.StandardTypes;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimeWithTimeZoneType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeManager;
import io.trino.spi.type.TypeSignature;
import org.neo4j.driver.Record;
import org.neo4j.driver.Value;
import org.neo4j.driver.Values;
import org.neo4j.driver.types.TypeSystem;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.google.common.base.Verify.verify;
import static io.airlift.slice.Slices.utf8Slice;
import static io.trino.plugin.neo4j.Neo4jErrorCode.NEO4J_UNSUPPORTED_PROPERTY_TYPE;
import static io.trino.spi.StandardErrorCode.JSON_OUTPUT_CONVERSION_ERROR;
import static io.trino.spi.StandardErrorCode.TYPE_MISMATCH;
import static io.trino.spi.block.ArrayValueBuilder.buildArrayValue;
import static io.trino.spi.block.MapValueBuilder.buildMapValue;
import static io.trino.spi.block.RowValueBuilder.buildRowValue;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.TimeType.TIME_NANOS;
import static io.trino.spi.type.TimeWithTimeZoneType.TIME_TZ_NANOS;
import static io.trino.spi.type.TimestampType.TIMESTAMP_NANOS;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_TZ_NANOS;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

public class Neo4jTypeManager
{
    private final TypeSystem typeSystem;
    private final ObjectMapper objectMapper;

    private final Type jsonType;
    private final Descriptor dynamicResultDescriptor;
    private final Neo4jColumnHandle dynamicResultColumn;

    @Inject
    public Neo4jTypeManager(
            TypeManager typeManager,
            TypeSystem typeSystem,
            ObjectMapper objectMapper)
    {
        this.typeSystem = requireNonNull(typeSystem, "typeSystem is null");
        this.objectMapper = requireNonNull(objectMapper, "objectMapper is null");

        this.jsonType = typeManager.getType(new TypeSignature(StandardTypes.JSON));
        this.dynamicResultDescriptor = new Descriptor(ImmutableList.of(new Descriptor.Field("result", Optional.of(this.jsonType))));
        this.dynamicResultColumn = new Neo4jColumnHandle("result", this.jsonType, true);
    }

    public Type getJsonType()
    {
        return this.jsonType;
    }

    public Descriptor getDynamicResultDescriptor()
    {
        return this.dynamicResultDescriptor;
    }

    public boolean isDynamicResultDescriptor(Descriptor descriptor)
    {
        return this.dynamicResultDescriptor.equals(descriptor);
    }

    public ColumnHandle getDynamicResultColumn()
    {
        return this.dynamicResultColumn;
    }

    public Type propertyTypesToTrinoType(List<String> propertyTypes)
    {
        if (propertyTypes.size() != 1) {
            // A property can have values of different types, bail to JSON in this case.
            // throw new TrinoException(NEO4J_AMBIGUOUS_PROPERTY_TYPE, "%s".formatted(String.join(", ", propertyTypes)));
            return this.jsonType;
        }
        return propertyTypeToTrinoType(propertyTypes.get(0));
    }

    private static final Pattern NEO4J_ARRAY_PROPERTY = Pattern.compile("(.*)Array", CASE_INSENSITIVE);

    public Type propertyTypeToTrinoType(String propertyTypeName)
    {
        return switch (propertyTypeName.toLowerCase(Locale.ENGLISH)) {
            case "boolean" -> BOOLEAN;
            case "integer", "long" -> BIGINT;
            case "float", "double" -> DOUBLE;
            case "string" -> VARCHAR;
            case "date" -> DATE;
            case "duration" -> VARCHAR;
            case "localtime" -> TIME_NANOS;
            case "localdatetime" -> TIMESTAMP_NANOS;
            case "time" -> TIME_TZ_NANOS;
            case "datetime" -> TIMESTAMP_TZ_NANOS;
            case "point" -> this.jsonType;
            default -> {
                // Properties can have homogenous lists of primitive types,
                // these will be called *Array, for example StringArray, DateArray.
                Matcher matcher = NEO4J_ARRAY_PROPERTY.matcher(propertyTypeName);
                if (matcher.matches()) {
                    String listElementTypeName = matcher.group(1);
                    yield new ArrayType(propertyTypeToTrinoType(listElementTypeName));
                }
                throw new TrinoException(NEO4J_UNSUPPORTED_PROPERTY_TYPE, "Unsupported Neo4j property type: " + propertyTypeName);
            }
        };
    }

    public boolean toBoolean(Value value, Type type)
    {
        if (value.hasType(typeSystem.BOOLEAN())) {
            if (type.equals(BOOLEAN)) {
                return value.asBoolean();
            }
        }

        throw typeMismatchException(value, type);
    }

    public long toLong(Value value, Type type)
    {
        if (value.hasType(typeSystem.DATE())) {
            if (type.equals(DATE)) {
                return value.asLocalDate().toEpochDay();
            }
        }

        if (value.hasType(typeSystem.INTEGER())) {
            if (type.equals(INTEGER)) {
                return value.asLong();
            }
            if (type.equals(BIGINT)) {
                return value.asLong();
            }
        }

        if (value.hasType(typeSystem.LOCAL_TIME())) {
            if (type instanceof TimeType timeType) {
                return Encodings.toLongEncodedTime(value.asLocalTime(), timeType.getPrecision());
            }
        }

        if (value.hasType(typeSystem.TIME())) {
            if (type instanceof TimeWithTimeZoneType timeType) {
                return Encodings.toLongEncodedTime(value.asOffsetTime(), timeType.getPrecision());
            }
        }

        if (value.hasType(typeSystem.LOCAL_DATE_TIME())) {
            if (type instanceof TimestampType timestampType) {
                return Encodings.toLongEncodedTimestamp(value.asLocalDateTime(), timestampType.getPrecision());
            }
        }

        if (value.hasType(typeSystem.DATE_TIME())) {
            return Encodings.toLongEncodedTimestamp(value.asZonedDateTime());
        }

        throw typeMismatchException(value, type);
    }

    public double toDouble(Value value, Type type)
    {
        // float -> double
        if (value.hasType(typeSystem.FLOAT())) {
            if (type.equals(DOUBLE)) {
                return value.asDouble();
            }
        }

        // integer -> double
        if (value.hasType(typeSystem.INTEGER())) {
            if (type.equals(DOUBLE)) {
                return value.asDouble();
            }
        }

        throw typeMismatchException(value, type);
    }

    public Slice toSlice(Value value, Type type)
    {
        // string -> varchar, json
        if (value.hasType(typeSystem.STRING())) {
            if (type.equals(VARCHAR)) {
                return utf8Slice(value.asString());
            }
            if (type.equals(this.jsonType)) {
                return toJson(value.asString());
            }
        }

        // integer -> varchar, json
        if (value.hasType(typeSystem.INTEGER())) {
            if (type.equals(VARCHAR)) {
                return utf8Slice(Long.toString(value.asLong()));
            }
            if (type.equals(this.jsonType)) {
                return toJson(value.asLong());
            }
        }

        // number -> ?
        /*if (value.hasType(typeSystem.NUMBER())) {
        }*/

        // float -> varchar, json
        if (value.hasType(typeSystem.FLOAT())) {
            if (type.equals(VARCHAR)) {
                return utf8Slice(Double.toString(value.asDouble()));
            }
            if (type.equals(this.jsonType)) {
                return toJson(value.asDouble());
            }
        }

        // boolean -> varchar, json
        if (value.hasType(typeSystem.BOOLEAN())) {
            if (type.equals(VARCHAR)) {
                return utf8Slice(Boolean.toString(value.asBoolean()));
            }
            if (type.equals(this.jsonType)) {
                return toJson(value.asBoolean());
            }
        }

        // bytes -> varbinary
        if (value.hasType(typeSystem.BYTES())) {
            if (type.equals(VARBINARY)) {
                byte[] bytes = value.asByteArray();
                return Slices.wrappedBuffer(bytes, 0, bytes.length);
            }
        }

        // date -> varchar
        if (value.hasType(typeSystem.DATE())) {
            if (type.equals(this.jsonType)) {
                return toJson(value.asLocalDate());
            }
        }

        // time -> json
        if (value.hasType(typeSystem.TIME())) {
            if (type.equals(this.jsonType)) {
                return toJson(value.asOffsetTime());
            }
        }

        // localtime -> json
        if (value.hasType(typeSystem.LOCAL_TIME())) {
            if (type.equals(this.jsonType)) {
                return toJson(value.asLocalTime());
            }
        }

        // localdatetime -> json
        if (value.hasType(typeSystem.LOCAL_DATE_TIME())) {
            if (type.equals(this.jsonType)) {
                return toJson(value.asLocalDateTime());
            }
        }

        // datetime -> json
        if (value.hasType(typeSystem.DATE_TIME())) {
            if (type.equals(this.jsonType)) {
                return toJson(value.asZonedDateTime());
            }
        }

        // duration -> varchar, json
        if (value.hasType(typeSystem.DURATION())) {
            if (type.equals(VARCHAR)) {
                return utf8Slice(value.asIsoDuration().toString());
            }
            if (type.equals(this.jsonType)) {
                return toJson(value.asIsoDuration().toString());
            }
        }

        // list -> json [array]
        if (value.hasType(typeSystem.LIST())) {
            if (type.equals(this.jsonType)) {
                return toJson(value.asList());
            }
        }

        // node -> json
        if (value.hasType(typeSystem.NODE())) {
            if (type.equals(this.jsonType)) {
                return toJson(value.asNode());
            }
        }

        // relationship -> json
        if (value.hasType(typeSystem.RELATIONSHIP())) {
            if (type.equals(this.jsonType)) {
                return toJson(value.asRelationship());
            }
        }

        /*if (value.hasType(typeSystem.PATH())) {
        }*/

        if (value.hasType(typeSystem.POINT())) {
            if (type.equals(this.jsonType)) {
                return toJson(value.asPoint());
            }
        }

        // map -> json [object]
        if (value.hasType(typeSystem.MAP())) {
            if (type.equals(this.jsonType)) {
                return toJson(value.asMap());
            }
        }

        throw typeMismatchException(value, type);
    }

    public Object toObject(Value value, Type type)
    {
        // localdatetime -> timestamp(7-9)
        if (value.hasType(typeSystem.LOCAL_DATE_TIME())) {
            if (type instanceof TimestampType timestampType) {
                return Encodings.toLongTimestamp(value.asLocalDateTime(), timestampType.getPrecision());
            }
        }

        // datetime -> timestamp with time zone(7-9)
        if (value.hasType(typeSystem.DATE_TIME())) {
            if (type instanceof TimestampWithTimeZoneType timestampType) {
                return Encodings.toLongTimestampWithTimeZone(value.asOffsetDateTime(), timestampType.getPrecision());
            }
        }

        // list -> array, row
        if (value.hasType(typeSystem.LIST())) {
            if (type instanceof RowType rowType) {
                List<Object> values = value.asList();
                List<RowType.Field> fields = rowType.getFields();

                verify(values.size() == fields.size(),
                        "Mismatch between list size (%s) and row field count (%s)",
                        values.size(), fields.size());

                return buildRowValue(rowType, b -> {
                    verify(b.size() == values.size(), "size mismatch between values field builders");

                    for (int i = 0; i < values.size(); i++) {
                        writeObject(b.get(i), fields.get(i).getType(), values.get(i));
                    }
                });
            }
            if (type instanceof ArrayType arrayType) {
                List<Object> list = value.asList();
                return buildArrayValue(arrayType, list.size(), b -> {
                    for (Object o : list) {
                        writeObject(b, arrayType.getElementType(), o);
                    }
                });
            }
        }

        // map -> map
        if (value.hasType(typeSystem.MAP())) {
            if (type instanceof MapType mapType) {
                Map<String, Object> map = value.asMap();
                return buildMapValue(mapType, map.size(),
                        (keyBuilder, valueBuilder) -> map.forEach((k, v) -> {
                            writeObject(keyBuilder, mapType.getKeyType(), k);
                            writeObject(valueBuilder, mapType.getValueType(), v);
                        }));
            }
        }

        throw typeMismatchException(value, type);
    }

    private void writeObject(BlockBuilder b, Type type, Object o)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            //type.writeBoolean(b, (boolean) o);
            type.writeBoolean(b, this.toBoolean(Values.value(o), type));
        }
        else if (javaType == long.class) {
            //type.writeLong(b, (long) o);
            type.writeLong(b, this.toLong(Values.value(o), type));
        }
        else if (javaType == double.class) {
            //type.writeDouble(b, (double) o);
            type.writeDouble(b, this.toDouble(Values.value(o), type));
        }
        else if (javaType == Slice.class) {
            Slice slice = this.toSlice(Values.value(o), type);
            type.writeSlice(b, slice, 0, slice.length());
        }
        else {
            type.writeObject(b, this.toObject(Values.value(o), type));
        }
    }

    public Slice toJson(Object value)
    {
        try {
            return Slices.wrappedBuffer(this.objectMapper.writeValueAsBytes(value));
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(JSON_OUTPUT_CONVERSION_ERROR, "Conversion to JSON failed for: " + value, e);
        }
    }

    public Slice toJson(Value value)
    {
        try {
            return Slices.wrappedBuffer(this.objectMapper.writeValueAsBytes(value.asObject()));
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(JSON_OUTPUT_CONVERSION_ERROR, "Conversion to JSON failed for: " + value, e);
        }
    }

    public Slice toJson(Record record)
    {
        try {
            return Slices.wrappedBuffer(this.objectMapper.writeValueAsBytes(record.asMap()));
        }
        catch (JsonProcessingException e) {
            throw new TrinoException(JSON_OUTPUT_CONVERSION_ERROR, "Conversion to JSON failed for: " + record, e);
        }
    }

    private TrinoException typeMismatchException(Value value, Type type)
    {
        return new TrinoException(TYPE_MISMATCH, "Cannot convert Neo4j value '%s' to '%s'".formatted(value.type().name(), type.getDisplayName()));
    }
}
